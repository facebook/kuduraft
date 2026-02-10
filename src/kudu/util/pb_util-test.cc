// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <gtest/gtest.h>

#include "kudu/gutil/port.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/pb_util_test.pb.h"
#include "kudu/util/proto_container_test.pb.h"
#include "kudu/util/proto_container_test2.pb.h"
#include "kudu/util/proto_container_test3.pb.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace pb_util {

using google::protobuf::FileDescriptorSet;
using internal::WritableFileOutputStream;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

static const char* kTestFileName = "pb_container.meta";
static const char* kTestKeyvalName = "my-key";
static const int kTestKeyvalValue = 1;
static const int kUseDefaultVersion =
    0; // Use the default container version (don't set it).

class TestPbUtil : public KuduTest {
 public:
  virtual void SetUp() override {
    KuduTest::SetUp();
    path_ = GetTestPath(kTestFileName);
  }

 protected:
  // Create a container file with expected values.
  // Since this is a unit test class, and we want it to be fast, we do not
  // fsync by default.
  Status createKnownGoodContainerFile(
      CreateMode create = OVERWRITE,
      SyncMode sync = NO_SYNC);

  // Create a new Protobuf Container File Writer.
  // Set version to kUseDefaultVersion to use the default version.
  Status newPbcWriter(
      int version,
      RWFileOptions opts,
      unique_ptr<WritablePBContainerFile>* pbWriter);

  // Same as createKnownGoodContainerFile(), but with settable file version.
  // Set version to kUseDefaultVersion to use the default version.
  Status createKnownGoodContainerFileWithVersion(
      int version,
      CreateMode create = OVERWRITE,
      SyncMode sync = NO_SYNC);

  // XORs the data in the specified range of the file at the given path.
  Status
  bitFlipFileByteRange(const string& path, uint64_t offset, uint64_t length);

  void dumpPbcToString(
      const string& path,
      ReadablePBContainerFile::Format format,
      string* ret);

  // Truncate the specified file to the specified length.
  Status truncateFile(const string& path, uint64_t size);

  // Output file name for most unit tests.
  string path_;
};

// Parameterized test class for running tests across various versions of PB
// container files.
class TestPbContainerVersions : public TestPbUtil,
                                public ::testing::WithParamInterface<int> {
 public:
  TestPbContainerVersions() : version_(GetParam()) {}

 protected:
  const int version_; // The parameterized container version we are testing.
};

INSTANTIATE_TEST_CASE_P(
    SupportedVersions,
    TestPbContainerVersions,
    ::testing::Values(1, 2, kUseDefaultVersion));

Status TestPbUtil::createKnownGoodContainerFile(
    CreateMode create,
    SyncMode sync) {
  ProtoContainerTestPB testPb;
  testPb.set_name(kTestKeyvalName);
  testPb.set_value(kTestKeyvalValue);
  return WritePBContainerToPath(env_, path_, testPb, create, sync);
}

Status TestPbUtil::newPbcWriter(
    int version,
    RWFileOptions opts,
    unique_ptr<WritablePBContainerFile>* pbWriter) {
  unique_ptr<RWFile> writer;
  RETURN_NOT_OK(env_->NewRWFile(opts, path_, &writer));
  pbWriter->reset(new WritablePBContainerFile(std::move(writer)));
  if (version != kUseDefaultVersion) {
    (*pbWriter)->SetVersionForTests(version);
  }
  return Status::OK();
}

Status TestPbUtil::createKnownGoodContainerFileWithVersion(
    int version,
    CreateMode create,
    SyncMode sync) {
  ProtoContainerTestPB testPb;
  testPb.set_name(kTestKeyvalName);
  testPb.set_value(kTestKeyvalValue);

  unique_ptr<WritablePBContainerFile> pbWriter;
  RETURN_NOT_OK(newPbcWriter(version, RWFileOptions(), &pbWriter));
  RETURN_NOT_OK(pbWriter->CreateNew(testPb));
  RETURN_NOT_OK(pbWriter->Append(testPb));
  RETURN_NOT_OK(pbWriter->Close());
  return Status::OK();
}

Status TestPbUtil::bitFlipFileByteRange(
    const string& path,
    uint64_t offset,
    uint64_t length) {
  faststring buf;
  // Read the data from disk.
  {
    unique_ptr<RandomAccessFile> file;
    RETURN_NOT_OK(env_->NewRandomAccessFile(path, &file));
    uint64_t size;
    RETURN_NOT_OK(file->Size(&size));
    faststring scratch;
    scratch.resize(size);
    Slice slice(scratch.data(), size);
    RETURN_NOT_OK(file->Read(0, slice));
    buf.append(slice.data(), slice.size());
  }

  // Flip the bits.
  for (uint64_t i = 0; i < length; i++) {
    uint8_t* addr = buf.data() + offset + i;
    *addr = ~*addr;
  }

  // Write the data back to disk.
  unique_ptr<WritableFile> file;
  RETURN_NOT_OK(env_->NewWritableFile(path, &file));
  RETURN_NOT_OK(file->Append(buf));
  RETURN_NOT_OK(file->Close());

  return Status::OK();
}

Status TestPbUtil::truncateFile(const string& path, uint64_t size) {
  unique_ptr<RWFile> file;
  RWFileOptions opts;
  opts.mode = Env::OPEN_EXISTING;
  RETURN_NOT_OK(env_->NewRWFile(opts, path, &file));
  RETURN_NOT_OK(file->Truncate(size));
  return Status::OK();
}

TEST_F(TestPbUtil, TestWritableFileOutputStream) {
  shared_ptr<WritableFile> file;
  string path = GetTestPath("test.out");
  ASSERT_OK(env_util::openFileForWrite(env_, path, &file));

  WritableFileOutputStream stream(file.get(), 4096);

  void* buf;
  int size;

  // First call should yield the whole buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(4096, stream.ByteCount());

  // Backup 1000 and the next call should yield 1000
  stream.BackUp(1000);
  ASSERT_EQ(3096, stream.ByteCount());

  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(1000, size);

  // Another call should flush and yield a new buffer of 4096
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(8192, stream.ByteCount());

  // Should be able to backup to 7192
  stream.BackUp(1000);
  ASSERT_EQ(7192, stream.ByteCount());

  // Flushing shouldn't change written count.
  ASSERT_TRUE(stream.flush());
  ASSERT_EQ(7192, stream.ByteCount());

  // Since we just flushed, we should get another full buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(7192 + 4096, stream.ByteCount());

  ASSERT_TRUE(stream.flush());

  ASSERT_EQ(stream.ByteCount(), file->Size());
}

// Basic read/write test.
TEST_F(TestPbUtil, TestPbContainerSimple) {
  // Exercise both the SYNC and NO_SYNC codepaths, despite the fact that we
  // aren't able to observe a difference in the test.
  vector<SyncMode> modes = {SYNC, NO_SYNC};
  for (SyncMode mode : modes) {
    // Write the file.
    ASSERT_OK(createKnownGoodContainerFile(NO_OVERWRITE, mode));

    // Read it back, should validate and contain the expected values.
    ProtoContainerTestPB testPb;
    ASSERT_OK(ReadPBContainerFromPath(env_, path_, &testPb));
    ASSERT_EQ(kTestKeyvalName, testPb.name());
    ASSERT_EQ(kTestKeyvalValue, testPb.value());

    // Delete the file.
    ASSERT_OK(env_->DeleteFile(path_));
  }
}

// Corruption / various failure mode test.
TEST_P(TestPbContainerVersions, TestCorruption) {
  // Test that we indicate when the file does not exist.
  ProtoContainerTestPB testPb;
  Status s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsNotFound())
      << "Should not be found: " << path_ << ": " << s.ToString();

  // Test that an empty file looks like corruption.
  {
    // Create the empty file.
    unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(path_, &file));
    ASSERT_OK(file->Close());
  }
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsIncomplete())
      << "Should be zero length: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");

  // Test truncated file.
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  uint64_t knownGoodSize = 0;
  ASSERT_OK(env_->GetFileSize(path_, &knownGoodSize));
  ASSERT_OK(truncateFile(path_, knownGoodSize - 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption())
        << "Should be incorrect size: " << path_ << ": " << s.ToString();
  } else {
    ASSERT_TRUE(s.IsIncomplete())
        << "Should be incorrect size: " << path_ << ": " << s.ToString();
  }
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");

  // Test corrupted magic.
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(bitFlipFileByteRange(path_, 0, 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsCorruption())
      << "Should have invalid magic: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid magic number");

  // Test corrupted version.
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(bitFlipFileByteRange(path_, 8, 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsNotSupported())
      << "Should have unsupported version number: " << path_ << ": "
      << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), " Protobuf container has unsupported version");

  // Test corrupted magic+version checksum (only exists in the V2+ format).
  if (version_ >= 2) {
    ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
    ASSERT_OK(bitFlipFileByteRange(path_, 12, 2));
    s = ReadPBContainerFromPath(env_, path_, &testPb);
    ASSERT_TRUE(s.IsCorruption())
        << "Should have corrupted file header checksum: " << path_ << ": "
        << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "File header checksum does not match");
  }

  // Test record corruption below.
  const int kFirstRecordOffset = (version_ == 1) ? 12 : 16;

  // Test corrupted data length.
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(bitFlipFileByteRange(path_, kFirstRecordOffset, 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  } else {
    ASSERT_TRUE(s.IsCorruption())
        << "Should be invalid data length checksum: " << path_ << ": "
        << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
  }

  // Test corrupted data (looks like bad checksum).
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(bitFlipFileByteRange(path_, kFirstRecordOffset + 4, 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsCorruption())
      << "Should be incorrect checksum: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");

  // Test corrupted checksum.
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(bitFlipFileByteRange(path_, knownGoodSize - 4, 2));
  s = ReadPBContainerFromPath(env_, path_, &testPb);
  ASSERT_TRUE(s.IsCorruption())
      << "Should be incorrect checksum: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
}

// Test partial record at end of file.
TEST_P(TestPbContainerVersions, TestPartialRecord) {
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  uint64_t knownGoodSize;
  ASSERT_OK(env_->GetFileSize(path_, &knownGoodSize));
  ASSERT_OK(truncateFile(path_, knownGoodSize - 2));

  unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &file));
  ReadablePBContainerFile pbFile(std::move(file));
  ASSERT_OK(pbFile.Open());
  ProtoContainerTestPB testPb;
  Status s = pbFile.ReadNextPB(&testPb);
  // Loop to verify that the same response is repeatably returned.
  for (int i = 0; i < 2; i++) {
    if (version_ == 1) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    }
    ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  }
  ASSERT_OK(pbFile.Close());
}

// KUDU-2260: Test handling extra null bytes at the end of file. This can
// occur, for example, on ext4 in default data=ordered mode when a write
// increases the filesize but the system crashes before the actual data is
// persisted.
TEST_P(TestPbContainerVersions, TestExtraNullBytes) {
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  uint64_t knownGoodSize;
  ASSERT_OK(env_->GetFileSize(path_, &knownGoodSize));
  for (const auto extraBytes : {1, 8, 128}) {
    ASSERT_OK(truncateFile(path_, knownGoodSize + extraBytes));

    unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(path_, &file));
    ReadablePBContainerFile pbFile(std::move(file));
    ASSERT_OK(pbFile.Open());
    ProtoContainerTestPB testPb;
    // Read the first good PB. Trouble starts at the second.
    ASSERT_OK(pbFile.ReadNextPB(&testPb));
    Status s = pbFile.ReadNextPB(&testPb);
    // Loop to verify that the same response is repeatably returned.
    for (int i = 0; i < 2; i++) {
      ASSERT_TRUE(version_ == 1 ? s.IsCorruption() : s.IsIncomplete())
          << s.ToString();
      if (extraBytes < 8) {
        ASSERT_STR_CONTAINS(
            s.ToString(), "File size not large enough to be valid");
      } else if (version_ == 1) {
        ASSERT_STR_CONTAINS(
            s.ToString(), "Length and data checksum does not match");
      } else {
        ASSERT_STR_CONTAINS(s.ToString(), "rest of file is NULL bytes");
      }
    }
    ASSERT_OK(pbFile.Close());
  }
}

// Test that it is possible to append after a partial write if we truncate the
// partial record. This is only fully supported in V2+.
TEST_P(TestPbContainerVersions, TestAppendAfterPartialWrite) {
  uint64_t knownGoodSize;
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(env_->GetFileSize(path_, &knownGoodSize));

  unique_ptr<WritablePBContainerFile> writer;
  RWFileOptions opts;
  opts.mode = Env::OPEN_EXISTING;
  ASSERT_OK(newPbcWriter(version_, opts, &writer));
  ASSERT_OK(writer->OpenExisting());

  ASSERT_OK(truncateFile(path_, knownGoodSize - 2));

  unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &file));
  ReadablePBContainerFile reader(std::move(file));
  ASSERT_OK(reader.Open());
  ProtoContainerTestPB testPb;
  Status s = reader.ReadNextPB(&testPb);
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    return; // The rest of the test does not apply to version 1.
  }
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();

  // Now truncate cleanly.
  ASSERT_OK(truncateFile(path_, reader.offset()));
  s = reader.ReadNextPB(&testPb);
  ASSERT_TRUE(s.IsEndOfFile()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Reached end of file");

  // Reopen the writer to allow appending more records.
  // Append a record and read it back.
  ASSERT_OK(newPbcWriter(version_, opts, &writer));
  ASSERT_OK(writer->OpenExisting());
  testPb.set_name("hello");
  testPb.set_value(1);
  ASSERT_OK(writer->Append(testPb));
  testPb.Clear();
  ASSERT_OK(reader.ReadNextPB(&testPb));
  ASSERT_EQ("hello", testPb.name());
  ASSERT_EQ(1, testPb.value());
}

// Simple test for all versions.
TEST_P(TestPbContainerVersions, TestSingleMessage) {
  ASSERT_OK(createKnownGoodContainerFileWithVersion(version_));
  ProtoContainerTestPB testPb;
  ASSERT_OK(ReadPBContainerFromPath(env_, path_, &testPb));
  ASSERT_EQ(kTestKeyvalName, testPb.name());
  ASSERT_EQ(kTestKeyvalValue, testPb.value());
}

TEST_P(TestPbContainerVersions, TestMultipleMessages) {
  ProtoContainerTestPB pb;
  pb.set_name("foo");
  pb.set_note("bar");

  unique_ptr<WritablePBContainerFile> pbWriter;
  ASSERT_OK(newPbcWriter(version_, RWFileOptions(), &pbWriter));
  ASSERT_OK(pbWriter->CreateNew(pb));

  for (int i = 0; i < 10; i++) {
    pb.set_value(i);
    ASSERT_OK(pbWriter->Append(pb));
  }
  ASSERT_OK(pbWriter->Close());

  int pbsRead = 0;
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &reader));
  ReadablePBContainerFile pbReader(std::move(reader));
  ASSERT_OK(pbReader.Open());
  for (int i = 0;; i++) {
    ProtoContainerTestPB readPb;
    Status s = pbReader.ReadNextPB(&readPb);
    if (s.IsEndOfFile()) {
      break;
    }
    ASSERT_OK(s);
    ASSERT_EQ(pb.name(), readPb.name());
    ASSERT_EQ(readPb.value(), i);
    ASSERT_EQ(pb.note(), readPb.note());
    pbsRead++;
  }
  ASSERT_EQ(10, pbsRead);
  ASSERT_OK(pbReader.Close());
}

TEST_P(TestPbContainerVersions, TestInterleavedReadWrite) {
  ProtoContainerTestPB pb;
  pb.set_name("foo");
  pb.set_note("bar");

  // Open the file for writing and reading.
  unique_ptr<WritablePBContainerFile> pbWriter;
  ASSERT_OK(newPbcWriter(version_, RWFileOptions(), &pbWriter));
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &reader));
  ReadablePBContainerFile pbReader(std::move(reader));

  // Write the header (writer) and validate it (reader).
  ASSERT_OK(pbWriter->CreateNew(pb));
  ASSERT_OK(pbReader.Open());

  for (int i = 0; i < 10; i++) {
    SCOPED_TRACE(i);
    // Write a message and read it back.
    pb.set_value(i);
    ASSERT_OK(pbWriter->Append(pb));
    ProtoContainerTestPB readPb;
    ASSERT_OK(pbReader.ReadNextPB(&readPb));
    ASSERT_EQ(pb.name(), readPb.name());
    ASSERT_EQ(readPb.value(), i);
    ASSERT_EQ(pb.note(), readPb.note());
  }

  // After closing the writer, the reader should be out of data.
  ASSERT_OK(pbWriter->Close());
  ASSERT_TRUE(pbReader.ReadNextPB(nullptr).IsEndOfFile());
  ASSERT_OK(pbReader.Close());
}

TEST_F(TestPbUtil, TestPopulateDescriptorSet) {
  {
    // No dependencies --> just one proto.
    ProtoContainerTestPB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(1, protos.file_size());
  }
  {
    // One direct dependency --> two protos.
    ProtoContainerTest2PB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(2, protos.file_size());
  }
  {
    // One direct and one indirect dependency --> three protos.
    ProtoContainerTest3PB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(3, protos.file_size());
  }
}

void TestPbUtil::dumpPbcToString(
    const string& path,
    ReadablePBContainerFile::Format format,
    string* ret) {
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path, &reader));
  ReadablePBContainerFile pbReader(std::move(reader));
  ASSERT_OK(pbReader.Open());
  ostringstream oss;
  ASSERT_OK(pbReader.Dump(&oss, format));
  ASSERT_OK(pbReader.Close());
  *ret = oss.str();
}

TEST_P(TestPbContainerVersions, TestDumpPbContainer) {
  const char* kExpectedOutput =
      "Message 0\n"
      "-------\n"
      "record_one {\n"
      "  name: \"foo\"\n"
      "  value: 0\n"
      "}\n"
      "record_two {\n"
      "  record {\n"
      "    name: \"foo\"\n"
      "    value: 0\n"
      "  }\n"
      "}\n"
      "\n"
      "Message 1\n"
      "-------\n"
      "record_one {\n"
      "  name: \"foo\"\n"
      "  value: 1\n"
      "}\n"
      "record_two {\n"
      "  record {\n"
      "    name: \"foo\"\n"
      "    value: 2\n"
      "  }\n"
      "}\n\n";

  const char* kExpectedOutputShort =
      "0\trecord_one { name: \"foo\" value: 0 } record_two { record { name: \"foo\" value: 0 } }\n"
      "1\trecord_one { name: \"foo\" value: 1 } record_two { record { name: \"foo\" value: 2 } }\n";

  const char* kExpectedOutputJson =
      "{\"recordOne\":{\"name\":\"foo\",\"value\":0},\"recordTwo\":{\"record\":{\"name\":\"foo\",\"value\":0}}}\n" // NOLINT
      "{\"recordOne\":{\"name\":\"foo\",\"value\":1},\"recordTwo\":{\"record\":{\"name\":\"foo\",\"value\":2}}}\n"; // NOLINT

  ProtoContainerTest3PB pb;
  pb.mutable_record_one()->set_name("foo");
  pb.mutable_record_two()->mutable_record()->set_name("foo");

  unique_ptr<WritablePBContainerFile> pbWriter;
  ASSERT_OK(newPbcWriter(version_, RWFileOptions(), &pbWriter));
  ASSERT_OK(pbWriter->CreateNew(pb));

  for (int i = 0; i < 2; i++) {
    pb.mutable_record_one()->set_value(i);
    pb.mutable_record_two()->mutable_record()->set_value(i * 2);
    ASSERT_OK(pbWriter->Append(pb));
  }
  ASSERT_OK(pbWriter->Close());

  string output;
  NO_FATALS(dumpPbcToString(
      path_, ReadablePBContainerFile::Format::DEFAULT, &output));
  ASSERT_STREQ(kExpectedOutput, output.c_str());

  NO_FATALS(dumpPbcToString(
      path_, ReadablePBContainerFile::Format::ONELINE, &output));
  ASSERT_STREQ(kExpectedOutputShort, output.c_str());

  NO_FATALS(
      dumpPbcToString(path_, ReadablePBContainerFile::Format::JSON, &output));
  ASSERT_STREQ(kExpectedOutputJson, output.c_str());
}

TEST_F(TestPbUtil, TestOverwriteExistingPb) {
  ASSERT_OK(createKnownGoodContainerFile(NO_OVERWRITE));
  ASSERT_TRUE(createKnownGoodContainerFile(NO_OVERWRITE).IsAlreadyPresent());
  ASSERT_OK(createKnownGoodContainerFile(OVERWRITE));
  ASSERT_OK(createKnownGoodContainerFile(OVERWRITE));
}

TEST_F(TestPbUtil, TestRedaction) {
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
  TestSecurePrintingPB pb;

  pb.set_insecure1("public 1");
  pb.set_insecure2("public 2");
  pb.set_secure1("private 1");
  pb.set_secure2("private 2");
  pb.add_repeated_secure("private 3");
  pb.add_repeated_secure("private 4");
  pb.set_insecure3("public 3");

  for (auto s : {SecureDebugString(pb), SecureShortDebugString(pb)}) {
    ASSERT_EQ(string::npos, s.find("private"));
    ASSERT_STR_CONTAINS(s, "<redacted>");
    ASSERT_STR_CONTAINS(s, "public 1");
    ASSERT_STR_CONTAINS(s, "public 2");
    ASSERT_STR_CONTAINS(s, "public 3");
  }

  // If we disable redaction, we should see the private fields.
  ASSERT_NE("", gflags::SetCommandLineOption("redact", ""));
  ASSERT_STR_CONTAINS(SecureDebugString(pb), "private");
}

} // namespace pb_util
} // namespace kudu
