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

#include "kudu/consensus/log_util.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/array_view.h" // IWYU pragma: keep
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"

DEFINE_int32(
    log_segment_size_mb,
    8,
    "The default size for log segments, in MB");
TAG_FLAG(log_segment_size_mb, advanced);

DEFINE_bool(
    log_force_fsync_all,
    false,
    "Whether the Log/WAL should explicitly call fsync() after each write.");
TAG_FLAG(log_force_fsync_all, stable);

DEFINE_bool(
    log_preallocate_segments,
    true,
    "Whether the WAL should preallocate the entire segment before writing to it");
TAG_FLAG(log_preallocate_segments, advanced);

DEFINE_bool(
    log_async_preallocate_segments,
    true,
    "Whether the WAL segments preallocation should happen asynchronously");
TAG_FLAG(log_async_preallocate_segments, advanced);

DEFINE_double(
    fault_crash_before_write_log_segment_header,
    0.0,
    "Fraction of the time we will crash just before writing the log segment header");
TAG_FLAG(fault_crash_before_write_log_segment_header, unsafe);

using kudu::consensus::OpId;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu::log {

const char kLogSegmentHeaderMagicString[] = "kudulogf";

// A magic that is written as the very last thing when a segment is closed.
// Segments that were not closed (usually the last one being written) will not
// have this magic.
const char kLogSegmentFooterMagicString[] = "closedls";

// Header is prefixed with the header magic (8 bytes) and the header length (4
// bytes).
const size_t kLogSegmentHeaderMagicAndHeaderLength = 12;

// Footer is suffixed with the footer magic (8 bytes) and the footer length (4
// bytes).
const size_t kLogSegmentFooterMagicAndFooterLength = 12;

// Versions of Kudu <= 1.2  used a 12-byte entry header.
const size_t kEntryHeaderSizeV1 = 12;
// Later versions, which added support for compression, use a 16-byte header.
const size_t kEntryHeaderSizeV2 = 16;

// Maximum log segment header/footer size, in bytes (8 MB).
const uint32_t kLogSegmentMaxHeaderOrFooterSize = 8 * 1024 * 1024;

////////////////////////////////////////////////////////////
// LogEntryReader
////////////////////////////////////////////////////////////

LogEntryReader::LogEntryReader(ReadableLogSegment* seg)
    : seg_(seg),
      num_batches_read_(0),
      num_entries_read_(0),
      offset_(seg_->first_entry_offset()) {
  int64_t readable_to_offset = seg_->readable_to_offset_.Load();

  // If we have a footer we only read up to it. If we don't we likely crashed
  // and always read to the end.
  read_up_to_ = (seg_->footer_.IsInitialized() && !seg_->footer_was_rebuilt_)
      ? seg_->file_size() - seg_->footer_.ByteSize() -
          kLogSegmentFooterMagicAndFooterLength
      : readable_to_offset;
  VLOG(1) << "Reading segment entries from " << seg_->path_
          << ": offset=" << offset_ << " file_size=" << seg_->file_size()
          << " readable_to_offset=" << readable_to_offset;
}

LogEntryReader::~LogEntryReader() = default;

Status LogEntryReader::readNextEntry(unique_ptr<LogEntryPB>* entry) {
  // Refill pending_entries_ if none are available.
  while (pending_entries_.empty()) {
    // If we are done reading, check that we got the expected number of entries
    // and return EOF.
    if (offset_ >= read_up_to_) {
      if (seg_->footer_.IsInitialized() &&
          seg_->footer_.num_entries() != num_entries_read_) {
        return Status::Corruption(
            fmt::format(
                "Read {} log entries from {}, but expected {} based on the footer",
                num_entries_read_,
                seg_->path_,
                seg_->footer_.num_entries()));
      }

      return Status::EndOfFile("Reached end of log");
    }

    // We still expect to have more entries in the log.
    unique_ptr<LogEntryBatchPB> current_batch;

    // Read and validate the entry header first.
    Status s;
    EntryHeaderStatus s_detail = EntryHeaderStatus::OtherError;
    if (offset_ + seg_->entry_header_size() < read_up_to_) {
      s = seg_->readEntryHeaderAndBatch(
          &offset_, &tmp_buf_, &current_batch, &s_detail);
    } else {
      s = Status::Corruption(
          fmt::format("Truncated log entry at offset {}", offset_));
    }

    if (PREDICT_FALSE(!s.ok())) {
      return handleReadError(s, s_detail);
    }

    // Add the entries from this batch to our pending queue.
    for (int i = 0; i < current_batch->entry_size(); i++) {
      auto current_entry = current_batch->mutable_entry(i);
      pending_entries_.emplace_back(current_entry);
      num_entries_read_++;

      // Record it in the 'recent entries' deque.
      OpId opId;
      if (current_entry->type() == log::REPLICATE &&
          current_entry->has_replicate()) {
        opId = current_entry->replicate().id();
      } else if (
          current_entry->has_commit() &&
          current_entry->commit().has_commited_op_id()) {
        opId = current_entry->commit().commited_op_id();
      }
      if (recent_entries_.size() == kNumRecentEntries) {
        recent_entries_.pop_front();
      }
      recent_entries_.push_back({offset_, current_entry->type(), opId});
    }
    current_batch->mutable_entry()->UnsafeArenaExtractSubrange(
        0, current_batch->entry_size(), nullptr);
  }

  *entry = std::move(pending_entries_.front());
  pending_entries_.pop_front();
  return Status::OK();
}

Status LogEntryReader::handleReadError(
    const Status& s,
    EntryHeaderStatus status_detail) const {
  if (!s.IsCorruption()) {
    // IO errors should always propagate back
    return s.CloneAndPrepend(
        fmt::format("error reading from log {}", seg_->path_));
  }
  Status corruption_status = makeCorruptionStatus(s);

  // If we have a valid footer in the segment, then the segment was correctly
  // closed, and we shouldn't see any corruption anywhere (including the last
  // batch).
  if (seg_->hasFooter() && !seg_->footer_was_rebuilt_) {
    LOG(WARNING) << "Found a corruption in a closed log segment: "
                 << corruption_status.ToString();
    return corruption_status;
  }

  // If we read a corrupt entry, but we don't have a footer, then it's
  // possible that we crashed in the middle of writing an entry.
  // In this case, we scan forward to see if there are any more valid looking
  // entries after this one in the file. If there are, it's really a corruption.
  // if not, we just WARN it, since it's OK for the last entry to be partially
  // written.
  bool has_valid_entries;
  RETURN_NOT_OK_PREPEND(
      seg_->scanForValidEntryHeaders(
          offset_ + seg_->entry_header_size(), &has_valid_entries),
      "Scanning forward for valid entries");
  if (has_valid_entries) {
    return corruption_status;
  }

  CHECK(status_detail != EntryHeaderStatus::Ok);
  if (status_detail == EntryHeaderStatus::AllZeros) {
    // In the common case of hitting the end of valid entries, we'll read a
    // header which is all zero bytes, and find no more entries following it.
    // This isn't really a "Corruption" so much as an expected EOF-type
    // condition, so we'll just log at VLOG(1) instead of INFO.
    VLOG(1) << "Reached preallocated space while reading log segment "
            << seg_->path_;
  } else {
    LOG(INFO)
        << "Ignoring log segment corruption in " << seg_->path_ << " because "
        << "there are no log entries following the corrupted one. "
        << "The server probably crashed in the middle of writing an entry "
        << "to the write-ahead log or downloaded an active log via tablet copy. "
        << "Error detail: " << corruption_status.ToString();
  }
  return Status::EndOfFile("");
}

Status LogEntryReader::makeCorruptionStatus(const Status& status) const {
  string err = "Log file corruption detected. ";
  err += fmt::format(
      "Failed trying to read batch #{} at offset {} for log segment {}: ",
      num_batches_read_,
      offset_,
      seg_->path_);
  err.append("Prior entries:");

  for (const auto& r : recent_entries_) {
    if (r.offset >= 0) {
      err += fmt::format(
          " [off={} {} ({})]",
          r.offset,
          LogEntryTypePB_Name(r.type),
          OpIdToString(r.opId));
    }
  }

  return status.CloneAndAppend(err);
}

////////////////////////////////////////////////////////////
// ReadableLogSegment
////////////////////////////////////////////////////////////

Status ReadableLogSegment::open(
    Env* env,
    const string& path,
    std::shared_ptr<ReadableLogSegment>* segment) {
  VLOG(1) << "Parsing wal segment: " << path;
  shared_ptr<RandomAccessFile> readable_file;
  RETURN_NOT_OK_PREPEND(
      env_util::openFileForRandom(env, path, &readable_file),
      "Unable to open file for reading");

  *segment = std::shared_ptr<ReadableLogSegment>(
      new ReadableLogSegment(path, readable_file));
  RETURN_NOT_OK_PREPEND((*segment)->init(), "Unable to initialize segment");
  return Status::OK();
}

ReadableLogSegment::ReadableLogSegment(
    std::string path,
    shared_ptr<RandomAccessFile> readable_file)
    : path_(std::move(path)),
      file_size_(0),
      readable_to_offset_(0),
      readable_file_(std::move(readable_file)),
      codec_(nullptr),
      is_initialized_(false),
      footer_was_rebuilt_(false) {}

Status ReadableLogSegment::init(
    const LogSegmentHeaderPB& header,
    const LogSegmentFooterPB& footer,
    int64_t first_entry_offset) {
  DCHECK(!isInitialized()) << "Can only call init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";
  DCHECK(footer.IsInitialized()) << "Log segment footer must be initialized";

  RETURN_NOT_OK(readFileSize());

  header_.CopyFrom(header);
  RETURN_NOT_OK(initCompressionCodec());

  footer_.CopyFrom(footer);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;
  readable_to_offset_.Store(file_size());

  return Status::OK();
}

Status ReadableLogSegment::init(
    const LogSegmentHeaderPB& header,
    int64_t first_entry_offset) {
  DCHECK(!isInitialized()) << "Can only call init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";

  RETURN_NOT_OK(readFileSize());

  header_.CopyFrom(header);
  first_entry_offset_ = first_entry_offset;
  RETURN_NOT_OK(initCompressionCodec());
  is_initialized_ = true;

  // On a new segment, we don't expect any readable entries yet.
  readable_to_offset_.Store(first_entry_offset);

  return Status::OK();
}

Status ReadableLogSegment::init() {
  DCHECK(!isInitialized()) << "Can only call init() once";

  RETURN_NOT_OK(readFileSize());

  RETURN_NOT_OK(readHeader());
  RETURN_NOT_OK(initCompressionCodec());

  Status s = readFooter();
  if (!s.ok()) {
    if (s.IsNotFound()) {
      VLOG(1) << "Log segment " << path_
              << " has no footer. This segment was likely "
              << "being written when the server previously shut down.";
    } else {
      LOG(WARNING) << "Could not read footer for segment: " << path_ << ": "
                   << s.ToString();
      return s;
    }
  }

  is_initialized_ = true;

  readable_to_offset_.Store(file_size());

  return Status::OK();
}

Status ReadableLogSegment::initCompressionCodec() {
  // Init the compression codec.
  if (header_.has_compression_codec() &&
      header_.compression_codec() != NO_COMPRESSION) {
    RETURN_NOT_OK_PREPEND(
        CompressionCodecManager::GetCodec(header_.compression_codec(), &codec_),
        "could not init compression codec");
  }
  return Status::OK();
}

const int64_t ReadableLogSegment::readable_up_to() const {
  return readable_to_offset_.Load();
}

void ReadableLogSegment::updateReadableToOffset(int64_t readable_to_offset) {
  readable_to_offset_.Store(readable_to_offset);
  file_size_.StoreMax(readable_to_offset);
}

Status ReadableLogSegment::rebuildFooterByScanning() {
  TRACE_EVENT1(
      "log", "ReadableLogSegment::rebuildFooterByScanning", "path", path_);

  DCHECK(!footer_.IsInitialized());

  LogEntryReader reader(this);

  LogSegmentFooterPB new_footer;
  int num_entries = 0;
  while (true) {
    unique_ptr<LogEntryPB> entry;
    Status s = reader.readNextEntry(&entry);
    if (s.IsEndOfFile()) {
      break;
    }
    RETURN_NOT_OK(s);

    DCHECK(entry);
    if (entry->has_replicate()) {
      updateFooterForReplicateEntry(*entry, &new_footer);
    }
    num_entries++;
  }

  new_footer.set_num_entries(num_entries);
  footer_ = new_footer;
  DCHECK(footer_.IsInitialized());
  footer_was_rebuilt_ = true;
  readable_to_offset_.Store(reader.offset());

  VLOG(1) << "Successfully rebuilt footer for segment: " << path_
          << " (valid entries through byte offset " << reader.offset() << ")";
  return Status::OK();
}

Status ReadableLogSegment::readFileSize() {
  // Check the size of the file.
  // Env uses uint here, even though we generally prefer signed ints to avoid
  // underflow bugs. Use a local to convert.
  uint64_t size;
  RETURN_NOT_OK_PREPEND(
      readable_file_->Size(&size), "Unable to read file size");
  file_size_.Store(size);
  if (size == 0) {
    VLOG(1) << "Log segment file $0 is zero-length: " << path();
    return Status::OK();
  }
  return Status::OK();
}

Status ReadableLogSegment::readHeader() {
  uint32_t header_size;
  RETURN_NOT_OK(readHeaderMagicAndHeaderLength(&header_size));

  if (header_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::Corruption(
        fmt::format(
            "File is corrupted. "
            "Parsed header size: {} is zero or bigger than max header size: {}",
            header_size,
            kLogSegmentMaxHeaderOrFooterSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice(header_space, header_size);
  LogSegmentHeaderPB header;

  // Read and parse the log segment header.
  RETURN_NOT_OK_PREPEND(
      readable_file_->Read(kLogSegmentHeaderMagicAndHeaderLength, header_slice),
      "Unable to read fully");

  RETURN_NOT_OK_PREPEND(
      pb_util::ParseFromArray(&header, header_slice.data(), header_size),
      "Unable to parse protobuf");

  if (header.incompatible_features_size() > 0) {
    return Status::NotSupported(
        "log segment uses a feature not supported by this version "
        "of Kudu");
  }

  header_.Swap(&header);
  first_entry_offset_ = header_size + kLogSegmentHeaderMagicAndHeaderLength;

  return Status::OK();
}

Status ReadableLogSegment::readHeaderMagicAndHeaderLength(uint32_t* len) {
  uint8_t scratch[kLogSegmentHeaderMagicAndHeaderLength];
  Slice slice(scratch, kLogSegmentHeaderMagicAndHeaderLength);
  RETURN_NOT_OK(readable_file_->Read(0, slice));
  RETURN_NOT_OK(parseHeaderMagicAndHeaderLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::parseHeaderMagicAndHeaderLength(
    const Slice& data,
    uint32_t* parsed_len) {
  RETURN_NOT_OK_PREPEND(
      data.checkSize(kLogSegmentHeaderMagicAndHeaderLength),
      "Log segment file is too small to contain initial magic number");

  if (memcmp(
          kLogSegmentHeaderMagicString,
          data.data(),
          strlen(kLogSegmentHeaderMagicString)) != 0) {
    // As a special case, we check whether the file was allocated but no header
    // was written. We treat that case as an uninitialized file, much in the
    // same way we treat zero-length files.
    // Note: While the above comparison checks 8 bytes, this one checks the full
    // 12 to ensure we have a full 12 bytes of NULL data.
    if (IsAllZeros(data)) {
      // 12 bytes of NULLs, good enough for us to consider this a file that
      // was never written to (but apparently preallocated).
      LOG(WARNING) << "Log segment file " << path()
                   << " has 12 initial NULL bytes instead of "
                   << "magic and header length: "
                   << KUDU_REDACT(data.ToDebugString())
                   << " and will be treated as a blank segment.";
      return Status::Uninitialized(
          "log magic and header length are all NULL bytes");
    }
    // If no magic and not uninitialized, the file is considered corrupt.
    return Status::Corruption(
        fmt::format(
            "Invalid log segment file {}: Bad magic. {}",
            path(),
            KUDU_REDACT(data.ToDebugString())));
  }

  *parsed_len =
      DecodeFixed32(data.data() + strlen(kLogSegmentHeaderMagicString));
  return Status::OK();
}

Status ReadableLogSegment::readFooter() {
  uint32_t footer_size;
  RETURN_NOT_OK(readFooterMagicAndFooterLength(&footer_size));

  if (footer_size == 0 || footer_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::Corruption(
        fmt::format(
            "File is corrupted. "
            "Parsed header size: {} is zero or bigger than max header size: {}",
            footer_size,
            kLogSegmentMaxHeaderOrFooterSize));
  }

  if (footer_size > (file_size() - first_entry_offset_)) {
    return Status::Corruption(
        "Footer not found. File corrupted. "
        "Decoded footer length pointed at a footer before the first entry.");
  }

  uint8_t footer_space[footer_size];
  Slice footer_slice(footer_space, footer_size);

  int64_t footer_offset =
      file_size() - kLogSegmentFooterMagicAndFooterLength - footer_size;

  LogSegmentFooterPB footer;

  // Read and parse the log segment footer.
  RETURN_NOT_OK_PREPEND(
      readable_file_->Read(footer_offset, footer_slice),
      "Footer not found. Could not read fully.");

  RETURN_NOT_OK_PREPEND(
      pb_util::ParseFromArray(&footer, footer_slice.data(), footer_size),
      "Unable to parse protobuf");

  footer_.Swap(&footer);
  return Status::OK();
}

Status ReadableLogSegment::readFooterMagicAndFooterLength(uint32_t* len) {
  uint8_t scratch[kLogSegmentFooterMagicAndFooterLength];
  Slice slice(scratch, kLogSegmentFooterMagicAndFooterLength);

  CHECK_GT(file_size(), kLogSegmentFooterMagicAndFooterLength);
  RETURN_NOT_OK(readable_file_->Read(
      file_size() - kLogSegmentFooterMagicAndFooterLength, slice));

  RETURN_NOT_OK(parseFooterMagicAndFooterLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::parseFooterMagicAndFooterLength(
    const Slice& data,
    uint32_t* parsed_len) {
  RETURN_NOT_OK_PREPEND(
      data.checkSize(kLogSegmentFooterMagicAndFooterLength),
      "Slice is too small to contain final magic number");

  if (memcmp(
          kLogSegmentFooterMagicString,
          data.data(),
          strlen(kLogSegmentFooterMagicString)) != 0) {
    return Status::NotFound("Footer not found. Footer magic doesn't match");
  }

  *parsed_len =
      DecodeFixed32(data.data() + strlen(kLogSegmentFooterMagicString));
  return Status::OK();
}

Status ReadableLogSegment::readEntries(LogEntries* entries) {
  TRACE_EVENT1("log", "ReadableLogSegment::readEntries", "path", path_);
  LogEntryReader reader(this);

  while (true) {
    unique_ptr<LogEntryPB> entry;
    Status s = reader.readNextEntry(&entry);
    if (s.IsEndOfFile()) {
      break;
    }
    RETURN_NOT_OK(s);
    DCHECK(entry);
    entries->emplace_back(std::move(entry));
  }

  return Status::OK();
}

size_t ReadableLogSegment::entry_header_size() const {
  DCHECK(is_initialized_);
  return header_.has_deprecated_major_version() ? kEntryHeaderSizeV1
                                                : kEntryHeaderSizeV2;
}

Status ReadableLogSegment::scanForValidEntryHeaders(
    int64_t offset,
    bool* has_valid_entries) {
  TRACE_EVENT1(
      "log", "ReadableLogSegment::scanForValidEntryHeaders", "path", path_);
  VLOG(1) << "Scanning " << path_ << " for valid entry headers "
          << "following offset " << offset << "...";
  *has_valid_entries = false;

  constexpr auto kChunkSize = 1024 * 1024;
  unique_ptr<uint8_t[]> buf(new uint8_t[kChunkSize]);

  // We overlap the reads by the size of the header, so that if a header
  // spans chunks, we don't miss it.
  for (; offset < file_size() - entry_header_size();
       offset += kChunkSize - entry_header_size()) {
    int rem = std::min<int64_t>(file_size() - offset, kChunkSize);
    Slice chunk(buf.get(), rem);
    RETURN_NOT_OK(readable_file()->Read(offset, chunk));

    // Optimization for the case where a chunk is all zeros -- this is common in
    // the case of pre-allocated files. This avoids a lot of redundant CRC
    // calculation.
    if (IsAllZeros(chunk)) {
      continue;
    }

    // Check if this chunk has a valid entry header.
    for (int off_in_chunk = 0;
         off_in_chunk < chunk.size() - entry_header_size();
         off_in_chunk++) {
      Slice potential_header = Slice(&chunk[off_in_chunk], entry_header_size());

      EntryHeader header;
      if (decodeEntryHeader(potential_header, &header) ==
          EntryHeaderStatus::Ok) {
        VLOG(1) << "Found a valid entry header at offset "
                << (offset + off_in_chunk);
        *has_valid_entries = true;
        return Status::OK();
      }
    }
  }

  VLOG(1) << "Found no log entry headers";
  return Status::OK();
}

Status ReadableLogSegment::readEntryHeaderAndBatch(
    int64_t* offset,
    faststring* tmp_buf,
    unique_ptr<LogEntryBatchPB>* batch,
    EntryHeaderStatus* status_detail) {
  int64_t cur_offset = *offset;
  EntryHeader header;
  RETURN_NOT_OK(readEntryHeader(&cur_offset, &header, status_detail));
  Status s = readEntryBatch(&cur_offset, header, tmp_buf, batch);
  if (PREDICT_FALSE(!s.ok())) {
    // If we failed to actually decode the batch, make sure to set status_detail
    // to non-Ok.
    *status_detail = EntryHeaderStatus::OtherError;
    return s;
  }
  *offset = cur_offset;
  return Status::OK();
}

Status ReadableLogSegment::readEntryHeader(
    int64_t* offset,
    EntryHeader* header,
    EntryHeaderStatus* status_detail) {
  const size_t header_size = entry_header_size();
  uint8_t scratch[header_size];
  Slice slice(scratch, header_size);
  RETURN_NOT_OK_PREPEND(
      readable_file()->Read(*offset, slice), "Could not read log entry header");

  *status_detail = decodeEntryHeader(slice, header);
  switch (*status_detail) {
    case EntryHeaderStatus::CrcMismatch:
      return Status::Corruption("CRC mismatch in log entry header");
    case EntryHeaderStatus::AllZeros:
      return Status::Corruption("preallocated space found");
    case EntryHeaderStatus::Ok:
      break;
    default:
      LOG(FATAL) << "unexpected result from decoding";
  }

  *offset += slice.size();
  return Status::OK();
}

EntryHeaderStatus ReadableLogSegment::decodeEntryHeader(
    const Slice& data,
    EntryHeader* header) {
  uint32_t computedHeaderCrc;
  if (entry_header_size() == kEntryHeaderSizeV2) {
    header->msgLengthCompressed = DecodeFixed32(data.data());
    header->msgLength = DecodeFixed32(&data[4]);
    header->msgCrc = DecodeFixed32(&data[8]);
    header->headerCrc = DecodeFixed32(&data[12]);
    computedHeaderCrc = crc::Crc32c(data.data(), 12);
  } else {
    DCHECK_EQ(kEntryHeaderSizeV1, data.size());
    header->msgLength = DecodeFixed32(data.data());
    header->msgLengthCompressed = header->msgLength;
    header->msgCrc = DecodeFixed32(&data[4]);
    header->headerCrc = DecodeFixed32(&data[8]);
    computedHeaderCrc = crc::Crc32c(data.data(), 8);
  }

  // Verify the header.
  if (computedHeaderCrc == header->headerCrc) {
    return EntryHeaderStatus::Ok;
  }
  if (IsAllZeros(data)) {
    return EntryHeaderStatus::AllZeros;
  }
  return EntryHeaderStatus::CrcMismatch;
}

Status ReadableLogSegment::readEntryBatch(
    int64_t* offset,
    const EntryHeader& header,
    faststring* tmp_buf,
    unique_ptr<LogEntryBatchPB>* entry_batch) {
  TRACE_EVENT2(
      "log",
      "ReadableLogSegment::readEntryBatch",
      "path",
      path_,
      "range",
      fmt::format("offset={} entry_len={}", *offset, header.msgLength));

  if (header.msgLength == 0) {
    return Status::Corruption("Invalid 0 entry length");
  }
  int64_t limit = readable_up_to();
  if (PREDICT_FALSE(header.msgLengthCompressed + *offset > limit)) {
    // The log was likely truncated during writing.
    return Status::Corruption(
        fmt::format(
            "Could not read {}-byte log entry from offset {} in {}: "
            "log only readable up to offset {}",
            header.msgLengthCompressed,
            *offset,
            path_,
            limit));
  }

  tmp_buf->clear();
  size_t buf_len = header.msgLengthCompressed;
  if (codec_) {
    // Reserve some space for the decompressed copy as well.
    buf_len += header.msgLength;
  }
  tmp_buf->resize(buf_len);
  Slice entry_batch_slice(tmp_buf->data(), header.msgLengthCompressed);
  Status s = readable_file()->Read(*offset, entry_batch_slice);

  if (!s.ok()) {
    return Status::IOError(
        fmt::format("Could not read entry. Cause: {}", s.ToString()));
  }

  // Verify the CRC.
  uint32_t read_crc =
      crc::Crc32c(entry_batch_slice.data(), entry_batch_slice.size());
  if (PREDICT_FALSE(read_crc != header.msgCrc)) {
    return Status::Corruption(
        fmt::format(
            "Entry CRC mismatch in byte range {}-{}: "
            "expected CRC={}, computed={}",
            *offset,
            *offset + header.msgLength,
            header.msgCrc,
            read_crc));
  }

  // If it was compressed, decompress it.
  if (codec_) {
    // We pre-reserved space for the decompression up above.
    uint8_t* uncompress_buf = &(*tmp_buf)[header.msgLengthCompressed];
    RETURN_NOT_OK_PREPEND(
        codec_->Uncompress(entry_batch_slice, uncompress_buf, header.msgLength),
        "failed to uncompress entry");
    entry_batch_slice = Slice(uncompress_buf, header.msgLength);
  }

  unique_ptr<LogEntryBatchPB> read_entry_batch(new LogEntryBatchPB);
  s = pb_util::ParseFromArray(
      read_entry_batch.get(), entry_batch_slice.data(), header.msgLength);

  if (!s.ok()) {
    return Status::Corruption(
        fmt::format("Could not parse PB. Cause: {}", s.ToString()));
  }

  *offset += header.msgLengthCompressed;
  entry_batch->reset(read_entry_batch.release());
  return Status::OK();
}

WritableLogSegment::WritableLogSegment(
    string path,
    shared_ptr<WritableFile> writable_file)
    : path_(std::move(path)),
      writable_file_(std::move(writable_file)),
      is_header_written_(false),
      is_footer_written_(false),
      written_offset_(0) {}

Status WritableLogSegment::WriteHeaderAndOpen(
    const LogSegmentHeaderPB& new_header) {
  MAYBE_FAULT(FLAGS_fault_crash_before_write_log_segment_header);

  DCHECK(!IsHeaderWritten()) << "Can only call WriteHeader() once";
  DCHECK(new_header.IsInitialized()) << "Log segment header must be initialized"
                                     << new_header.InitializationErrorString();
  faststring buf;

  // First the magic.
  buf.append(kLogSegmentHeaderMagicString);
  // Then Length-prefixed header.
  putFixed32(&buf, new_header.ByteSize());
  // Then Serialize the PB.
  pb_util::AppendToString(new_header, &buf);
  RETURN_NOT_OK(writable_file()->Append(Slice(buf)));

  header_.CopyFrom(new_header);
  first_entry_offset_ = buf.size();
  written_offset_ = first_entry_offset_;
  is_header_written_ = true;

  return Status::OK();
}

Status WritableLogSegment::WriteFooterAndClose(
    const LogSegmentFooterPB& footer) {
  TRACE_EVENT1("log", "WritableLogSegment::WriteFooterAndClose", "path", path_);
  DCHECK(IsHeaderWritten());
  DCHECK(!IsFooterWritten());
  DCHECK(footer.IsInitialized()) << footer.InitializationErrorString();

  faststring buf;
  pb_util::AppendToString(footer, &buf);
  buf.append(kLogSegmentFooterMagicString);
  putFixed32(&buf, footer.ByteSize());

  RETURN_NOT_OK_PREPEND(
      writable_file()->Append(Slice(buf)), "Could not write the footer");

  footer_.CopyFrom(footer);
  is_footer_written_ = true;

  RETURN_NOT_OK(writable_file_->Close());

  written_offset_ += buf.size();

  return Status::OK();
}

Status WritableLogSegment::WriteEntryBatch(
    const Slice& data,
    const std::shared_ptr<CompressionCodec>& codec) {
  DCHECK(is_header_written_);
  DCHECK(!is_footer_written_);
  uint8_t header_buf[kEntryHeaderSizeV2];

  const uint32_t uncompressed_len = data.size();

  // If necessary, compress the data.
  Slice data_to_write;
  if (codec) {
    DCHECK_NE(header_.compression_codec(), NO_COMPRESSION);
    compress_buf_.resize(codec->MaxCompressedLength(uncompressed_len));
    size_t compressed_len;
    RETURN_NOT_OK(codec->Compress(data, compress_buf_.data(), &compressed_len));
    compress_buf_.resize(compressed_len);
    data_to_write = Slice(compress_buf_.data(), compress_buf_.size());
  } else {
    data_to_write = data;
  }

  // Fill in the header.
  inlineEncodeFixed32(&header_buf[0], data_to_write.size());
  inlineEncodeFixed32(&header_buf[4], uncompressed_len);
  inlineEncodeFixed32(
      &header_buf[8], crc::Crc32c(data_to_write.data(), data_to_write.size()));
  inlineEncodeFixed32(
      &header_buf[12], crc::Crc32c(&header_buf[0], kEntryHeaderSizeV2 - 4));

  // Write the header to the file, followed by the batch data itself.
  Slice slices[2] = {Slice(header_buf, arraysize(header_buf)), data_to_write};
  RETURN_NOT_OK(writable_file_->AppendV(slices));
  written_offset_ += arraysize(header_buf) + data_to_write.size();
  return Status::OK();
}

unique_ptr<LogEntryBatchPB> createBatchFromAllocatedOperations(
    const vector<consensus::ReplicateRefPtr>& msgs) {
  unique_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->mutable_entry()->Reserve(msgs.size());
  for (const auto& msg : msgs) {
    LogEntryPB* entry_pb = entry_batch->add_entry();
    entry_pb->set_type(log::REPLICATE);
    entry_pb->set_allocated_replicate(msg->get());
  }
  return entry_batch;
}

bool isLogFileName(const string& fname) {
  if (hasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file: " << fname;
    return false;
  }

  vector<string> v = strings::Split(fname, "-");
  if (v.size() != 2 || v[0] != FsManager::kWalFileNamePrefix) {
    VLOG(1) << "Not a log file: " << fname;
    return false;
  }

  return true;
}

void updateFooterForReplicateEntry(
    const LogEntryPB& entry_pb,
    LogSegmentFooterPB* footer) {
  DCHECK(entry_pb.has_replicate());
  int64_t index = entry_pb.replicate().id().index();
  if (!footer->has_min_replicate_index() ||
      index < footer->min_replicate_index()) {
    footer->set_min_replicate_index(index);
  }
  if (!footer->has_max_replicate_index() ||
      index > footer->max_replicate_index()) {
    footer->set_max_replicate_index(index);
  }
}

} // namespace kudu::log
