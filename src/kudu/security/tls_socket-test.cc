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

#include "kudu/security/tls_handshake.h"

#include <pthread.h>
#include <sched.h>
#include <sys/uio.h>
#include <algorithm>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/ScopeGuard.h>
#include "kudu/gutil/macros.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

const MonoDelta kTimeout = MonoDelta::FromSeconds(10);

// Size is big enough to not fit into output socket buffer of default size
// (controlled by setsockopt() with SO_SNDBUF).
constexpr size_t kEchoChunkSize = 32 * 1024 * 1024;

class TlsSocketTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(clientTls_.Init());
  }

 protected:
  void connectClient(const Sockaddr& addr, unique_ptr<Socket>* sock);
  TlsContext clientTls_;
};

Status doNegotiationSide(Socket* sock, TlsHandshake* tls, const char* side) {
  tls->setVerificationMode(TlsVerificationMode::VerifyNone);

  bool done = false;
  string received;
  while (!done) {
    string toSend;
    Status s = tls->continueHandshake(received, &toSend);
    if (s.ok()) {
      done = true;
    } else if (!s.IsIncomplete()) {
      RETURN_NOT_OK_PREPEND(s, "unexpected tls error");
    }
    if (!toSend.empty()) {
      size_t nwritten;
      auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(10);
      RETURN_NOT_OK_PREPEND(
          sock->BlockingWrite(
              reinterpret_cast<const uint8_t*>(toSend.data()),
              toSend.size(),
              &nwritten,
              deadline),
          "error sending");
    }

    if (!done) {
      uint8_t buf[1024];
      int32_t n = 0;
      RETURN_NOT_OK_PREPEND(
          sock->Recv(buf, arraysize(buf), &n), "error receiving");
      received = string(reinterpret_cast<char*>(&buf[0]), n);
    }
  }
  LOG(INFO) << side << ": negotiation complete";
  return Status::OK();
}

void TlsSocketTest::connectClient(
    const Sockaddr& addr,
    unique_ptr<Socket>* sock) {
  unique_ptr<Socket> clientSock(new Socket());
  ASSERT_OK(clientSock->Init(0));
  ASSERT_OK(clientSock->Connect(addr));

  TlsHandshake client;
  ASSERT_OK(clientTls_.InitiateHandshake(TlsHandshakeType::Client, &client));
  ASSERT_OK(doNegotiationSide(clientSock.get(), &client, "client"));
  ASSERT_OK(client.finish(&clientSock));
  *sock = std::move(clientSock);
}

class EchoServer {
 public:
  EchoServer() : pthreadSync_(1) {}
  ~EchoServer() {
    stop();
    join();
  }

  void start() {
    ASSERT_OK(serverTls_.Init());
    ASSERT_OK(serverTls_.GenerateSelfSignedCertAndKey());
    ASSERT_OK(listenAddr_.ParseString("127.0.0.1", 0));
    ASSERT_OK(listener_.Init(0));
    ASSERT_OK(listener_.BindAndListen(listenAddr_, /*listen_queue_size=*/10));
    ASSERT_OK(listener_.GetSocketAddress(&listenAddr_));

    thread_ = thread([&] {
      pthread_ = pthread_self();
      pthreadSync_.CountDown();
      unique_ptr<Socket> sock(new Socket());
      Sockaddr remote;
      CHECK_OK(listener_.Accept(sock.get(), &remote, /*flags=*/0));

      TlsHandshake server;
      CHECK_OK(serverTls_.InitiateHandshake(TlsHandshakeType::Server, &server));
      CHECK_OK(doNegotiationSide(sock.get(), &server, "server"));
      CHECK_OK(server.finish(&sock));

      CHECK_OK(sock->SetRecvTimeout(kTimeout));
      unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
      // An "echo" loop for kEchoChunkSize byte buffers.
      while (!stop_) {
        size_t n;
        Status s = sock->BlockingRecv(
            buf.get(), kEchoChunkSize, &n, MonoTime::Now() + kTimeout);
        if (!s.ok()) {
          CHECK(stop_) << "unexpected error reading: " << s.ToString();
        }

        LOG(INFO) << "server echoing " << n << " bytes";
        size_t written;
        s = sock->BlockingWrite(
            buf.get(), n, &written, MonoTime::Now() + kTimeout);
        if (!s.ok()) {
          CHECK(stop_) << "unexpected error writing: " << s.ToString();
        }
        if (slowRead_) {
          SleepFor(MonoDelta::FromMilliseconds(10));
        }
      }

      CHECK_OK(listener_.Close());
    });
  }

  void enableSlowRead() {
    slowRead_ = true;
  }

  const Sockaddr& listenAddr() const {
    return listenAddr_;
  }

  bool stopped() const {
    return stop_;
  }

  void stop() {
    stop_ = true;
  }
  void join() {
    thread_.join();
  }

  const pthread_t& pthread() {
    pthreadSync_.Wait();
    return pthread_;
  }

 private:
  TlsContext serverTls_;
  Socket listener_;
  Sockaddr listenAddr_;
  thread thread_;
  pthread_t pthread_;
  CountDownLatch pthreadSync_;
  std::atomic<bool> stop_{false};

  bool slowRead_ = false;
};

void handler(int /* signal */) {}

TEST_F(TlsSocketTest, TestRecvFailure) {
  EchoServer server;
  server.start();
  unique_ptr<Socket> clientSock;
  NO_FATALS(connectClient(server.listenAddr(), &clientSock));
  unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);

  SleepFor(MonoDelta::FromMilliseconds(100));
  server.stop();

  size_t nwritten;
  ASSERT_OK(clientSock->BlockingWrite(
      buf.get(), kEchoChunkSize, &nwritten, MonoTime::Now() + kTimeout));
  size_t nread;

  ASSERT_OK(clientSock->BlockingRecv(
      buf.get(), kEchoChunkSize, &nread, MonoTime::Now() + kTimeout));

  Status s = clientSock->BlockingRecv(
      buf.get(), kEchoChunkSize, &nread, MonoTime::Now() + kTimeout);

  ASSERT_TRUE(!s.ok());
  ASSERT_TRUE(s.IsNetworkError());
  ASSERT_STR_MATCHES(
      s.message().ToString(),
      "BlockingRecv error: failed to read from "
      "TLS socket \\(remote: 127.0.0.1:[0-9]+\\): ");
}

// Test for failures to handle EINTR during TLS connection
// negotiation and data send/receive.
TEST_F(TlsSocketTest, TestTlsSocketInterrupted) {
  // Set up a no-op signal handler for SIGUSR2.
  struct sigaction sa, saOld;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = &handler;
  sigaction(SIGUSR2, &sa, &saOld);
  SCOPE_EXIT {
    sigaction(SIGUSR2, &saOld, nullptr);
  };

  EchoServer server;
  NO_FATALS(server.start());

  // Start a thread to send signals to the server thread.
  thread killer([&]() {
    while (!server.stopped()) {
      PCHECK(pthread_kill(server.pthread(), SIGUSR2) == 0);
      SleepFor(MonoDelta::FromMicroseconds(rand() % 10));
    }
  });
  SCOPE_EXIT {
    killer.join();
  };

  unique_ptr<Socket> clientSock;
  NO_FATALS(connectClient(server.listenAddr(), &clientSock));

  unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
  for (int i = 0; i < 10; i++) {
    SleepFor(MonoDelta::FromMilliseconds(1));
    size_t nwritten;
    ASSERT_OK(clientSock->BlockingWrite(
        buf.get(), kEchoChunkSize, &nwritten, MonoTime::Now() + kTimeout));
    size_t n;
    ASSERT_OK(clientSock->BlockingRecv(
        buf.get(), kEchoChunkSize, &n, MonoTime::Now() + kTimeout));
  }
  server.stop();
  ASSERT_OK(clientSock->Close());
  LOG(INFO) << "client done";
}

// Return an iovec containing the same data as the buffer 'buf' with the length
// 'len', but split into random-sized chunks. The chunks are sized randomly
// between 1 and 'maxChunkSize' bytes.
vector<struct iovec>
chunkIoVec(Random* rng, uint8_t* buf, int len, int maxChunkSize) {
  vector<struct iovec> ret;
  uint8_t* p = buf;
  int rem = len;
  while (rem > 0) {
    int len = rng->Uniform(maxChunkSize) + 1;
    len = std::min(len, rem);
    ret.push_back({p, static_cast<size_t>(len)});
    p += len;
    rem -= len;
  }
  return ret;
}

// Regression test for KUDU-2218, a bug in which Writev would improperly handle
// partial writes in non-blocking mode.
TEST_F(TlsSocketTest, TestNonBlockingWritev) {
  Random rng(getRandomSeed32());

  EchoServer server;
  server.enableSlowRead();
  NO_FATALS(server.start());

  unique_ptr<Socket> clientSock;
  NO_FATALS(connectClient(server.listenAddr(), &clientSock));

  unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
  unique_ptr<uint8_t[]> rbuf(new uint8_t[kEchoChunkSize]);
  randomString(buf.get(), kEchoChunkSize, &rng);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(clientSock->SetNonBlocking(true));

    // Prepare an IOV with the input data split into a bunch of randomly-sized
    // chunks.
    vector<struct iovec> iov =
        chunkIoVec(&rng, buf.get(), kEchoChunkSize, 1024 * 1024);

    // Loop calling writev until the iov is exhausted
    int rem = kEchoChunkSize;
    while (rem > 0) {
      CHECK(!iov.empty()) << rem;
      int64_t n;
      Status s = clientSock->Writev(&iov[0], iov.size(), &n);
      if (Socket::IsTemporarySocketError(s.posix_code())) {
        sched_yield();
        continue;
      }
      ASSERT_OK(s);
      ASSERT_LE(n, rem);
      rem -= n;
      ASSERT_GE(n, 0);
      while (n > 0) {
        if (n < iov[0].iov_len) {
          iov[0].iov_len -= n;
          iov[0].iov_base = reinterpret_cast<uint8_t*>(iov[0].iov_base) + n;
          n = 0;
        } else {
          n -= iov[0].iov_len;
          iov.erase(iov.begin());
        }
      }
    }
    LOG(INFO) << "client waiting";

    size_t n;
    ASSERT_OK(clientSock->SetNonBlocking(false));
    ASSERT_OK(clientSock->BlockingRecv(
        rbuf.get(), kEchoChunkSize, &n, MonoTime::Now() + kTimeout));
    LOG(INFO) << "client got response";

    ASSERT_EQ(0, memcmp(buf.get(), rbuf.get(), kEchoChunkSize));
  }

  server.stop();
  ASSERT_OK(clientSock->Close());
}

} // namespace security
} // namespace kudu
