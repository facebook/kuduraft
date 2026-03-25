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

#include "kudu/util/net/socket.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"

DEFINE_string(
    local_ip_for_outbound_sockets,
    "",
    "IP to bind to when making outgoing socket connections. "
    "This must be an IP address of the form A.B.C.D, not a hostname. "
    "Advanced parameter, subject to change.");
TAG_FLAG(local_ip_for_outbound_sockets, experimental);

DEFINE_bool(
    socket_inject_short_recvs,
    false,
    "Inject short recv() responses which return less data than "
    "requested");
TAG_FLAG(socket_inject_short_recvs, hidden);
TAG_FLAG(socket_inject_short_recvs, unsafe);

using std::string;

// Min sock buf allowed by kernel, see socket(7)
constexpr int kMinSockBuf = 1024;

namespace kudu {

Socket::Socket() : fd_(-1) {}

Socket::Socket(int fd) : fd_(fd) {}

void Socket::Reset(int fd) {
  ignoreResult(Close());
  fd_ = fd;
}

int Socket::Release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

Socket::~Socket() {
  ignoreResult(Close());
}

Status Socket::Close() {
  if (fd_ < 0) {
    return Status::OK();
  }
  int fd = fd_;
  int ret;
  RETRY_ON_EINTR(ret, ::close(fd));
  if (ret < 0) {
    int err = errno;
    return Status::NetworkError("close error", errnoToString(err), err);
  }
  fd_ = -1;
  return Status::OK();
}

Status Socket::Shutdown(bool shutRead, bool shutWrite) {
  DCHECK_GE(fd_, 0);
  int flags = 0;
  if (shutRead && shutWrite) {
    flags |= SHUT_RDWR;
  } else if (shutRead) {
    flags |= SHUT_RD;
  } else if (shutWrite) {
    flags |= SHUT_WR;
  }
  if (::shutdown(fd_, flags) < 0) {
    int err = errno;
    return Status::NetworkError("shutdown error", errnoToString(err), err);
  }
  return Status::OK();
}

int Socket::GetFd() const {
  return fd_;
}

bool Socket::IsTemporarySocketError(int err) {
  return ((err == EAGAIN) || (err == EWOULDBLOCK) || (err == EINTR));
}

#if defined(__linux__)

Status Socket::Init(int flags) {
  int nonblockingFlag = (flags & kFlagNonblocking) ? SOCK_NONBLOCK : 0;
  Reset(::socket(AF_INET6, SOCK_STREAM | SOCK_CLOEXEC | nonblockingFlag, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError(
        "error opening socket", errnoToString(err), err);
  }

  return Status::OK();
}

#else

Status Socket::Init(int flags) {
  Reset(::socket(AF_INET6, SOCK_STREAM, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError(
        "error opening socket", errnoToString(err), err);
  }
  RETURN_NOT_OK(SetNonBlocking(flags & kFlagNonblocking));
  RETURN_NOT_OK(SetCloseOnExec());

  // Disable SIGPIPE.
  int set = 1;
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(SOL_SOCKET, SO_NOSIGPIPE, set), "failed to set SO_NOSIGPIPE");
  return Status::OK();
}

#endif // defined(__linux__)

Status Socket::SetNoDelay(bool enabled) {
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(IPPROTO_TCP, TCP_NODELAY, flag), "failed to set TCP_NODELAY");
  return Status::OK();
}

Status Socket::SetTcpCork(bool enabled) {
#if defined(__linux__)
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(IPPROTO_TCP, TCP_CORK, flag), "failed to set TCP_CORK");
#endif // defined(__linux__)
  // TODO(unknown): Use TCP_NOPUSH for OSX if perf becomes an issue.
  return Status::OK();
}

Status Socket::SetNonBlocking(bool enabled) {
  int curFlags = ::fcntl(fd_, F_GETFL, 0);
  if (curFlags == -1) {
    int err = errno;
    return Status::NetworkError(
        fmt::format("Failed to get file status flags on fd {}", fd_),
        errnoToString(err),
        err);
  }
  int newFlags = (enabled) ? (curFlags | O_NONBLOCK) : (curFlags & ~O_NONBLOCK);
  if (::fcntl(fd_, F_SETFL, newFlags) == -1) {
    int err = errno;
    if (enabled) {
      return Status::NetworkError(
          fmt::format("Failed to set O_NONBLOCK on fd {}", fd_),
          errnoToString(err),
          err);
    } else {
      return Status::NetworkError(
          fmt::format("Failed to clear O_NONBLOCK on fd {}", fd_),
          errnoToString(err),
          err);
    }
  }
  return Status::OK();
}

Status Socket::IsNonBlocking(bool* isNonblock) const {
  int curFlags = ::fcntl(fd_, F_GETFL, 0);
  if (curFlags == -1) {
    int err = errno;
    return Status::NetworkError(
        fmt::format("Failed to get file status flags on fd {}", fd_),
        errnoToString(err),
        err);
  }
  *isNonblock = ((curFlags & O_NONBLOCK) != 0);
  return Status::OK();
}

Status Socket::SetCloseOnExec() {
  int curFlags = fcntl(fd_, F_GETFD, 0);
  if (curFlags == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError(
        "fcntl(F_GETFD) error", errnoToString(err), err);
  }
  if (fcntl(fd_, F_SETFD, curFlags | FD_CLOEXEC) == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError(
        "fcntl(F_SETFD) error", errnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::SetSendTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_SNDTIMEO, "SO_SNDTIMEO", timeout);
}

Status Socket::SetRecvTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_RCVTIMEO, "SO_RCVTIMEO", timeout);
}

Status Socket::SetReuseAddr(bool flag) {
  int intFlag = flag ? 1 : 0;
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(SOL_SOCKET, SO_REUSEADDR, intFlag),
      "failed to set SO_REUSEADDR");
  return Status::OK();
}

Status Socket::BindAndListen(const Sockaddr& sockaddr, int listenQueueSize) {
  RETURN_NOT_OK(SetReuseAddr(true));
  RETURN_NOT_OK(Bind(sockaddr));
  RETURN_NOT_OK(Listen(listenQueueSize));
  return Status::OK();
}

Status Socket::Listen(int listenQueueSize) {
  if (listen(fd_, listenQueueSize)) {
    int err = errno;
    return Status::NetworkError("listen() error", errnoToString(err));
  }
  return Status::OK();
}

Status Socket::GetSocketAddress(Sockaddr* curAddr) const {
  struct sockaddr_in6 sin;
  socklen_t len = sizeof(sin);
  DCHECK_GE(fd_, 0);
  if (::getsockname(fd_, reinterpret_cast<struct sockaddr*>(&sin), &len) ==
      -1) {
    int err = errno;
    return Status::NetworkError("getsockname error", errnoToString(err), err);
  }
  *curAddr = sin;
  return Status::OK();
}

Status Socket::GetPeerAddress(Sockaddr* curAddr) const {
  struct sockaddr_in6 sin;
  socklen_t len = sizeof(sin);
  DCHECK_GE(fd_, 0);
  if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&sin), &len) ==
      -1) {
    int err = errno;
    return Status::NetworkError("getpeername error", errnoToString(err), err);
  }
  *curAddr = sin;
  return Status::OK();
}

bool Socket::IsLoopbackConnection() const {
  Sockaddr local, remote;
  if (!GetSocketAddress(&local).ok()) {
    return false;
  }
  if (!GetPeerAddress(&remote).ok()) {
    return false;
  }

  // Compare without comparing ports.
  local.set_port(0);
  remote.set_port(0);
  return local == remote;
}

Status Socket::Bind(const Sockaddr& bindAddr) {
  struct sockaddr_in6 addr = bindAddr.addr();

  DCHECK_GE(fd_, 0);
  if (PREDICT_FALSE(::bind(fd_, (struct sockaddr*)&addr, sizeof(addr)))) {
    int err = errno;
    Status s = Status::NetworkError(
        fmt::format(
            "error binding socket to {}: {}",
            bindAddr.ToString(),
            errnoToString(err)),
        Slice(),
        err);

    if (s.IsNetworkError() && s.posixCode() == EADDRINUSE &&
        bindAddr.port() != 0) {
      tryRunLsof(bindAddr);
    }
    return s;
  }

  return Status::OK();
}

Status Socket::Accept(Socket* newConn, Sockaddr* remote, int flags) {
  TRACE_EVENT0("net", "Socket::Accept");
  struct sockaddr_in6 addr;
  socklen_t olen = sizeof(addr);
  DCHECK_GE(fd_, 0);
#if defined(__linux__)
  int acceptFlags = SOCK_CLOEXEC;
  if (flags & kFlagNonblocking) {
    acceptFlags |= SOCK_NONBLOCK;
  }
  int fd = -1;
  RETRY_ON_EINTR(fd, accept4(fd_, (struct sockaddr*)&addr, &olen, acceptFlags));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept4(2) error", errnoToString(err), err);
  }
  newConn->Reset(fd);

#else
  int fd = -1;
  RETRY_ON_EINTR(fd, accept(fd_, (struct sockaddr*)&addr, &olen));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept(2) error", errnoToString(err), err);
  }
  newConn->Reset(fd);
  RETURN_NOT_OK(newConn->SetNonBlocking(flags & kFlagNonblocking));
  RETURN_NOT_OK(newConn->SetCloseOnExec());
#endif // defined(__linux__)

  *remote = addr;
  TRACE_EVENT_INSTANT1(
      "net",
      "Accepted",
      TRACE_EVENT_SCOPE_THREAD,
      "remote",
      remote->ToString());
  return Status::OK();
}

Status Socket::BindForOutgoingConnection() {
  Sockaddr bindHost;
  Status s = bindHost.ParseString(FLAGS_local_ip_for_outbound_sockets, 0);
  CHECK(s.ok() && bindHost.port() == 0)
      << "Invalid local IP set for 'local_ip_for_outbound_sockets': '"
      << FLAGS_local_ip_for_outbound_sockets << "': " << s.ToString();

  RETURN_NOT_OK(Bind(bindHost));
  return Status::OK();
}

Status Socket::Connect(const Sockaddr& remote) {
  TRACE_EVENT1("net", "Socket::Connect", "remote", remote.ToString());
  if (PREDICT_FALSE(!FLAGS_local_ip_for_outbound_sockets.empty())) {
    RETURN_NOT_OK(BindForOutgoingConnection());
  }

  struct sockaddr_in6 addr;
  memcpy(&addr, &remote.addr(), sizeof(sockaddr_in6));
  DCHECK_GE(fd_, 0);
  int ret;
  RETRY_ON_EINTR(
      ret,
      ::connect(
          fd_, reinterpret_cast<const struct sockaddr*>(&addr), sizeof(addr)));
  if (ret < 0) {
    int err = errno;
    return Status::NetworkError("connect(2) error", errnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::GetSockError() const {
  int val = 0, ret;
  socklen_t valLen = sizeof(val);
  DCHECK_GE(fd_, 0);
  ret = ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &val, &valLen);
  if (ret) {
    int err = errno;
    return Status::NetworkError(
        "getsockopt(SO_ERROR) failed", errnoToString(err), err);
  }
  if (val != 0) {
    return Status::NetworkError(errnoToString(val), Slice(), val);
  }
  return Status::OK();
}

Status Socket::Write(const uint8_t* buf, int32_t amt, int32_t* nwritten) {
  if (amt <= 0) {
    return Status::NetworkError(
        fmt::format("invalid send of {} bytes", amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, ::send(fd_, buf, amt, MSG_NOSIGNAL));
  if (res < 0) {
    int err = errno;
    return Status::NetworkError("write error", errnoToString(err), err);
  }
  *nwritten = res;
  return Status::OK();
}

Status
Socket::Writev(const struct ::iovec* iov, int iovLen, int64_t* nwritten) {
  if (PREDICT_FALSE(iovLen <= 0)) {
    return Status::NetworkError(
        fmt::format("writev: invalid io vector length of {}", iovLen),
        Slice(),
        EINVAL);
  }
  DCHECK_GE(fd_, 0);

  struct msghdr msg;
  memset(&msg, 0, sizeof(struct msghdr));
  msg.msg_iov = const_cast<iovec*>(iov);
  msg.msg_iovlen = iovLen;
  ssize_t res;
  RETRY_ON_EINTR(res, ::sendmsg(fd_, &msg, MSG_NOSIGNAL));
  if (PREDICT_FALSE(res < 0)) {
    int err = errno;
    return Status::NetworkError("sendmsg error", errnoToString(err), err);
  }

  *nwritten = res;
  return Status::OK();
}

// Mostly follows writen() from Stevens (2004) or Kerrisk (2010).
Status Socket::BlockingWrite(
    const uint8_t* buf,
    size_t buflen,
    size_t* nwritten,
    const MonoTime& deadline) {
  DCHECK_LE(buflen, std::numeric_limits<int32_t>::max())
      << "Writes > INT32_MAX not supported";
  DCHECK(nwritten);

  size_t totWritten = 0;
  while (totWritten < buflen) {
    int32_t incNumWritten = 0;
    int32_t numToWrite = buflen - totWritten;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut("BlockingWrite timed out");
    }
    RETURN_NOT_OK(SetSendTimeout(timeout));
    Status s = Write(buf, numToWrite, &incNumWritten);
    totWritten += incNumWritten;
    buf += incNumWritten;
    *nwritten = totWritten;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posixCode() == EINTR) {
        continue;
      }
      if (s.posixCode() == EAGAIN) {
        return Status::TimedOut("");
      }
      return s.CloneAndPrepend("BlockingWrite error");
    }
    if (PREDICT_FALSE(incNumWritten == 0)) {
      // Shouldn't happen on Linux with a blocking socket. Maybe other Unices.
      break;
    }
  }

  if (totWritten < buflen) {
    return Status::IOError(
        "Wrote zero bytes on a BlockingWrite() call",
        fmt::format("Transferred {} of {} bytes", totWritten, buflen));
  }
  return Status::OK();
}

Status Socket::Recv(uint8_t* buf, int32_t amt, int32_t* nread) {
  if (amt <= 0) {
    return Status::NetworkError(
        fmt::format("invalid recv of {} bytes", amt), Slice(), EINVAL);
  }

  // The recv() call can return fewer than the requested number of bytes.
  // Especially when 'amt' is small, this is very unlikely to happen in
  // the context of unit tests. So, we provide an injection hook which
  // simulates the same behavior.
  if (PREDICT_FALSE(FLAGS_socket_inject_short_recvs && amt > 1)) {
    Random r(getRandomSeed32());
    amt = 1 + r.Uniform(amt - 1);
  }

  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, recv(fd_, buf, amt, 0));
  if (res <= 0) {
    Sockaddr remote;
    GetPeerAddress(&remote);
    if (res == 0) {
      string errorMessage =
          fmt::format("recv got EOF from {}", remote.ToString());
      return Status::NetworkError(errorMessage, Slice(), ESHUTDOWN);
    }
    int err = errno;
    string errorMessage = fmt::format("recv error from {}", remote.ToString());
    return Status::NetworkError(errorMessage, errnoToString(err), err);
  }
  *nread = res;
  return Status::OK();
}

// Mostly follows readn() from Stevens (2004) or Kerrisk (2010).
// One place where we deviate: we consider EOF a failure if < amt bytes are
// read.
Status Socket::BlockingRecv(
    uint8_t* buf,
    size_t amt,
    size_t* nread,
    const MonoTime& deadline) {
  DCHECK_LE(amt, std::numeric_limits<int32_t>::max())
      << "Reads > INT32_MAX not supported";
  DCHECK(nread);
  size_t totRead = 0;
  while (totRead < amt) {
    int32_t incNumRead = 0;
    int32_t numToRead = amt - totRead;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut("");
    }
    RETURN_NOT_OK(SetRecvTimeout(timeout));
    Status s = Recv(buf, numToRead, &incNumRead);
    totRead += incNumRead;
    buf += incNumRead;
    *nread = totRead;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posixCode() == EINTR) {
        continue;
      }
      if (s.posixCode() == EAGAIN) {
        return Status::TimedOut("");
      }
      return s.CloneAndPrepend("BlockingRecv error");
    }
    if (PREDICT_FALSE(incNumRead == 0)) {
      // EOF.
      break;
    }
  }

  if (PREDICT_FALSE(totRead < amt)) {
    return Status::IOError(
        "Read zero bytes on a blocking Recv() call",
        fmt::format("Transferred {} of {} bytes", totRead, amt));
  }
  return Status::OK();
}

Status Socket::Peek(
    uint8_t* buf,
    size_t amt,
    size_t* nread,
    const MonoTime& deadline) {
  DCHECK_LE(amt, std::numeric_limits<int32_t>::max())
      << "Reads > INT32_MAX not supported";
  DCHECK(nread);
  DCHECK_GE(fd_, 0);

  MonoDelta timeout = deadline - MonoTime::Now();
  if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
    return Status::TimedOut("");
  }
  RETURN_NOT_OK(SetRecvTimeout(timeout));

  int res;
  RETRY_ON_EINTR(res, recv(fd_, buf, amt, MSG_PEEK));
  if (res <= 0) {
    Sockaddr remote;
    GetPeerAddress(&remote);
    if (res == 0) {
      string errorMessage =
          fmt::format("recv got EOF from {}", remote.ToString());
      return Status::NetworkError(errorMessage, Slice(), ESHUTDOWN);
    }
    int err = errno;
    string errorMessage = fmt::format("recv error from {}", remote.ToString());
    return Status::NetworkError(errorMessage, errnoToString(err), err);
  } else if (amt != res) {
    string errorMessage = fmt::format("Peek returned {} of {} bytes", amt, res);
    return Status::NetworkError(errorMessage);
  }
  *nread = res;
  return Status::OK();
}

Status Socket::SetSockBuf(int opt, const char* optname, int bufSize) {
  if (PREDICT_FALSE(bufSize < kMinSockBuf)) {
    return Status::InvalidArgument(
        fmt::format("{} cannot be lower than {}", optname, kMinSockBuf),
        std::to_string(bufSize));
  }
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(SOL_SOCKET, opt, bufSize),
      fmt::format("failed to set {} to {}", optname, bufSize));

  return Status::OK();
}

Status
Socket::SetTimeout(int opt, const char* optname, const MonoDelta& timeout) {
  if (PREDICT_FALSE(timeout.ToNanoseconds() < 0)) {
    return Status::InvalidArgument(
        "Timeout specified as negative to SetTimeout", timeout.ToString());
  }
  struct timeval tv;
  timeout.ToTimeVal(&tv);
  RETURN_NOT_OK_PREPEND(
      SetSockOpt(SOL_SOCKET, opt, tv),
      fmt::format(
          "failed to set socket option {} to {}", optname, timeout.ToString()));
  return Status::OK();
}

template <typename T>
Status Socket::SetSockOpt(int level, int option, const T& value) {
  if (::setsockopt(fd_, level, option, &value, sizeof(T)) == -1) {
    int err = errno;
    return Status::NetworkError(errnoToString(err), Slice(), err);
  }
  return Status::OK();
}

} // namespace kudu
