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

#include "kudu/util/subprocess.h"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <ev++.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <fmt/core.h>
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/signal.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;

namespace kudu {

// Make glog's STL-compatible operators visible inside this namespace.
using ::operator<<;

namespace {

static double kProcessWaitTimeoutSeconds = 5.0;

static const char* kProcSelfFd =
#if defined(__APPLE__)
    "/dev/fd";
#else
    "/proc/self/fd";
#endif // defined(__APPLE__)

#if defined(__linux__)
#define READDIR readdir64
#define DIRENT dirent64
#else
#define READDIR readdir
#define DIRENT dirent
#endif

// Since opendir() calls malloc(), this must be called before fork().
// This function is not async-signal-safe.
Status openProcFdDir(DIR** dir) {
  *dir = opendir(kProcSelfFd);
  if (PREDICT_FALSE(dir == nullptr)) {
    return Status::IOError(
        fmt::format("opendir(\"{}\") failed", kProcSelfFd),
        errnoToString(errno),
        errno);
  }
  return Status::OK();
}

// Close the directory stream opened by openProcFdDir().
// This function is not async-signal-safe.
void closeProcFdDir(DIR* dir) {
  if (PREDICT_FALSE(closedir(dir) == -1)) {
    LOG(WARNING) << "Unable to close fd dir: "
                 << Status::IOError(
                        fmt::format("closedir(\"{}\") failed", kProcSelfFd),
                        errnoToString(errno),
                        errno)
                        .ToString();
  }
}

// Close all open file descriptors other than stdin, stderr, stdout.
// Expects a directory stream created by openProcFdDir() as a parameter.
// This function is called after fork() and must not call malloc().
// The rule of thumb is to only call async-signal-safe functions in such cases
// if at all possible.
void closeNonStandardFds(DIR* fdDir) {
  // This is implemented by iterating over the open file descriptors
  // rather than using sysconf(SC_OPEN_MAX) -- the latter is error prone
  // since it may not represent the highest open fd if the fd soft limit
  // has changed since the process started. This should also be faster
  // since iterating over all possible fds is likely to cause 64k+ syscalls
  // in typical configurations.
  //
  // Note also that this doesn't use any of the Env utility functions, to
  // make it as lean and mean as possible -- this runs in the subprocess
  // after a fork, so there's some possibility that various global locks
  // inside malloc() might be held, so allocating memory is a no-no.
  PCHECK(fdDir != nullptr);
  int dirFd = dirfd(fdDir);

  struct DIRENT* ent;
  // readdir64() is not reentrant (it uses a static buffer) and it also
  // locks fd_dir->lock, so it must not be called in a multi-threaded
  // environment and is certainly not async-signal-safe.
  // However, it appears to be safe to call right after fork(), since only one
  // thread exists in the child process at that time. It also does not call
  // malloc() or free(). We could use readdir64_r() instead, but all that
  // buys us is reentrancy, and not async-signal-safety, due to the use of
  // dir->lock, so seems not worth the added complexity in lifecycle & plumbing.
  while ((ent = READDIR(fdDir)) != nullptr) {
    uint32_t fd;
    if (!safe_strtou32(ent->d_name, &fd)) {
      continue;
    }
    if (!(fd == STDIN_FILENO || fd == STDOUT_FILENO || fd == STDERR_FILENO ||
          fd == dirFd)) {
      int ret;
      RETRY_ON_EINTR(ret, close(fd));
    }
  }
}

void redirectToDevNull(int fd) {
  // We must not close stderr or stdout, because then when a new file
  // descriptor is opened, it might reuse the closed file descriptor's number
  // (we always allocate the lowest available file descriptor number).
  //
  // Instead, we open /dev/null as a new file descriptor, then use dup2() to
  // atomically close 'fd' and reuse its file descriptor number as an open file
  // handle to /dev/null.
  //
  // It is expected that the file descriptor allocated when opening /dev/null
  // will be closed when the child process closes all of its "non-standard"
  // file descriptors later on.
  int devNull;
  RETRY_ON_EINTR(devNull, open("/dev/null", O_WRONLY));
  if (devNull < 0) {
    PLOG(WARNING) << "failed to open /dev/null";
  } else {
    int ret;
    RETRY_ON_EINTR(ret, dup2(devNull, fd));
    PCHECK(ret);
  }
}

// Stateful libev watcher to help ReadFdsFully().
class ReadFdsFullyHelper {
 public:
  ReadFdsFullyHelper(string progname, ev::dynamic_loop* loop, int fd)
      : progname_(std::move(progname)) {
    // Bind the watcher to the provided loop, to this functor, and to the
    // readable fd.
    watcher_.set(*loop);
    watcher_.set(this);
    watcher_.set(fd, ev::READ);

    // The watcher will now be polled when its loop is run.
    watcher_.start();
  }

  void operator()(ev::io& w, int revents) {
    DCHECK_EQ(ev::READ, revents);

    char buf[1024];
    ssize_t n;
    RETRY_ON_EINTR(n, read(w.fd, buf, arraysize(buf)));
    if (n == 0) {
      // EOF, stop watching.
      w.stop();
    } else if (n < 0) {
      // A fatal error. Store it and stop watching.
      status_ = Status::IOError(
          "IO error reading from " + progname_, errnoToString(errno), errno);
      w.stop();
    } else {
      // Add our bytes and keep watching.
      output_.append(buf, n);
    }
  }

  const Status& status() const {
    return status_;
  }
  const string& output() const {
    return output_;
  }

 private:
  const string progname_;

  ev::io watcher_;
  string output_;
  Status status_;
};

// Reads from all descriptors in 'fds' until EOF on all of them. If any read
// yields an error, it is returned. Otherwise, 'out' contains the bytes read
// for each fd, in the same order as was in 'fds'.
Status readFdsFully(
    const string& progname,
    const vector<int>& fds,
    vector<string>* out) {
  ev::dynamic_loop loop;

  // Set up a watcher for each fd.
  vector<unique_ptr<ReadFdsFullyHelper>> helpers;
  for (int fd : fds) {
    helpers.emplace_back(new ReadFdsFullyHelper(progname, &loop, fd));
  }

  // This will read until all fds return EOF.
  loop.run();

  // Check for failures.
  for (const auto& h : helpers) {
    if (!h->status().ok()) {
      return h->status();
    }
  }

  // No failures; write the output to the caller.
  for (const auto& h : helpers) {
    out->push_back(h->output());
  }
  return Status::OK();
}

} // anonymous namespace

Subprocess::Subprocess(vector<string> argv, int sigOnDestruct)
    : program_(argv[0]),
      argv_(std::move(argv)),
      state_(kNotStarted),
      childPid_(-1),
      fdState_(),
      childFds_(),
      sigOnDestruct_(sigOnDestruct) {
  // By convention, the first argument in argv is the base name of the program.
  argv_[0] = BaseName(argv_[0]);

  fdState_[STDIN_FILENO] = kPiped;
  fdState_[STDOUT_FILENO] = kShared;
  fdState_[STDERR_FILENO] = kShared;
  childFds_[STDIN_FILENO] = -1;
  childFds_[STDOUT_FILENO] = -1;
  childFds_[STDERR_FILENO] = -1;
}

Subprocess::~Subprocess() {
  if (state_ == kRunning) {
    LOG(WARNING) << fmt::format(
        "Child process {} ({}) was orphaned. Sending signal {}...",
        childPid_,
        JoinStrings(argv_, " "),
        sigOnDestruct_);
    WARN_NOT_OK(
        KillAndWait(sigOnDestruct_),
        fmt::format("Failed to KillAndWait() with signal {}", sigOnDestruct_));
  }

  for (int i = 0; i < 3; ++i) {
    if (fdState_[i] == kPiped && childFds_[i] >= 0) {
      int ret;
      RETRY_ON_EINTR(ret, close(childFds_[i]));
    }
  }
}

#if defined(__APPLE__)
static int pipe2(int pipefd[2], int flags) {
  DCHECK_EQ(O_CLOEXEC, flags);

  int newFds[2];
  if (pipe(newFds) == -1) {
    return -1;
  }
  if (fcntl(newFds[0], F_SETFD, O_CLOEXEC) == -1) {
    int ret;
    RETRY_ON_EINTR(ret, close(newFds[0]));
    RETRY_ON_EINTR(ret, close(newFds[1]));
    return -1;
  }
  if (fcntl(newFds[1], F_SETFD, O_CLOEXEC) == -1) {
    int ret;
    RETRY_ON_EINTR(ret, close(newFds[0]));
    RETRY_ON_EINTR(ret, close(newFds[1]));
    return -1;
  }
  pipefd[0] = newFds[0];
  pipefd[1] = newFds[1];
  return 0;
}
#endif

Status Subprocess::Start() {
  VLOG(2) << "Invoking command: " << argv_;
  if (state_ != kNotStarted) {
    const string errStr = fmt::format("{}: illegal sub-process state", state_);
    LOG(DFATAL) << errStr;
    return Status::IllegalState(errStr);
  }
  if (argv_.empty()) {
    return Status::InvalidArgument("argv must have at least one elem");
  }

  // We explicitly set SIGPIPE to SIG_IGN here because we are using UNIX pipes.
  ignoreSigPipe();

  vector<char*> argvPtrs;
  for (const string& arg : argv_) {
    argvPtrs.push_back(const_cast<char*>(arg.c_str()));
  }
  argvPtrs.push_back(nullptr);

  // Pipe from caller process to child's stdin
  // [0] = stdin for child, [1] = how parent writes to it
  int childStdin[2] = {-1, -1};
  if (fdState_[STDIN_FILENO] == kPiped) {
    PCHECK(pipe2(childStdin, O_CLOEXEC) == 0);
  }
  // Pipe from child's stdout back to caller process
  // [0] = how parent reads from child's stdout, [1] = how child writes to it
  int childStdout[2] = {-1, -1};
  if (fdState_[STDOUT_FILENO] == kPiped) {
    PCHECK(pipe2(childStdout, O_CLOEXEC) == 0);
  }
  // Pipe from child's stderr back to caller process
  // [0] = how parent reads from child's stderr, [1] = how child writes to it
  int childStderr[2] = {-1, -1};
  if (fdState_[STDERR_FILENO] == kPiped) {
    PCHECK(pipe2(childStderr, O_CLOEXEC) == 0);
  }
  // The synchronization pipe: this trick is to make sure the parent returns
  // control only after the child process has invoked execvp().
  int syncPipe[2];
  PCHECK(pipe2(syncPipe, O_CLOEXEC) == 0);

  DIR* fdDir = nullptr;
  RETURN_NOT_OK_PREPEND(openProcFdDir(&fdDir), "Unable to open fd dir");
  unique_ptr<DIR, std::function<void(DIR*)>> fdDirCloser(fdDir, closeProcFdDir);
  int ret;
  RETRY_ON_EINTR(ret, fork());
  if (ret == -1) {
    return Status::RuntimeError("Unable to fork", errnoToString(errno), errno);
  }
  if (ret == 0) { // We are the child
    // Send the child a SIGTERM when the parent dies. This is done as early
    // as possible in the child's life to prevent any orphaning whatsoever
    // (e.g. from KUDU-402).
#if defined(__linux__)
    // TODO: prctl(PR_SET_PDEATHSIG) is Linux-specific, look into portable ways
    // to prevent orphans when parent is killed.
    prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif

    // stdin
    if (fdState_[STDIN_FILENO] == kPiped) {
      int dup2Ret;
      RETRY_ON_EINTR(dup2Ret, dup2(childStdin[0], STDIN_FILENO));
      PCHECK(dup2Ret == STDIN_FILENO);
    } else {
      DCHECK_EQ(kShared, fdState_[STDIN_FILENO]);
    }

    // stdout
    switch (fdState_[STDOUT_FILENO]) {
      case kPiped: {
        int dup2Ret;
        RETRY_ON_EINTR(dup2Ret, dup2(childStdout[1], STDOUT_FILENO));
        PCHECK(dup2Ret == STDOUT_FILENO);
        break;
      }
      case kDisabled: {
        redirectToDevNull(STDOUT_FILENO);
        break;
      }
      default:
        DCHECK_EQ(kShared, fdState_[STDOUT_FILENO]);
        break;
    }

    // stderr
    switch (fdState_[STDERR_FILENO]) {
      case kPiped: {
        int dup2Ret;
        RETRY_ON_EINTR(dup2Ret, dup2(childStderr[1], STDERR_FILENO));
        PCHECK(dup2Ret == STDERR_FILENO);
        break;
      }
      case kDisabled: {
        redirectToDevNull(STDERR_FILENO);
        break;
      }
      default:
        DCHECK_EQ(kShared, fdState_[STDERR_FILENO]);
        break;
    }

    // Close the read side of the sync pipe;
    // the write side should be closed upon execvp().
    int closeRet;
    RETRY_ON_EINTR(closeRet, close(syncPipe[0]));
    PCHECK(closeRet == 0);

    closeNonStandardFds(fdDir);

    // Ensure we are not ignoring or blocking signals in the child process.
    resetAllSignalMasksToUnblocked();

    // Reset the disposition of SIGPIPE to SIG_DFL because we routinely set its
    // disposition to SIG_IGN via ignoreSigPipe(). At the time of writing, we
    // don't explicitly ignore any other signals in Kudu.
    resetSigPipeHandlerToDefault();

    // Set the current working directory of the subprocess.
    if (!cwd_.empty()) {
      PCHECK(chdir(cwd_.c_str()) == 0);
    }

    // Set the environment for the subprocess. This is more portable than
    // using execvpe(), which doesn't exist on OS X. We rely on the 'p'
    // variant of exec to do $PATH searching if the executable specified
    // by the caller isn't an absolute path.
    for (const auto& env : env_) {
      ignoreResult(
          setenv(env.first.c_str(), env.second.c_str(), 1 /* overwrite */));
    }

    execvp(program_.c_str(), &argvPtrs[0]);
    int err = errno;
    PLOG(ERROR) << "Couldn't exec " << program_;
    _exit(err);
  } else {
    // We are the parent
    childPid_ = ret;
    // Close child's side of the pipes
    int closeRet;
    if (fdState_[STDIN_FILENO] == kPiped) {
      RETRY_ON_EINTR(closeRet, close(childStdin[0]));
    }
    if (fdState_[STDOUT_FILENO] == kPiped) {
      RETRY_ON_EINTR(closeRet, close(childStdout[1]));
    }
    if (fdState_[STDERR_FILENO] == kPiped) {
      RETRY_ON_EINTR(closeRet, close(childStderr[1]));
    }
    // Keep parent's side of the pipes
    childFds_[STDIN_FILENO] = childStdin[1];
    childFds_[STDOUT_FILENO] = childStdout[0];
    childFds_[STDERR_FILENO] = childStderr[0];

    // Wait for the child process to invoke execvp(). The trick involves
    // a pipe with O_CLOEXEC option for its descriptors. The parent process
    // performs blocking read from the pipe while the write side of the pipe
    // is kept open by the child (it does not write any data, though). The write
    // side of the pipe is closed when the child invokes execvp(). At that
    // point, the parent should receive EOF, i.e. read() should return 0.
    {
      // Close the write side of the sync pipe. It's crucial to make sure
      // it succeeds otherwise the blocking read() below might wait forever
      // even if the child process has closed the pipe.
      RETRY_ON_EINTR(closeRet, close(syncPipe[1]));
      PCHECK(closeRet == 0);
      while (true) {
        uint8_t buf;
        int err = 0;
        int rc;
        RETRY_ON_EINTR(rc, read(syncPipe[0], &buf, 1));
        if (rc == -1) {
          err = errno;
        }
        RETRY_ON_EINTR(closeRet, close(syncPipe[0]));
        PCHECK(closeRet == 0);
        if (rc == 0) {
          // That's OK -- expecting EOF from the other side of the pipe.
          break;
        } else if (rc == -1) {
          // Other errors besides EINTR are not expected.
          return Status::RuntimeError(
              "Unexpected error from the sync pipe", errnoToString(err), err);
        }
        // No data is expected from the sync pipe.
        LOG(FATAL) << fmt::format("{}: unexpected data from the sync pipe", rc);
      }
    }
  }

  state_ = kRunning;
  return Status::OK();
}

Status Subprocess::Wait(int* waitStatus) {
  return DoWait(waitStatus, kBlocking);
}

Status Subprocess::WaitNoBlock(int* waitStatus) {
  return DoWait(waitStatus, kNonBlocking);
}

Status Subprocess::GetProcfsState(int pid, ProcfsState* state) {
  faststring data;
  string filename = fmt::format("/proc/{}/stat", pid);
  RETURN_NOT_OK(ReadFileToString(Env::Default(), filename, &data));

  // The part of /proc/<pid>/stat that's relevant for us looks like this:
  //
  //   "16009 (subprocess-test) R ..."
  //
  // The first number is the PID, the string in the parens in the command, and
  // the single letter afterwards is the process' state.
  //
  // To extract the state, we scan backwards looking for the last ')', then
  // increment past it and the separating space. This is safer than scanning
  // forward as it properly handles commands containing parens.
  string dataStr = data.ToString();
  const char* endParens = strrchr(dataStr.c_str(), ')');
  if (endParens == nullptr) {
    return Status::RuntimeError(
        fmt::format("unexpected layout in {}", filename));
  }
  char procState = endParens[2];

  switch (procState) {
    case 'T':
      *state = ProcfsState::Paused;
      break;
    default:
      *state = ProcfsState::Running;
      break;
  }
  return Status::OK();
}

Status Subprocess::Kill(int signal) {
  if (state_ != kRunning) {
    const string errStr = "Sub-process is not running";
    LOG(DFATAL) << errStr;
    return Status::IllegalState(errStr);
  }
  if (kill(childPid_, signal) != 0) {
    return Status::RuntimeError("Unable to kill", errnoToString(errno), errno);
  }

  // Signal delivery is often asynchronous. For some signals, we try to wait
  // for the process to actually change state, using /proc/<pid>/stat as a
  // guide. This is best-effort.
  ProcfsState desiredState;
  switch (signal) {
    case SIGSTOP:
      desiredState = ProcfsState::Paused;
      break;
    case SIGCONT:
      desiredState = ProcfsState::Running;
      break;
    default:
      return Status::OK();
  }
  Stopwatch sw;
  sw.start();
  do {
    ProcfsState currentState;
    if (!GetProcfsState(childPid_, &currentState).ok()) {
      // There was some error parsing /proc/<pid>/stat (or perhaps it doesn't
      // exist on this platform).
      return Status::OK();
    }
    if (currentState == desiredState) {
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  } while (sw.elapsed().wall_seconds() < kProcessWaitTimeoutSeconds);
  return Status::OK();
}

Status Subprocess::KillAndWait(int signal) {
  string procname = fmt::format("{} (pid {})", argv0(), pid());

  // This is a fatal error because all errors in Kill() are signal-independent,
  // so Kill(SIGKILL) is just as likely to fail if this did.
  RETURN_NOT_OK_PREPEND(
      Kill(signal),
      fmt::format("Failed to send signal {} to {}", signal, procname));
  if (signal == SIGKILL) {
    RETURN_NOT_OK_PREPEND(
        Wait(), fmt::format("Failed to wait on {}", procname));
  } else {
    Status s;
    Stopwatch sw;
    sw.start();
    do {
      s = WaitNoBlock();
      if (s.ok()) {
        break;
      } else if (!s.IsTimedOut()) {
        // An unexpected error in WaitNoBlock() is likely to manifest
        // repeatedly, so there's no point in retrying this.
        RETURN_NOT_OK_PREPEND(
            s, fmt::format("Unexpected failure while waiting on {}", procname));
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    } while (sw.elapsed().wall_seconds() < kProcessWaitTimeoutSeconds);
    if (s.IsTimedOut()) {
      return KillAndWait(SIGKILL);
    }
  }
  return Status::OK();
}

Status Subprocess::GetExitStatus(int* exitStatus, string* infoStr) const {
  if (state_ != kExited) {
    const string errStr = "Sub-process termination hasn't yet been detected";
    LOG(DFATAL) << errStr;
    return Status::IllegalState(errStr);
  }
  string info;
  int status;
  if (WIFEXITED(waitStatus_)) {
    status = WEXITSTATUS(waitStatus_);
    if (status == 0) {
      info = fmt::format("{}: process successfully exited", program_);
    } else {
      info = fmt::format(
          "{}: process exited with non-zero status {}", program_, status);
    }
  } else if (WIFSIGNALED(waitStatus_)) {
    // Using signal number as exit status.
    status = WTERMSIG(waitStatus_);
    info = fmt::format("{}: process exited on signal {}", program_, status);
#if defined(WCOREDUMP)
    if (WCOREDUMP(waitStatus_)) {
      info += " (core dumped)";
    }
#endif
  } else {
    status = -1;
    info = fmt::format(
        "{}: process reported unexpected wait status {}",
        program_,
        waitStatus_);
    LOG(DFATAL) << info;
  }
  if (exitStatus) {
    *exitStatus = status;
  }
  if (infoStr) {
    *infoStr = info;
  }
  return Status::OK();
}

Status Subprocess::Call(const string& argStr) {
  vector<string> argv = Split(argStr, " ");
  return Call(argv, "", nullptr, nullptr);
}

Status Subprocess::Call(
    const vector<string>& argv,
    const string& stdinIn,
    string* stdoutOut,
    string* stderrOut) {
  Subprocess p(argv);

  if (stdoutOut) {
    p.shareParentStdout(false);
  }
  if (stderrOut) {
    p.shareParentStderr(false);
  }
  RETURN_NOT_OK_PREPEND(p.Start(), "Unable to fork " + argv[0]);

  if (!stdinIn.empty()) {
    ssize_t written;
    RETRY_ON_EINTR(
        written, write(p.to_child_stdin_fd(), stdinIn.data(), stdinIn.size()));
    if (written < stdinIn.size()) {
      return Status::IOError(
          "Unable to write to child process stdin",
          errnoToString(errno),
          errno);
    }
  }

  int err;
  RETRY_ON_EINTR(err, close(p.releaseChildStdinFd()));
  if (PREDICT_FALSE(err != 0)) {
    return Status::IOError(
        "Unable to close child process stdin", errnoToString(errno), errno);
  }

  vector<int> fds;
  if (stdoutOut) {
    fds.push_back(p.from_child_stdout_fd());
  }
  if (stderrOut) {
    fds.push_back(p.from_child_stderr_fd());
  }
  vector<string> outv;
  RETURN_NOT_OK(readFdsFully(argv[0], fds, &outv));

  // Given that readFdsFully captures the strings in the order in which we
  // had installed 'fds' above, it can be assured that we can receive
  // as many strings as there were 'fds' in the vector and in that order.
  CHECK_EQ(outv.size(), fds.size());
  if (stdoutOut) {
    *stdoutOut = std::move(outv.front());
  }
  if (stderrOut) {
    *stderrOut = std::move(outv.back());
  }

  RETURN_NOT_OK_PREPEND(p.Wait(), "Unable to wait() for " + argv[0]);
  int exitStatus;
  string exitInfoStr;
  RETURN_NOT_OK(p.GetExitStatus(&exitStatus, &exitInfoStr));
  if (exitStatus != 0) {
    return Status::RuntimeError(exitInfoStr);
  }
  return Status::OK();
}

pid_t Subprocess::pid() const {
  CHECK_EQ(state_, kRunning);
  return childPid_;
}

Status Subprocess::DoWait(int* waitStatus, WaitMode mode) {
  if (state_ == kExited) {
    if (waitStatus) {
      *waitStatus = waitStatus_;
    }
    return Status::OK();
  }
  if (state_ != kRunning) {
    const string errStr = fmt::format("{}: illegal sub-process state", state_);
    LOG(DFATAL) << errStr;
    return Status::IllegalState(errStr);
  }

  const int options = (mode == kNonBlocking) ? WNOHANG : 0;
  int status;
  int rc;
  RETRY_ON_EINTR(rc, waitpid(childPid_, &status, options));
  if (rc == -1) {
    return Status::RuntimeError(
        "Unable to wait on child", errnoToString(errno), errno);
  }
  if (mode == kNonBlocking && rc == 0) {
    return Status::TimedOut("");
  }
  CHECK_EQ(rc, childPid_);
  CHECK(WIFEXITED(status) || WIFSIGNALED(status));

  childPid_ = -1;
  waitStatus_ = status;
  state_ = kExited;
  if (waitStatus) {
    *waitStatus = status;
  }
  return Status::OK();
}

void Subprocess::setEnvVars(map<string, string> env) {
  CHECK_EQ(state_, kNotStarted);
  env_ = std::move(env);
}

void Subprocess::setCurrentDir(string cwd) {
  CHECK_EQ(state_, kNotStarted);
  cwd_ = std::move(cwd);
}

void Subprocess::setFdShared(int stdfd, bool share) {
  CHECK_EQ(state_, kNotStarted);
  fdState_[stdfd] = share ? kShared : kPiped;
}

void Subprocess::disableStderr() {
  CHECK_EQ(state_, kNotStarted);
  fdState_[STDERR_FILENO] = kDisabled;
}

void Subprocess::disableStdout() {
  CHECK_EQ(state_, kNotStarted);
  fdState_[STDOUT_FILENO] = kDisabled;
}

int Subprocess::checkAndOffer(int stdfd) const {
  CHECK_EQ(state_, kRunning);
  CHECK_EQ(fdState_[stdfd], kPiped);
  return childFds_[stdfd];
}

int Subprocess::releaseChildFd(int stdfd) {
  CHECK_EQ(state_, kRunning);
  CHECK_GE(childFds_[stdfd], 0);
  CHECK_EQ(fdState_[stdfd], kPiped);
  int ret = childFds_[stdfd];
  childFds_[stdfd] = -1;
  return ret;
}

} // namespace kudu
