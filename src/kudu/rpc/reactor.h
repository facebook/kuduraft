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
#ifndef KUDU_RPC_REACTOR_H
#define KUDU_RPC_REACTOR_H

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include <boost/function.hpp> // IWYU pragma: keep
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <ev++.h>

#include "kudu/gutil/macros.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {

class Sockaddr;
class Socket;

namespace rpc {

typedef std::list<std::shared_ptr<Connection>> ConnListT;

class DumpRunningRpcsRequestPB;
class DumpRunningRpcsResponsePB;
class OutboundCall;
class Reactor;
class ReactorThread;
enum class CredentialsPolicy;

// Simple metrics information from within a reactor.
// TODO(todd): switch these over to use util/metrics.h style metrics.
struct ReactorMetrics {
  // Number of client RPC connections currently connected.
  int32_t numClientConnections;
  // Number of server RPC connections currently connected.
  int32_t numServerConnections;

  // Total number of client RPC connections opened during Reactor's lifetime.
  uint64_t totalClientConnections;
  // Total number of server RPC connections opened during Reactor's lifetime.
  uint64_t totalServerConnections;

  // Total number of client normal TLS RPC connections opened during Reactor's
  // lifetime.
  uint64_t totalClientNormalTlsConnections;
  // Total number of server normal TLS RPC connections opened during Reactor's
  // lifetime.
  uint64_t totalServerNormalTlsConnections;
};

// A task which can be enqueued to run on the reactor thread.
class ReactorTask : public boost::intrusive::list_base_hook<> {
 public:
  ReactorTask();

  // Run the task. 'reactor' is guaranteed to be the current thread.
  virtual void run(ReactorThread* reactor) = 0;

  // Abort the task, in the case that the reactor shut down before the
  // task could be processed. This may or may not run on the reactor thread
  // itself.
  //
  // The Reactor guarantees that the Reactor lock is free when this
  // method is called.
  virtual void abort(const Status& abortStatus) {}

  virtual ~ReactorTask();

 private:
  DISALLOW_COPY_AND_ASSIGN(ReactorTask);
};

// A ReactorTask that is scheduled to run at some point in the future.
//
// Semantically it works like RunFunctionTask with a few key differences:
// 1. The user function is called during abort. Put another way, the
//    user function is _always_ invoked, even during reactor shutdown.
// 2. To differentiate between abort and non-abort, the user function
//    receives a Status as its first argument.
class DelayedTask : public ReactorTask {
 public:
  DelayedTask(boost::function<void(const Status&)> func, MonoDelta when);

  // Schedules the task for running later but doesn't actually run it yet.
  void run(ReactorThread* thread) override;

  // Behaves like ReactorTask::abort.
  void abort(const Status& abortStatus) override;

 private:
  // libev callback for when the registered timer fires.
  void timerHandler(ev::timer& watcher, int revents);

  // User function to invoke when timer fires or when task is aborted.
  const boost::function<void(const Status&)> func_;

  // Delay to apply to this task.
  const MonoDelta when_;

  // Link back to registering reactor thread.
  ReactorThread* thread_;

  // libev timer. Set when run() is invoked.
  ev::timer timer_;
};

// A ReactorThread is a libev event handler thread which manages I/O
// on a list of sockets.
//
// All methods in this class are _only_ called from the reactor thread itself
// except where otherwise specified. New methods should
// DCHECK(isCurrentThread()) to ensure this.
class ReactorThread {
 public:
  friend class Connection;

  // Client-side connection map. Multiple connections could be open to a remote
  // server if multiple credential policies are used for individual RPCs.
  typedef std::unordered_multimap<
      ConnectionId,
      std::shared_ptr<Connection>,
      ConnectionIdHash,
      ConnectionIdEqual>
      ConnMultimapT;

  ReactorThread(Reactor* reactor, const MessengerBuilder& bld);

  // This may be called from another thread.
  Status init();

  // Add any connections on this reactor thread into the given status dump.
  Status dumpRunningRpcs(
      const DumpRunningRpcsRequestPB& req,
      DumpRunningRpcsResponsePB* resp);

  void incrementNormalTlsConnections(bool isServer);

  // Shuts down a reactor thread, optionally waiting for it to exit.
  // Reactor::shutdown() must have been called already.
  //
  // If mode == SYNC, may not be called from the reactor thread itself.
  void shutdown(Messenger::ShutdownMode mode);

  // This method is thread-safe.
  void wakeThread();

  // libev callback for handling async notifications in our epoll thread.
  void asyncHandler(ev::async& watcher, int revents);

  // libev callback for handling timer events in our epoll thread.
  void timerHandler(ev::timer& watcher, int revents);

  // Register an epoll timer watcher with our event loop.
  // Does not set a timeout or start it.
  void registerTimeout(ev::timer* watcher);

  // This may be called from another thread.
  const std::string& name() const;

  MonoTime curTime() const;

  // This may be called from another thread.
  Reactor* reactor();

  // Return true if this reactor thread is the thread currently
  // running. Should be used in DCHECK assertions.
  bool isCurrentThread() const;

  // Begin the process of connection negotiation.
  // Must be called from the reactor thread.
  Status startConnectionNegotiation(const std::shared_ptr<Connection>& conn);

  // Transition back from negotiating to processing requests.
  // Must be called from the reactor thread.
  void completeConnectionNegotiation(
      const std::shared_ptr<Connection>& conn,
      const Status& status,
      std::unique_ptr<ErrorStatusPB> rpcError);

  // Collect metrics.
  // Must be called from the reactor thread.
  Status getMetrics(ReactorMetrics* metrics);

 private:
  friend class AssignOutboundCallTask;
  friend class CancellationTask;
  friend class ResetConnectionsTask;
  friend class RegisterConnectionTask;
  friend class DelayedTask;

  // Run the main event loop of the reactor.
  void runThread();

  // When libev has noticed that it needs to wake up an application watcher,
  // it calls this callback. The callback simply calls back into libev's
  // ev_invoke_pending() to trigger all the watcher callbacks, but
  // wraps it with latency measurements.
  static void invokePendingCb(struct ev_loop* loop);

  // Similarly, libev calls these functions before/after invoking epoll_wait().
  // We use these to measure the amount of time spent waiting.
  //
  // NOTE: 'noexcept' is required to avoid compilation errors due to libev's
  // use of the same exception specification.
  static void aboutToPollCb(struct ev_loop* loop) noexcept;
  static void pollCompleteCb(struct ev_loop* loop) noexcept;

  // Find a connection to the given remote and returns it in 'conn'.
  // Returns true if a connection is found. Returns false otherwise.
  bool findConnection(
      const ConnectionId& connId,
      CredentialsPolicy credPolicy,
      std::shared_ptr<Connection>* conn);

  // Find or create a new connection to the given remote.
  // If such a connection already exists, returns that, otherwise creates a new
  // one. May return a bad Status if the connect() call fails. The resulting
  // connection object is managed internally by the reactor thread.
  Status findOrStartConnection(
      const ConnectionId& connId,
      CredentialsPolicy credPolicy,
      std::shared_ptr<Connection>* conn,
      std::shared_ptr<MetricEntity> metricEntity);

  // Shut down the given connection, removing it from the connection tracking
  // structures of this reactor.
  //
  // The connection is not explicitly deleted -- shared_ptr reference counting
  // may hold on to the object after this, but callers should assume that it
  // _may_ be deleted by this call.
  void destroyConnection(
      Connection* conn,
      const Status& connStatus,
      std::unique_ptr<ErrorStatusPB> rpcError = {});

  // Scan any open connections for idle ones that have been idle longer than
  // connectionKeepaliveTime_. If connectionKeepaliveTime_ < 0, the scan
  // is skipped.
  void scanIdleConnections();

  // Create a new client socket (non-blocking, NODELAY)
  static Status createClientSocket(Socket* sock);

  // Initiate a new connection on the given socket.
  static Status startConnect(Socket* sock, const Sockaddr& remote);

  // Assign a new outbound call to the appropriate connection object.
  // If this fails, the call is marked failed and completed.
  void assignOutboundCall(std::shared_ptr<OutboundCall> call);

  // Cancel the outbound call. May update corresponding connection
  // object to remove call from the CallAwaitingResponse object.
  // Also mark the call as slated for cancellation so the callback
  // may be invoked early if the RPC hasn't yet been sent or if it's
  // waiting for a response from the remote.
  void cancelOutboundCall(const std::shared_ptr<OutboundCall>& call);

  // Register a new connection.
  void registerConnection(std::shared_ptr<Connection> conn);

  // Manually destroy all connections so they can be recreated.
  void resetAllConnections();

  // Actually perform shutdown of the thread, tearing down any connections,
  // etc. This is called from within the thread.
  void shutdownInternal();

  std::shared_ptr<kudu::Thread> thread_;

  // our epoll object (or kqueue, etc).
  ev::dynamic_loop loop_;

  // Used by other threads to notify the reactor thread
  ev::async async_;

  // Handles the periodic timer.
  ev::timer timer_;

  // Scheduled (but not yet run) delayed tasks.
  //
  // Each task owns its own memory and must be freed by its TaskRun and
  // Abort members, provided it was allocated on the heap.
  boost::intrusive::list<DelayedTask> scheduledTasks_;

  // The current monotonic time.  Updated every coarseTimerGranularity_.
  MonoTime curTime_;

  // last time we did TCP timeouts.
  MonoTime lastUnusedTcpScan_;

  // Map of sockaddrs to Connection objects for outbound (client) connections.
  ConnMultimapT clientConns_;

  // List of current connections coming into the server.
  ConnListT serverConns_;

  Reactor* reactor_;

  // If a connection has been idle for this much time, it is torn down.
  const MonoDelta connectionKeepaliveTime_;

  // Scan for idle connections on this granularity.
  const MonoDelta coarseTimerGranularity_;

  // Metrics.
  std::shared_ptr<Histogram> invokeUsHistogram_;
  std::shared_ptr<Histogram> loadPercentHistogram_;

  // Total number of client connections opened during Reactor's lifetime.
  uint64_t totalClientConnsCnt_;

  // Total number of server connections opened during Reactor's lifetime.
  uint64_t totalServerConnsCnt_;

  // Total number of client normal TLS connections opened during Reactor's
  // lifetime. Atomic because it's incremented from negotiation threads.
  std::atomic<uint64_t> totalClientNormalTlsConnsCnt_;

  // Total number of server normal TLS connections opened during Reactor's
  // lifetime. Atomic because it's incremented from negotiation threads.
  std::atomic<uint64_t> totalServerNormalTlsConnsCnt_;

  // Set prior to calling epoll and then reset back to -1 after each invocation
  // completes. Used for accounting totalPollCycles_.
  int64_t cycleClockBeforePoll_ = -1;

  // The total number of cycles spent in epoll_wait() since this thread
  // started.
  int64_t totalPollCycles_ = 0;

  // Accounting for determining load average in each cycle of TimerHandler.
  struct {
    // The cycle-time at which the load average was last calculated.
    int64_t timeCycles = -1;
    // The value of totalPollCycles_ at the last-recorded time.
    int64_t pollCycles = -1;
  } lastLoadMeasurement_;

  std::shared_ptr<MetricEntity> metricEntity_;
};

// A Reactor manages a ReactorThread
class Reactor {
 public:
  Reactor(
      std::shared_ptr<Messenger> messenger,
      int index,
      const MessengerBuilder& bld);
  Status init();

  // Shuts down the reactor and its corresponding thread, optionally waiting
  // until the thread has exited.
  void shutdown(Messenger::ShutdownMode mode);

  ~Reactor();

  const std::string& name() const;

  // Collect metrics about the reactor.
  Status getMetrics(ReactorMetrics* metrics);

  // Add any connections on this reactor thread into the given status dump.
  Status dumpRunningRpcs(
      const DumpRunningRpcsRequestPB& req,
      DumpRunningRpcsResponsePB* resp);

  // Queue a new incoming connection. Takes ownership of the underlying fd from
  // 'socket', but not the Socket object itself.
  // If the reactor is already shut down, takes care of closing the socket.
  void registerInboundSocket(Socket* socket, const Sockaddr& remote);

  // Queue a new call to be sent. If the reactor is already shut down, marks
  // the call as failed.
  void queueOutboundCall(const std::shared_ptr<OutboundCall>& call);

  // Queue a new reactor task to cancel an outbound call.
  void queueCancellation(const std::shared_ptr<OutboundCall>& call);

  // Queues a task to reset this reactor's connections
  void queueResetConnections();

  // Schedule the given task's run() method to be called on the
  // reactor thread.
  // If the reactor shuts down before it is run, the abort method will be
  // called.
  // Does _not_ take ownership of 'task' -- the task should take care of
  // deleting itself after running if it is allocated on the heap.
  void scheduleReactorTask(ReactorTask* task);

  Status runOnReactorThread(const boost::function<Status()>& f);

  // If the Reactor is closing, returns false.
  // Otherwise, drains the pendingTasks_ queue into the provided list.
  bool drainTaskQueue(boost::intrusive::list<ReactorTask>* tasks);

  Messenger* messenger() const {
    return messenger_.get();
  }

  // Indicates whether the reactor is shutting down.
  //
  // This method is thread-safe.
  bool closing() const;

  // Is this reactor's thread the current thread?
  bool isCurrentThread() const {
    return thread_.isCurrentThread();
  }

 private:
  friend class ReactorThread;
  typedef SimpleSpinlock LockType;
  mutable LockType lock_;

  // parent messenger
  std::shared_ptr<Messenger> messenger_;

  const std::string name_;

  // Whether the reactor is shutting down.
  // Guarded by lock_.
  bool closing_;

  // Tasks to be run within the reactor thread.
  // Guarded by lock_.
  boost::intrusive::list<ReactorTask>
      pendingTasks_; // NOLINT(build/include_what_you_use)

  ReactorThread thread_;

  DISALLOW_COPY_AND_ASSIGN(Reactor);
};

} // namespace rpc
} // namespace kudu

#endif
