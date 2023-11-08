// Copyright (c) Meta Platforms, Inc. and affiliates.

#pragma once

#include <folly/Synchronized.h>
#include <gflags/gflags.h>
#include <future>
#include <optional>
#include <vector>

#include "kudu/consensus/log.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/util/debouncer.h"
#include "kudu/util/status.h"

DECLARE_int32(consensus_max_batch_size_bytes);

namespace kudu {
namespace consensus {

/**
 * A basic data structure to hold pointers to the buffered replicates, as well
 * as some markers on where the buffer starts.
 *
 * Note that this class is not thread safe and needs to be synchronized. See
 * SynchronizedBufferData.
 */
class BufferData {
 public:
  /**
   * Clears the buffer and resets its metadata.
   *
   * @param last_index The index to reset the buffer to
   */
  void resetBuffer(bool for_proxy = false, int64_t last_index = -1);

  /**
   * Appends a single new message to the buffer if it matches the tail of the
   * buffer.
   *
   * @param new_message The op to append
   * @return OK if message is appended
   */
  Status appendMessage(ReplicateRefPtr new_message);

  /**
   * Reads ops from the LogCache into this buffer.
   *
   * This method will continue reading from last_index() onwards unless the
   * buffer is not initialized (last_index() == -1)
   *
   * @param read_context Context on where to start reading and if we need
   *                     proxying
   * @param log_cache A reference to the log cache
   * @return OK if we read everything, Incomplete if the first index requested
   *         is not in the cache yet, Continue if we hit the size limit before
   *         reading all ops
   */
  Status readFromCache(const ReadContext& read_context, LogCache& log_cache);

  /**
   * First index in the buffer.
   *
   * @return the first index or -1 if buffer is empty
   */
  int64_t first_index() const {
    return msg_buffer_refs.empty()
        ? -1
        : msg_buffer_refs.front()->get()->id().index();
  }

  /**
   * The last index we've bufferred until. This index may not be in the message
   * buffer itself, but it's where we'll pick up bufferring from.
   *
   * @return The last bufferred index or -1 if nothing has been bufferred yet
   */
  int64_t last_index() const {
    return last_buffered;
  }

  /**
   * If the buffer is empty.
   *
   * @return true if buffer is empty
   */
  bool empty() const {
    return last_buffered == -1 || msg_buffer_refs.empty();
  }

  /**
   * If we bufferred ops that are meant for proxying.
   *
   * Proxyed ops may not be compressed and may not have checksums.
   *
   * @return true if the bufferred data was meant for proxying
   */
  bool for_proxying() const {
    return buffered_for_proxying;
  }

  /**
   * If the buffer is full and should not be filled further.
   *
   * @return true if buffer is full
   */
  bool buffer_full() const {
    return bytes_buffered >= FLAGS_consensus_max_batch_size_bytes;
  }

  /**
   * Moves the data out from this buffer into another BufferData.
   *
   * This buffer will be cleared of any data, but last_index() will be persisted
   * for future reads.
   *
   * return A new BufferData instance with the ops in the buffer
   */
  BufferData moveDataAndReset();

 protected:
  /**
   * The vector of bufferred replicates.
   */
  std::vector<ReplicateRefPtr> msg_buffer_refs;
  /**
   * The preceding_opid (contains both term and index) of the replicate before
   * the start of the buffer.
   */
  OpId preceding_opid;
  /**
   * The last index we buffered. We should pick up buffering from here. Will be
   * -1 at the start. Could refer to a previous buffer point if the buffer was
   * flushed out to a RPC.
   */
  int64_t last_buffered = -1;
  /**
   * If we buffered data that was meant for proxying.
   */
  bool buffered_for_proxying = false;
  /**
   * The number of bytes we buffered into the buffer. Note that we don't
   * actually store bytes, but pointers, so this represents the size of the data
   * pointed to.
   */
  int64_t bytes_buffered = 0;
};

/**
 * BufferData synchronized on a debouncer.
 */
using SynchronizedBufferData = folly::Synchronized<BufferData, MutexDebouncer>;

/**
 * The struct we use to handoff the buffer to a RPC that's being prepared.
 */
struct HandedOffBufferData : public BufferData {
  /**
   * Takes in a BufferData object and converts to HandedOffBufferData by
   * tagging a status.
   */
  HandedOffBufferData(Status s, BufferData&& buffer_data)
      : BufferData(std::move(buffer_data)), status(std::move(s)) {}
  /**
   * Status of the prepared buffer.
   *
   * This will be OK if we grabbed the prepared buffer, or the status of
   * LogCache::ReadOps if not.
   */
  Status status;

  /**
   * Extracts the data from this handoff buffer into the provided containers.
   *
   * This leaves the handoff buffer in an inconsistent state and is a move
   * function.
   *
   * @param msg The container for the bufferred ops
   * @param preceding_id The container for populating the preceding OpId for the
   *                     bufferred data
   */
  void getData(std::vector<ReplicateRefPtr>* msg, OpId* preceding_id) &&;
};

/**
 * The complete PeerMessageBuffer struct, containing a data section and a
 * handoff section.
 *
 * The data section is synchronized with a mutex debouncer and contains the
 * bufferred data and metadata.
 *
 * The handoff section comprises of a number of thread safe objects that
 * orchestrates handing off the buffer. handoff_initial_index_ is the sync point
 * signaling a need to handoff, and the data is handed off to handoff_promise_.
 */
struct PeerMessageBuffer {
 public:
  /**
   * A RAII lock handle for the data section. When this struct is alive, the
   * debouncer on the data section will be held.
   *
   * The struct also contains a reference back to PeerMessageBuffer and as such
   * provides helpers for handoffs by accessing the data under lock.
   */
  struct LockedBufferHandle : public SynchronizedBufferData::TryLockedPtr {
    /**
     * Inits a handle with a reference to PeerMessageBuffer and a lock pointer
     * to the data.
     *
     * Called by PeerMessageBuffer::tryLock.
     *
     * @param message_buffer Reference to PeerMessageBuffer
     * @param locked_ptr folly:Synchronized lock for PeerMessageBuffer::data_
     */
    LockedBufferHandle(
        PeerMessageBuffer& message_buffer,
        SynchronizedBufferData::TryLockedPtr&& locked_ptr);

    /**
     * Swaps out PeerMessageBuffer::handoff_initial_index_ and returns it if
     * handoff is needed.
     *
     * @return The index for the initial index needed, or std::nullopt if no
     * handoff is needed.
     */
    std::optional<int64_t> getIndexForHandoff();

    /**
     * Returns true of the buffer data satisfies the proxy requirements from the
     * requested handoff.
     *
     * Return value behaviour is undefined if no handoff is requested.
     *
     * @return true if the buffer satisfies handoff requirements
     */
    bool proxyRequirementSatisfied() const;

    /**
     * Extracts the buffer from the locked data section and feeds it to
     * PeerMessageBuffer:handoff_promise_. The promise should be fulfilled after
     * calling this method.
     *
     * Behaviour is undefined if no handoff has been requested.
     *
     * @param s status of the handoff, to be wrapped into HandedOffBufferData
     */
    void fulfillPromiseWithBuffer(Status s);

   private:
    /**
     * A reference to PeerMessageBuffer to orchestrate handoffs.
     */
    PeerMessageBuffer& message_buffer_;
  };

  /**
   * Attempts to locks data section via a debouncer.
   *
   * Returned object needs to be checked via the boolean operator or isNull() to
   * ascertain if lock has been acquired.
   *
   * Note that the data section is locked with a debouncer, so it may reject
   * duplicate lockers. See kudu/util/Debouncer.h
   *
   * @return A RAII handle for the locked data section is locked, a null handle
   * otherwise
   */
  LockedBufferHandle tryLock();

  /**
   * Swaps out handoff_initial_index_ and returns it if handoff is needed.
   *
   * @return The index for the initial index needed, or std::nullopt if no
   * handoff is needed.
   */
  std::optional<int64_t> getIndexForHandoff();

  /**
   * Returns if the handoff requested for ops to be proxied. Proxied ops have a
   * different type as compared to non-proxied ops.
   *
   * Return value is undefined if no handoff has been requested.
   *
   * @return true if handoff requested proxy ops.
   */
  bool getProxyOpsNeeded() const;

  /**
   * Requests a handoff. Data will flow into the returned future.
   *
   * Note that this method is not internally mutexed. Caller should ensure
   * that it is the exclusive party requesting the handoff till it gets a result
   * from the returned future.
   *
   * @param index The initial index required
   * @param proxy_ops_needed If we need ops for proxying
   * @return A future that will be populated with the requested data, either
   * from the buffer or from reading from the source (LogCache).
   */
  std::future<HandedOffBufferData> requestHandoff(
      int64_t index,
      bool proxy_ops_needed);

 private:
  /**
   * The synchronized data section.
   */
  SynchronizedBufferData data_;

  // Handoff section
  /**
   * The sync point for requesting handoffs.
   *
   * The initial index populated here denotes the initial index the RPC
   * requesting handoff requires.
   *
   * The rest of the handoff section should be prepared before this is set.
   */
  std::atomic_int64_t handoff_initial_index_ = -1;
  /**
   * If the handoff requested ops for proxying.
   */
  std::atomic_bool proxy_ops_needed_ = false;
  /**
   * The promise where we handoff the bufferred ops to. A RPC will be waiting on
   * a future linked to this promise.
   */
  std::promise<HandedOffBufferData> handoff_promise_;
};

} // namespace consensus
} // namespace kudu
