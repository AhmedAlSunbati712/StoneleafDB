#pragma once

#include <KeyStore.h>
#include <Log/Log.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>
#include <TransactionManager/TransactionManager.h>

#include <cstddef>
#include <cstdint>

// The one thread that applies committed Raft entries to the local state
// machine. Leaders and followers run byte-identical code: it waits for
// commit_index to pass last_applied, applies entries in index order, and moves
// last_applied forward.
//
// It takes no logical key locks. On a leader the session that proposed the
// entry still holds them and is parked waiting on last_applied, so acquiring
// them here would deadlock against that session; on a follower there are no
// sessions to exclude. Write-write safety comes instead from this being the
// only writer to the B-tree.
class RaftApplier {
public:
    static constexpr std::size_t DEFAULT_MAX_APPLY_BATCH_SIZE = 64;

    RaftApplier(RaftState& state,
                RaftLog& raft_log,
                KeyStore& key_store,
                TransactionManager& transaction_manager,
                Log& wal,
                std::size_t max_apply_batch_size = DEFAULT_MAX_APPLY_BATCH_SIZE);

    RaftApplier(const RaftApplier&) = delete;
    RaftApplier& operator=(const RaftApplier&) = delete;

    // Thread body: park on apply_cv until there is committed work or the server
    // is shutting down, apply one batch, repeat. Returns only once
    // shutting_down is set, so the thread can be joined.
    //
    // Failures propagate out. A committed entry must be applied on every node,
    // so a node that cannot apply one must not continue as if it had; letting
    // the exception escape the thread halts the process, and restart replays
    // the entry from the Raft log.
    void run();

    // Applies at most max_apply_batch_size entries, returning how many were
    // applied and 0 when already caught up. Never waits. run() calls this;
    // tests drive it directly.
    std::size_t apply_pending_batch();

private:
    RaftState& state_;
    RaftLog& raft_log_;
    KeyStore& key_store_;
    TransactionManager& transaction_manager_;
    Log& wal_;
    std::size_t max_apply_batch_size_;
};
