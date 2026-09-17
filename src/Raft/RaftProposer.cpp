#include <Raft/RaftProposer.h>

#include <Raft/RaftCommitIndex.h>

#include <chrono>
#include <utility>

RaftProposer::RaftProposer(RaftState& state, RaftLog& raft_log)
    : state_(state), raft_log_(raft_log) {}

ProposeStatus RaftProposer::propose(std::vector<MutationOp> operations) {
    std::uint64_t index = 0;
    std::uint64_t my_term = 0;

    {
        std::lock_guard append_lock(state_.append_mutex);
        {
            std::lock_guard lock(state_.state_mutex);

            // Leadership can change mid-session: a transaction may open while
            // we lead, buffer writes for seconds, and reach COMMIT after we
            // have been deposed. The fast path at accept time is not the
            // correctness-critical check - this one is.
            if (state_.state() != State::Leader) return ProposeStatus::NotLeader;
            my_term = state_.current_term();
        }

        // Appended under append_mutex only, so the write never holds
        // state_mutex. Losing leadership since the check above is safe; see
        // RaftState::append_mutex.
        index = raft_log_.append(my_term, std::move(operations));
    }
    state_.replication_cv.notify_all();

    // Durable before our copy can count toward a majority, and deliberately
    // outside state_mutex: this is an fsync. advance_commit_index() tests
    // durable_index() rather than syncing itself precisely so this can live
    // here instead of under the lock.
    raft_log_.sync_through(index);

    std::unique_lock lock(state_.state_mutex);

    // Our own copy only just became durable, which may itself be what completes
    // the majority - and in a single-node cluster there are no replication
    // threads to notice. Counting here is what makes both cases commit.
    advance_commit_index(state_, raft_log_);

    return await_commit(lock, index, my_term);
}

ProposeStatus RaftProposer::await_commit(std::unique_lock<std::mutex>& lock,
                                         std::uint64_t index,
                                         std::uint64_t my_term) {
    const auto deadline = std::chrono::steady_clock::now() + RaftState::COMMIT_TIMEOUT;

    while (true) {
        // Checked FIRST, and the order matters: once an entry has applied it has
        // applied, even if we were deposed a microsecond later.
        if (state_.last_applied() >= index) return ProposeStatus::Committed;

        // Upgrades an Unknown into a definite Failed, which is worth the extra
        // check: by Leader Completeness a committed entry is never overwritten,
        // so our term being gone from our index proves the entry never
        // committed. Failed is safe to retry; Unknown is not. term_at() returns
        // 0 past the end of the log, which covers a truncation that shortened it
        // below index.
        if (raft_log_.term_at(index) != my_term) return ProposeStatus::Failed;

        // Shutdown is genuinely unknown: the entry may well commit elsewhere,
        // and claiming otherwise would be a lie to the client.
        if (state_.shutting_down) return ProposeStatus::Unknown;

        if (std::chrono::steady_clock::now() >= deadline) return ProposeStatus::Unknown;

        // Losing leadership is deliberately NOT an exit condition. It says
        // nothing about the entry's fate - a new leader either already holds it,
        // in which case it commits and replicates back to us, or it does not, in
        // which case it is truncated and the check above catches that. Exiting
        // early would convert both definite outcomes into Unknown.
        state_.applied_cv.wait_until(lock, deadline);
    }
}
