#include <Raft/RaftReadIndex.h>

#include <mutex>

RaftReadIndex::RaftReadIndex(RaftState& state) : state_(state) {}

ReadIndexStatus RaftReadIndex::wait_until_readable() {
    const auto deadline = std::chrono::steady_clock::now() + READ_TIMEOUT;

    std::unique_lock lock(state_.state_mutex);
    if (state_.state() != State::Leader) return ReadIndexStatus::NotLeader;
    const std::uint64_t term = state_.current_term();

    // Leadership is re-checked after every wait: losing it invalidates all
    // three steps, and the answer the client needs then is "ask someone else",
    // not a stale value.
    const auto leading = [this, term] {
        return state_.state() == State::Leader && state_.current_term() == term;
    };

    // 1. Our own term's no-op must be committed before commit_index can be
    //    read as the committed prefix.
    while (state_.leader_term_first_index() == 0 ||
           state_.commit_index() < state_.leader_term_first_index()) {
        if (!leading()) return ReadIndexStatus::NotLeader;
        if (state_.shutting_down) return ReadIndexStatus::Timeout;
        if (state_.read_cv.wait_until(lock, deadline) == std::cv_status::timeout &&
            std::chrono::steady_clock::now() >= deadline) {
            return leading() ? ReadIndexStatus::Timeout : ReadIndexStatus::NotLeader;
        }
    }

    // The point this read must see. Taken before the confirmation round, so
    // anything committed before the read arrived is at or below it.
    const std::uint64_t read_index = state_.commit_index();

    // 2. A round that opens now can only be confirmed by replies that arrive
    //    after it, which is what makes the confirmation mean "still leader".
    const std::uint64_t round = state_.start_read_round();
    state_.replication_cv.notify_all();   // heartbeat now, not at the next interval
    while (state_.confirmed_read_round() < round) {
        if (!leading()) return ReadIndexStatus::NotLeader;
        if (state_.shutting_down) return ReadIndexStatus::Timeout;
        if (state_.read_cv.wait_until(lock, deadline) == std::cv_status::timeout &&
            std::chrono::steady_clock::now() >= deadline) {
            return leading() ? ReadIndexStatus::Timeout : ReadIndexStatus::NotLeader;
        }
    }

    // 3. Committed is not applied: without this wait the read could miss a
    //    write that was already committed when it arrived.
    while (state_.last_applied() < read_index) {
        if (state_.shutting_down) return ReadIndexStatus::Timeout;
        // Deliberately NOT gated on still leading: an entry that has committed
        // will be applied by every node, and this read is entitled to it.
        if (state_.applied_cv.wait_until(lock, deadline) == std::cv_status::timeout &&
            std::chrono::steady_clock::now() >= deadline) {
            return ReadIndexStatus::Timeout;
        }
    }

    return ReadIndexStatus::Ready;
}
