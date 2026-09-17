#include <Raft/RaftCommitIndex.h>

#include <cstddef>

void advance_commit_index(RaftState& state, RaftLog& raft_log) {
    // Only a leader commits by counting. A follower's commit_index arrives in
    // leader_commit on the AppendEntries it receives.
    if (state.state() != State::Leader) return;

    const std::uint64_t first_uncommitted = state.commit_index() + 1;
    const std::size_t majority = state.cluster_size() / 2 + 1;

    for (std::uint64_t n = raft_log.last_index(); n >= first_uncommitted && n > 0; --n) {
        // Raft 5.4.2 / Figure 8: a leader may only commit by counting replicas
        // for an entry from its OWN term. Committing an earlier term's entry
        // this way can lose acknowledged data. Entries below n commit
        // transitively once n does.
        if (raft_log.term_at(n) != state.current_term()) continue;

        // The leader counts itself only once its own copy is durable. Testing
        // durable_index rather than calling sync_through() here keeps the fsync
        // off state_mutex - the propose path syncs after appending, with the
        // lock released, and that is what moves this forward.
        std::size_t replicas = raft_log.durable_index() >= n ? 1 : 0;
        for (const auto& [address, replicated] : state.replicated_indexes()) {
            if (replicated >= n) ++replicas;
        }

        if (replicas >= majority) {
            state.set_commit_index(n);
            state.apply_cv.notify_one();   // single waiter: the apply loop
            state.read_cv.notify_all();    // readers waiting for the leadership no-op
            break;
        }
    }
}
