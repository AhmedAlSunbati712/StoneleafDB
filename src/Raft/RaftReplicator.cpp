#include <Raft/RaftReplicator.h>

#include <Raft/RaftCommitIndex.h>
#include <Raft/RaftPeerClients.h>
#include <Raft/RaftProtoCodec.h>

#include <grpcpp/client_context.h>

#include <algorithm>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

RaftReplicator::RaftReplicator(RaftState& state,
                               RaftLog& raft_log,
                               RaftPeerClients& peers,
                               NodeAddress peer,
                               std::size_t max_entries_per_batch)
    : state_(state),
      raft_log_(raft_log),
      peers_(peers),
      peer_(std::move(peer)),
      max_entries_per_batch_(std::max<std::size_t>(max_entries_per_batch, 1)) {}

void RaftReplicator::run() {
    while (true) {
        {
            std::unique_lock lock(state_.state_mutex);

            // Parked while not leading. Created once at startup and never
            // spawned per leadership: joining a thread in become_follower()
            // would block a step-down on an in-flight RPC to an unreachable
            // peer, which is the path that most needs to be fast.
            state_.replication_cv.wait(lock, [this] {
                return state_.shutting_down || state_.state() == State::Leader;
            });
            if (state_.shutting_down) return;

            // Leading. Wait for something to send, but wake for the heartbeat
            // regardless: silence is what makes a follower campaign.
            if (state_.send_next(peer_) > raft_log_.last_index()) {
                state_.replication_cv.wait_for(lock, RaftState::HEARTBEAT_INTERVAL);
                if (state_.shutting_down) return;
                if (state_.state() != State::Leader) continue;
            }
        }

        replicate_once();
    }
}

bool RaftReplicator::replicate_once() {
    std::uint64_t sent_term = 0;
    std::uint64_t prev_index = 0;
    std::uint64_t prev_term = 0;
    std::uint64_t leader_commit = 0;
    NodeAddress self;
    std::vector<RaftMutationEntry> entries;

    {
        std::lock_guard lock(state_.state_mutex);
        if (state_.shutting_down || state_.state() != State::Leader) return false;

        sent_term = state_.current_term();
        self = state_.self_raft_address();
        leader_commit = state_.commit_index();

        // send_next and replicated_index are only populated for the current
        // leadership, which the state check above guarantees we hold.
        const std::uint64_t next = state_.send_next(peer_);

        // prev_index 0 is a vacuous match: nothing precedes the first entry.
        prev_index = next > 1 ? next - 1 : 0;
        prev_term = prev_index > 0 ? raft_log_.term_at(prev_index) : 0;

        if (next <= raft_log_.last_index()) {
            entries = raft_log_.scan_from(next);
            if (entries.size() > max_entries_per_batch_) {
                entries.resize(max_entries_per_batch_);
            }
        }
    }

    stoneleaf::raft::AppendEntriesRequest request;
    request.set_term(sent_term);
    request.set_leader(self.to_string());
    request.set_prev_log_idx(prev_index);
    request.set_prev_log_term(prev_term);
    request.set_leader_commit_idx(leader_commit);
    for (const RaftMutationEntry& entry : entries) {
        RaftProtoCodec::to_proto(entry, request.add_entries());
    }

    // Sent with the lock released: no RPC is ever issued under state_mutex.
    grpc::ClientContext context;
    RaftRpc::apply_deadline(context);
    stoneleaf::raft::AppendEntriesResponse response;
    const grpc::Status status =
        peers_.stub(peer_).AppendEntries(&context, request, &response);

    // An unreachable peer is normal operation, not an error. send_next is left
    // untouched so the next heartbeat retries exactly the same batch.
    if (!status.ok()) return false;

    std::lock_guard lock(state_.state_mutex);

    // A higher term anywhere means we are no longer leader, whatever we think.
    if (response.term() > state_.current_term()) {
        state_.become_follower(response.term(), std::nullopt);
        return false;
    }

    // Stale reply: we may have changed term or lost leadership while this RPC
    // was in flight, in which case it says nothing about the current term's
    // replication progress and must not touch the progress maps.
    if (sent_term != state_.current_term() || state_.state() != State::Leader) {
        return false;
    }

    if (!response.success()) {
        // The follower's log diverges at prev_index. Back up one and retry;
        // eventually send_next reaches a point where the logs match. The paper's
        // conflicting-term hint would make this O(terms) instead of O(entries).
        const std::uint64_t next = state_.send_next(peer_);
        if (next > 1) state_.set_send_next(peer_, next - 1);
        return false;
    }

    // Computed from what we sent, not from the follower's reply: the follower
    // reports success, not a position, and a reordered duplicate must not move
    // progress backwards.
    const std::uint64_t matched = prev_index + entries.size();
    if (matched > state_.replicated_index(peer_)) {
        state_.set_replicated_index(peer_, matched);
        state_.set_send_next(peer_, matched + 1);
    }

    advance_commit_index(state_, raft_log_);
    return true;
}
