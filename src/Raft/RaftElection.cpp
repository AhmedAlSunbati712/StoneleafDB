#include <Raft/RaftElection.h>

#include <Raft/RaftPeerClients.h>

#include <grpcpp/client_context.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

RaftElection::RaftElection(RaftState& state, RaftLog& raft_log, RaftPeerClients& peers)
    : state_(state), raft_log_(raft_log), peers_(peers) {}

void RaftElection::run() {
    while (true) {
        {
            std::unique_lock lock(state_.state_mutex);

            // A leader never campaigns. become_follower() notifies election_cv,
            // which is what restarts the timer on a step-down.
            state_.election_cv.wait(lock, [this] {
                return state_.shutting_down || state_.state() != State::Leader;
            });
            if (state_.shutting_down) return;

            // Sleep until the deadline, re-reading it on every wake. A valid
            // AppendEntries from the current leader, or granting a vote, pushes
            // it forward - and that is exactly how a healthy cluster keeps its
            // followers from campaigning.
            if (std::chrono::steady_clock::now() < state_.election_deadline()) {
                state_.election_cv.wait_until(lock, state_.election_deadline());
                if (state_.shutting_down) return;

                // Woken early, promoted, or the timer moved under us: go back to
                // the top rather than campaigning on a stale decision.
                if (state_.state() == State::Leader ||
                    std::chrono::steady_clock::now() < state_.election_deadline()) {
                    continue;
                }
            }
        }

        campaign();
    }
}

bool RaftElection::campaign() {
    std::uint64_t sent_term = 0;
    std::uint64_t last_log_index = 0;
    std::uint64_t last_log_term = 0;
    std::size_t majority = 0;
    NodeAddress self;
    std::vector<NodeAddress> peers;

    {
        std::lock_guard lock(state_.state_mutex);
        if (state_.shutting_down || state_.state() == State::Leader) return false;

        // Advances the term, votes for ourselves, and persists both BEFORE
        // returning. No RequestVote may carry a term that is not yet durable:
        // advertise a term that a crash forgets and this node can vote twice in
        // it, which costs Election Safety.
        state_.become_candidate();

        sent_term = state_.current_term();
        self = state_.self_raft_address();
        peers = state_.peers();
        majority = state_.cluster_size() / 2 + 1;

        // Lock order is state_mutex -> raft log mutex, so the log is read here
        // rather than before taking the state lock.
        last_log_index = raft_log_.last_index();
        last_log_term = raft_log_.last_term();

        // A single-node cluster is already a majority of one: our own vote wins
        // it, and there is no peer to ask.
        if (majority <= 1) {
            state_.become_leader(last_log_index);
            state_.replication_cv.notify_all();
            return true;
        }
    }

    std::atomic<bool> won{false};

    // In parallel, not in sequence. Serialized requests would cost up to
    // peers * RPC_DEADLINE, which exceeds the election timeout outright and
    // would make a campaign lose to its own successor.
    std::vector<std::thread> voters;
    voters.reserve(peers.size());
    for (const NodeAddress& peer : peers) {
        voters.emplace_back([&, peer] {
            stoneleaf::raft::RequestVoteRequest request;
            request.set_term(sent_term);
            request.set_candidate(self.to_string());
            request.set_last_log_idx(last_log_index);
            request.set_last_log_term(last_log_term);

            grpc::ClientContext context;
            RaftRpc::apply_deadline(context);
            stoneleaf::raft::RequestVoteResponse response;
            const grpc::Status status =
                peers_.stub(peer).RequestVote(&context, request, &response);

            // An unreachable peer is normal operation, not an error: it simply
            // does not vote, and the next timeout starts a fresh campaign.
            if (!status.ok()) return;

            std::lock_guard lock(state_.state_mutex);

            // A higher term anywhere means this campaign is already obsolete.
            // Strictly greater: advance_term() throws on a term that does not
            // move forward.
            if (response.term() > state_.current_term()) {
                state_.become_follower(response.term(), std::nullopt);
                return;
            }

            // Stale reply. We may have changed term or stopped being a candidate
            // while this RPC was in flight, in which case the reply says nothing
            // about the current campaign and must not be counted. This also
            // makes a late duplicate harmless once we have already won.
            if (sent_term != state_.current_term() ||
                state_.state() != State::Candidate) {
                return;
            }

            if (!response.vote_granted()) return;

            // votes_ is a set, so a duplicate reply from one peer cannot count
            // twice; record_vote() counts our own vote without it being there.
            if (state_.record_vote(peer)) {
                state_.become_leader(raft_log_.last_index());
                // become_leader() does not notify, so the replication threads
                // are woken here. They do not exist yet; this keeps the
                // documented contract true the moment they land.
                state_.replication_cv.notify_all();
                won.store(true);
            }
        });
    }

    for (std::thread& voter : voters) voter.join();

    // A leader may only commit an entry of its own term by counting replicas
    // (Figure 8), so until one exists commit_index can sit below the true
    // committed prefix. Read-index confirmation needs commit_index to be
    // accurate, so every leadership starts by appending one no-op.
    if (won.load()) append_leader_noop();
    return won.load();
}

void RaftElection::append_leader_noop() {
    std::uint64_t index = 0;
    std::uint64_t term = 0;

    {
        std::lock_guard append_lock(state_.append_mutex);
        {
            std::lock_guard lock(state_.state_mutex);
            // Deposed between winning and here: the next leader appends its own.
            if (state_.state() != State::Leader) return;
            term = state_.current_term();
        }

        index = raft_log_.append(term, {});

        std::lock_guard lock(state_.state_mutex);
        if (state_.state() == State::Leader && state_.current_term() == term) {
            state_.set_leader_term_first_index(index);
        }
    }

    state_.replication_cv.notify_all();
}
