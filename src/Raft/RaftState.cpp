#include <Raft/RaftState.h>

#include <stdexcept>
#include <utility>

namespace {
    std::mt19937::result_type random_seed() {
        thread_local std::random_device device;
        return device();
    }
}
RaftState::RaftState(std::vector<ClusterMember> cluster, const NodeAddress& self_client_address, RaftHardStateStore& hard_state_store, std::uint64_t last_applied) : hard_state_store_(hard_state_store), commit_index_(last_applied), last_applied_(last_applied), timeout_rng_(random_seed()) {
    // Set the size of the cluster. Used for majority checks
    bool found_self = false;
    // populate peers_ and self_raft_address_ from cluster
    for (const ClusterMember& member : cluster) {
        if (cluster_nodes_.contains(member.raft)) {
            continue;
        }

        if (member.database_server == self_client_address) {
            self_raft_address_ = member.raft;
            found_self = true;
        } else {
            peers_.push_back(member.raft);
        }
        cluster_nodes_[member.raft] = member.database_server;
    }

    cluster_size_ = cluster_nodes_.size();

    if (!found_self) {
        throw std::invalid_argument("Error: Couldn't find a matching row for self_client_address");
    }

    RaftHardState raft_hard_state = hard_state_store_.load();
    current_term_ = raft_hard_state.term;
    if (raft_hard_state.voted_for) {
        voted_for_ = NodeAddress::from_string(*raft_hard_state.voted_for);
    }

    reset_election_timer();
}
void RaftState::advance_term(std::uint64_t new_term,
                             std::optional<NodeAddress> new_vote) {
    // Validate the new term argument
    if (new_term <= current_term_) {
        throw std::invalid_argument("Error (RaftState.advance_term): Invalid new term value.");
    }

    std::optional<std::string> persisted_vote;
    if (new_vote) {
        persisted_vote = new_vote->to_string();
    }

    // Persist the complete new hard state before changing in-memory state.
    hard_state_store_.persist(new_term, persisted_vote);

    // On a new term, replace the vote and clear peer votes from the old campaign.
    voted_for_ = new_vote;
    votes_.clear();
    leader_raft_address_.reset();

    // Update to the new term
    current_term_ = new_term;
}

bool RaftState::grant_vote(const NodeAddress& candidate, std::uint64_t candidate_last_log_index, std::uint64_t candidate_last_log_term, std::uint64_t own_last_log_index, std::uint64_t own_last_log_term) {
    if (voted_for_ && *voted_for_ != candidate) {
        return false;
    }
    
    const bool up_to_date = (candidate_last_log_term > own_last_log_term || (candidate_last_log_term == own_last_log_term && candidate_last_log_index >= own_last_log_index));
    if (!up_to_date) {
        return false;
    }

    // Re-granting the same vote needs no write: the durable state already says
    // exactly this, so a retried RequestVote must not cost an fsync.
    if (voted_for_ != candidate) {
        hard_state_store_.persist(current_term_, candidate.to_string());
        voted_for_ = candidate;
    }
   
    // The caller resets the election timer on true return
    return true;

}

void RaftState::become_candidate() {
    // Advance the term and self-vote in one durable state transition.
    advance_term(current_term_ + 1, self_raft_address_);
    state_ = State::Candidate;
    reset_election_timer();
}

void RaftState::become_leader(std::uint64_t last_log_index) {
    send_next_.clear();
    replicated_index_.clear();
    // Read confirmation is per leadership for the same reason progress is: an
    // acknowledgement from an earlier term proves nothing about this one, and
    // the no-op index belongs to the leadership that appended it.
    acked_read_round_.clear();
    confirmed_read_round_ = 0;
    leader_term_first_index_ = 0;
    for (const NodeAddress& peer : peers_) {
        send_next_.emplace(peer, last_log_index + 1);
        replicated_index_.emplace(peer, 0);
        acked_read_round_.emplace(peer, 0);
    }
    leader_raft_address_ = self_raft_address_;
    state_ = State::Leader;
}

void RaftState::become_follower(std::uint64_t new_term,
                                 std::optional<NodeAddress> leader) {
    if (new_term > current_term_) {
        advance_term(new_term);
    }

    leader_raft_address_ = std::move(leader);
    state_ = State::Follower;
    reset_election_timer();
    election_cv.notify_all();
    read_cv.notify_all();   // waiting readers must be told to give up, not stall
}

std::optional<NodeAddress> RaftState::leader_client_address() const {
    if (!leader_raft_address_) {
        return std::nullopt;
    }

    return cluster_nodes_.at(*leader_raft_address_);
}

void RaftState::reset_election_timer() {
    
    std::uniform_int_distribution<std::chrono::milliseconds::rep> distribution{
        ELECTION_TIMEOUT_MIN.count(),
        ELECTION_TIMEOUT_MAX.count()
    };
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds{distribution(timeout_rng_)};
}
