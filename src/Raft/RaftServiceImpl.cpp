#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftProtoCodec.h>

#include <algorithm>
#include <exception>
#include <span>
#include <vector>

RaftServiceImpl::RaftServiceImpl(RaftState& state, RaftLog& log) : state_(state), log_(log) {}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* context, const stoneleaf::raft::RequestVoteRequest* request, stoneleaf::raft::RequestVoteResponse* response) {
    // Extract the candidate first
    NodeAddress candidate;
    try {
        candidate = NodeAddress::from_string(request->candidate());
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, e.what());
    }

    // Make sure the candidate is in the cluster config
    std::lock_guard lock(state_.state_mutex);
    if (!state_.is_peer(candidate)) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "sender not in cluster config");
    }

    // Next check the term on the request
    // if the term on the request is lower than our current term, reject the 
    // request and return the correct term
    std::uint64_t candidate_term = request->term();
    if (candidate_term < state_.current_term()) {
        response->set_term(state_.current_term());
        response->set_vote_granted(false);
        return grpc::Status::OK;
    }

    // if the request is from a higher term, become a follower to that term
    if (request->term() > state_.current_term()) state_.become_follower(request->term(), std::nullopt);
    
    const bool granted = state_.grant_vote(
        candidate,
        request->last_log_idx(),
        request->last_log_term(),
        log_.last_index(),
        log_.last_term()
    );

    if (granted) state_.reset_election_timer();

    response->set_term(state_.current_term());
    response->set_vote_granted(granted);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* context, const stoneleaf::raft::AppendEntriesRequest* request, stoneleaf::raft::AppendEntriesResponse* response) {
    // Parse the leader address from the request
    NodeAddress leader;
    try {
        leader = NodeAddress::from_string(request->leader());
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, e.what());
    }

    // Decode every entry BEFORE taking the lock and before touching the log.
    // from_proto throws on anything the domain types cannot represent, and a
    // throw after the truncation below would leave the log permanently missing
    // entries this call then refused to replace - a malformed peer message must
    // be a rejected RPC, never a half-rewritten log. Decoding needs no Raft
    // state, so doing it here also keeps the work out of the critical section.
    std::vector<RaftMutationEntry> entries;
    entries.reserve(request->entries().size());
    try {
        for (const stoneleaf::raft::RaftEntry& entry : request->entries()) {
            entries.push_back(RaftProtoCodec::from_proto(entry));
        }
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, e.what());
    }

    // Held across the log writes below, sync_through() included: one of the two
    // deliberate exceptions to the locking rule. Releasing it between the state
    // decision and the log write would let a concurrent AppendEntries from
    // another term interleave its truncation with ours, and the consistency
    // check would no longer mean anything by the time we acted on it.
    // Lock order is state_mutex -> raft log mutex, which is why state is taken
    // first even though the consistency check only reads the log.
    std::lock_guard lock(state_.state_mutex);

    // With byte-identical configs this cannot fail; if it does, two nodes
    // disagree about the cluster.
    if (!state_.is_peer(leader)) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "leader address not part of the cluster");
    }

    const std::uint64_t prev_index = request->prev_log_idx();

    // 1. Stale leader. No timer reset - a deposed leader must not be able to
    //    suppress elections.
    if (request->term() < state_.current_term()) {
        response->set_term(state_.current_term());
        response->set_success(false);
        return grpc::Status::OK;
    }

    // The term is valid, so this is the current leader. Record it and reset the
    // timer BEFORE the consistency check: a log mismatch does not mean the
    // leader is dead, it means the leader is alive and repairing us. Testing
    // leader_raft_address() here instead would skip the reset for a follower
    // that already knows this leader - the steady heartbeat case - so it would
    // campaign every timeout while being heartbeated.
    if (request->term() > state_.current_term() || state_.state() != State::Follower) {
        state_.become_follower(request->term(), leader);
    } else {
        state_.set_leader_raft_address(leader);
        state_.reset_election_timer();
    }

    // 2. Consistency check. prev_index 0 is a vacuous match: nothing precedes
    //    the first entry. term_at() returns 0 past the end of the log, so a
    //    follower whose log is too short fails here without a bounds check.
    if (prev_index > 0 && log_.term_at(prev_index) != request->prev_log_term()) {
        response->set_term(state_.current_term());
        response->set_success(false);   // leader decrements send_next and retries
        return grpc::Status::OK;
    }

    // 3. Truncate ONLY at a genuine conflict - same index, different term.
    //    Truncating unconditionally at prev_index + 1 would let a duplicated or
    //    reordered AppendEntries delete entries the leader has already counted
    //    toward a majority, committed ones included.
    //    new_from is an offset into entries, not a log index: entries[k] belongs
    //    at log index prev_index + 1 + k.
    std::size_t new_from = entries.size();   // default: everything already present
    for (std::size_t k = 0; k < entries.size(); ++k) {
        const std::uint64_t idx = prev_index + 1 + k;
        const std::uint64_t existing = log_.term_at(idx);
        if (existing == 0) {                 // past the end of our log
            new_from = k;
            break;
        }
        if (existing != entries[k].term) {   // conflict: discard it and all after
            log_.truncate_suffix(idx);
            // A just-deposed leader's sessions can now observe Failed rather
            // than waiting out COMMIT_TIMEOUT for an entry that no longer exists.
            state_.applied_cv.notify_all();
            new_from = k;
            break;
        }
        // Already present with a matching term: leave it alone, keep scanning.
    }

    // 4. Append what is actually new. new_from == entries.size() means the leader
    //    sent nothing we do not already hold - the duplicate-RPC and heartbeat
    //    case - so there is nothing to write.
    if (new_from < entries.size()) {
        log_.append_from_leader(
            prev_index + 1 + new_from,
            std::span<const RaftMutationEntry>(entries).subspan(new_from));
    }

    // Durable BEFORE replying success: otherwise the leader may count us toward
    // a majority for an entry we lose in a crash. The truncation above is
    // covered by the same sync. Guarded because sync_through rejects index 0,
    // and the sum is 0 for exactly one case - a heartbeat to a still-empty log.
    const std::uint64_t last_new_index = prev_index + entries.size();
    if (last_new_index > 0) log_.sync_through(last_new_index);

    // 5. Advance commit_index, bounded by what we actually hold: leader_commit
    //    can run ahead of our log, because the leader commits as soon as a
    //    majority has an entry and we may not be in that majority.
    //
    //    The BOUND is what must clear commit_index, not leader_commit. A leader
    //    probing backwards after a failed consistency check sends a low
    //    prev_index while still carrying a high leader_commit, so testing
    //    leader_commit would admit a bound below our own commit_index and trip
    //    set_commit_index's assert.
    const std::uint64_t commit_bound =
        std::min(request->leader_commit_idx(), last_new_index);
    if (commit_bound > state_.commit_index()) {
        state_.set_commit_index(commit_bound);
        state_.apply_cv.notify_one();   // single waiter: the apply loop
    }

    response->set_term(state_.current_term());
    response->set_success(true);
    return grpc::Status::OK;
}