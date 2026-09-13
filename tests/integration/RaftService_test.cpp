#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftProtoCodec.h>
#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftState.h>
#include <ValueCodec.h>
#include <storage/Index.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {

namespace raftpb = stoneleaf::raft;

Config raft_config() {
    return {.max_index_bytes = 1000 * Index::ENTRY_SIZE,
            .max_store_bytes = 16 * 1024 * 1024,
            .initial_lsn = 1};
}

Key key_for(std::uint64_t id) { return KeyCodec::encode(KeyInput{id}).value(); }
Value value_for(const std::string& text) { return ValueCodec::encode(ValueInput{text}).value(); }

MutationOp put_op(std::uint64_t id, const std::string& text) {
    return {.type = RaftMutationType::Put, .operation = PutMutation{key_for(id), value_for(text)}};
}

const NodeAddress SELF_RAFT{"node-a", 5001};
const NodeAddress SELF_CLIENT{"node-a", 6001};
const NodeAddress LEADER_RAFT{"node-b", 5001};
const NodeAddress LEADER_CLIENT{"node-b", 6001};
const NodeAddress THIRD_RAFT{"node-c", 5001};
const NodeAddress THIRD_CLIENT{"node-c", 6001};

class RaftServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        const std::string suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() / ("stoneleafdb_raft_service_" + suffix);
        std::filesystem::create_directories(temp_dir);

        raft_log = std::make_unique<RaftLog>(raft_config());
        raft_log->open((temp_dir / "test.raft").string());

        hard_state = std::make_unique<RaftHardStateStore>();
        hard_state->open((temp_dir / "hardstate").string());

        // Three nodes, so a majority is two. self_client picks our row out of
        // the config; the other two rows become peers.
        state = std::make_unique<RaftState>(
            std::vector<ClusterMember>{
                {.raft = SELF_RAFT, .database_server = SELF_CLIENT},
                {.raft = LEADER_RAFT, .database_server = LEADER_CLIENT},
                {.raft = THIRD_RAFT, .database_server = THIRD_CLIENT},
            },
            SELF_CLIENT,
            *hard_state,
            0);

        service = std::make_unique<RaftServiceImpl>(*state, *raft_log);
    }

    void TearDown() override {
        service.reset();
        state.reset();
        hard_state.reset();
        raft_log.reset();

        std::error_code error;
        std::filesystem::remove_all(temp_dir, error);
    }

    // Neither handler dereferences context, so the tests pass nullptr rather
    // than standing up a real ServerContext, which is only constructible from
    // inside the gRPC machinery.
    grpc::Status append(const raftpb::AppendEntriesRequest& request,
                        raftpb::AppendEntriesResponse* response) {
        return service->AppendEntries(nullptr, &request, response);
    }

    grpc::Status request_vote(std::uint64_t term, const NodeAddress& candidate,
                              std::uint64_t last_log_idx, std::uint64_t last_log_term,
                              raftpb::RequestVoteResponse* response) {
        raftpb::RequestVoteRequest request;
        request.set_term(term);
        request.set_candidate(candidate.to_string());
        request.set_last_log_idx(last_log_idx);
        request.set_last_log_term(last_log_term);
        return service->RequestVote(nullptr, &request, response);
    }

    static raftpb::AppendEntriesRequest make_request(
        std::uint64_t term, std::uint64_t prev_index,
        std::uint64_t prev_term, std::uint64_t leader_commit) {
        raftpb::AppendEntriesRequest request;
        request.set_term(term);
        request.set_leader(LEADER_RAFT.to_string());
        request.set_prev_log_idx(prev_index);
        request.set_prev_log_term(prev_term);
        request.set_leader_commit_idx(leader_commit);
        return request;
    }

    static void add_entry(raftpb::AppendEntriesRequest& request, std::uint64_t term,
                          std::uint64_t idx, const std::vector<MutationOp>& operations) {
        const RaftMutationEntry entry{.term = term, .idx = idx, .operations = operations};
        RaftProtoCodec::to_proto(entry, request.add_entries());
    }

    // Every accessor below takes state_mutex, as all RaftState callers must.
    std::uint64_t current_term() {
        std::lock_guard lock(state->state_mutex);
        return state->current_term();
    }
    std::uint64_t commit_index() {
        std::lock_guard lock(state->state_mutex);
        return state->commit_index();
    }
    State node_state() {
        std::lock_guard lock(state->state_mutex);
        return state->state();
    }
    std::optional<NodeAddress> leader() {
        std::lock_guard lock(state->state_mutex);
        return state->leader_raft_address();
    }
    std::chrono::steady_clock::time_point election_deadline() {
        std::lock_guard lock(state->state_mutex);
        return state->election_deadline();
    }

    // Waits out the longest possible election timeout so that a subsequent
    // reset is observable. Comparing two random draws directly would be a coin
    // flip, since both come from the same [MIN, MAX] range.
    static void let_the_deadline_lapse() {
        std::this_thread::sleep_for(RaftState::ELECTION_TIMEOUT_MAX + std::chrono::milliseconds(20));
    }

    std::filesystem::path temp_dir;
    std::unique_ptr<RaftLog> raft_log;
    std::unique_ptr<RaftHardStateStore> hard_state;
    std::unique_ptr<RaftState> state;
    std::unique_ptr<RaftServiceImpl> service;
};

// --- AppendEntries: term handling -------------------------------------------

TEST_F(RaftServiceTest, StaleLeaderIsRejectedAndDoesNotResetTheTimer) {
    {
        std::lock_guard lock(state->state_mutex);
        state->advance_term(5);
    }
    let_the_deadline_lapse();
    ASSERT_LT(election_deadline(), std::chrono::steady_clock::now());

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(3, 0, 0, 0), &response).ok());

    EXPECT_FALSE(response.success());
    EXPECT_EQ(response.term(), 5u);
    // A deposed leader must not be able to suppress our elections.
    EXPECT_LT(election_deadline(), std::chrono::steady_clock::now());
}

TEST_F(RaftServiceTest, AHigherTermMakesUsAFollowerAndRecordsTheLeader) {
    {
        std::lock_guard lock(state->state_mutex);
        state->become_candidate();
    }
    ASSERT_EQ(node_state(), State::Candidate);

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(4, 0, 0, 0), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(node_state(), State::Follower);
    EXPECT_EQ(current_term(), 4u);
    EXPECT_EQ(leader(), std::optional<NodeAddress>{LEADER_RAFT});
}

TEST_F(RaftServiceTest, ACandidateInTheSameTermStepsDown) {
    {
        std::lock_guard lock(state->state_mutex);
        state->become_candidate();   // term 1, voted for ourselves
    }
    ASSERT_EQ(current_term(), 1u);

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 0, 0, 0), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(node_state(), State::Follower);
    EXPECT_EQ(leader(), std::optional<NodeAddress>{LEADER_RAFT});
}

TEST_F(RaftServiceTest, HeartbeatFromTheCurrentLeaderResetsTheElectionTimer) {
    raftpb::AppendEntriesResponse first;
    ASSERT_TRUE(append(make_request(1, 0, 0, 0), &first).ok());
    ASSERT_TRUE(first.success());
    ASSERT_EQ(leader(), std::optional<NodeAddress>{LEADER_RAFT});

    let_the_deadline_lapse();
    ASSERT_LT(election_deadline(), std::chrono::steady_clock::now());

    // Steady state: same term, leader already recorded. Gating the reset on
    // leader_raft_address() being unset would skip exactly this path, and the
    // follower would campaign every timeout while being heartbeated.
    raftpb::AppendEntriesResponse second;
    ASSERT_TRUE(append(make_request(1, 0, 0, 0), &second).ok());

    EXPECT_TRUE(second.success());
    EXPECT_GT(election_deadline(), std::chrono::steady_clock::now());
}

// --- AppendEntries: membership and malformed input --------------------------

TEST_F(RaftServiceTest, RejectsALeaderOutsideTheClusterConfig) {
    raftpb::AppendEntriesRequest request = make_request(1, 0, 0, 0);
    request.set_leader(NodeAddress{"ghost", 9999}.to_string());

    raftpb::AppendEntriesResponse response;
    EXPECT_EQ(append(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(RaftServiceTest, RejectsAMalformedLeaderAddress) {
    raftpb::AppendEntriesRequest request = make_request(1, 0, 0, 0);
    request.set_leader("not-an-address");

    raftpb::AppendEntriesResponse response;
    EXPECT_EQ(append(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(RaftServiceTest, AMalformedEntryLeavesTheLogUntouched) {
    raft_log->append(1, {put_op(1, "a")});
    raft_log->append(1, {put_op(2, "b")});
    raft_log->append(1, {put_op(3, "c")});

    raftpb::AppendEntriesRequest request = make_request(2, 0, 0, 0);
    // A well-formed entry that conflicts at index 1, so a handler that wrote to
    // the log before decoding would truncate here...
    add_entry(request, 2, 1, {put_op(1, "a")});
    // ...followed by one the domain types cannot represent.
    raftpb::RaftEntry* bad = request.add_entries();
    bad->set_term(2);
    bad->set_idx(2);
    raftpb::Mutation* mutation = bad->add_operations();
    mutation->set_type(raftpb::MUTATION_UNSPECIFIED);
    RaftProtoCodec::to_proto(key_for(7), mutation->mutable_key());

    raftpb::AppendEntriesResponse response;
    EXPECT_EQ(append(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);

    // Every entry is decoded before the log is touched, so the conflict was
    // never acted on: a rejected RPC must never leave a half-rewritten log.
    EXPECT_EQ(raft_log->last_index(), 3u);
    EXPECT_EQ(raft_log->term_at(1), 1u);
    EXPECT_EQ(raft_log->term_at(3), 1u);
}

// --- AppendEntries: consistency check ---------------------------------------

TEST_F(RaftServiceTest, ConsistencyCheckFailsWhenOurLogIsTooShort) {
    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 5, 1, 0), &response).ok());

    EXPECT_FALSE(response.success());
    EXPECT_EQ(response.term(), 1u);
    // The mismatch means the leader is alive and repairing us, so the timer is
    // still reset - it is reset before the check runs.
    EXPECT_GT(election_deadline(), std::chrono::steady_clock::now());
}

TEST_F(RaftServiceTest, ConsistencyCheckFailsOnATermMismatch) {
    raft_log->append(1, {put_op(1, "a")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(3, 1, 2, 0), &response).ok());

    EXPECT_FALSE(response.success());
    EXPECT_EQ(raft_log->last_index(), 1u);   // a failed check truncates nothing
}

// --- AppendEntries: heartbeats ----------------------------------------------

TEST_F(RaftServiceTest, HeartbeatToAnEmptyLogSucceeds) {
    ASSERT_EQ(raft_log->last_index(), 0u);

    // prev_index 0 with no entries makes the sync target 0, and sync_through
    // rejects index 0. Unguarded, this throws on the first heartbeat of every
    // fresh cluster.
    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 0, 0, 0), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(raft_log->last_index(), 0u);
}

TEST_F(RaftServiceTest, HeartbeatOverANonEmptyLogChangesNothing) {
    raft_log->append(1, {put_op(1, "a")});
    raft_log->append(1, {put_op(2, "b")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 2, 1, 0), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(raft_log->last_index(), 2u);
}

// --- AppendEntries: append and truncate ------------------------------------

TEST_F(RaftServiceTest, AppendsNewEntriesAndMakesThemDurable) {
    raftpb::AppendEntriesRequest request = make_request(1, 0, 0, 0);
    add_entry(request, 1, 1, {put_op(1, "one")});
    add_entry(request, 1, 2, {put_op(2, "two")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(request, &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(raft_log->last_index(), 2u);
    // Durable before the reply, or the leader may count us toward a majority
    // for an entry we would lose in a crash.
    EXPECT_EQ(raft_log->durable_index(), 2u);
}

TEST_F(RaftServiceTest, AppendsOnlyTheSuffixThatIsActuallyNew) {
    raft_log->append(1, {put_op(1, "a")});
    raft_log->append(1, {put_op(2, "b")});

    // The leader resends 1 and 2 and adds 3.
    raftpb::AppendEntriesRequest request = make_request(1, 0, 0, 0);
    add_entry(request, 1, 1, {put_op(1, "a")});
    add_entry(request, 1, 2, {put_op(2, "b")});
    add_entry(request, 1, 3, {put_op(3, "c")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(request, &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(raft_log->last_index(), 3u);
}

TEST_F(RaftServiceTest, ADuplicateRequestDoesNotTruncateEntriesItAlreadyHas) {
    for (std::uint64_t id = 1; id <= 5; ++id) raft_log->append(2, {put_op(id, "v")});
    ASSERT_EQ(raft_log->last_index(), 5u);

    // A retransmit of an earlier, smaller batch: the leader never saw our ack.
    raftpb::AppendEntriesRequest request = make_request(2, 0, 0, 0);
    add_entry(request, 2, 1, {put_op(1, "v")});
    add_entry(request, 2, 2, {put_op(2, "v")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(request, &response).ok());

    EXPECT_TRUE(response.success());
    // Truncating at prev_index + 1 unconditionally would have dropped 3, 4 and
    // 5 - entries the leader may already have counted toward a majority.
    EXPECT_EQ(raft_log->last_index(), 5u);
}

TEST_F(RaftServiceTest, AConflictTruncatesFromTheConflictingIndex) {
    raft_log->append(1, {put_op(1, "a")});
    raft_log->append(1, {put_op(2, "b")});
    raft_log->append(1, {put_op(3, "c")});
    ASSERT_EQ(raft_log->last_index(), 3u);

    // A new leader in term 2 replaces index 2 onwards.
    raftpb::AppendEntriesRequest request = make_request(2, 1, 1, 0);
    add_entry(request, 2, 2, {put_op(9, "z")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(request, &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(raft_log->last_index(), 2u);
    EXPECT_EQ(raft_log->term_at(1), 1u);   // the matching prefix survives
    EXPECT_EQ(raft_log->term_at(2), 2u);   // the conflict was replaced
}

// --- AppendEntries: commit index -------------------------------------------

TEST_F(RaftServiceTest, CommitIndexIsBoundedByWhatWeActuallyHold) {
    raftpb::AppendEntriesRequest request = make_request(1, 0, 0, 100);
    add_entry(request, 1, 1, {put_op(1, "one")});
    add_entry(request, 1, 2, {put_op(2, "two")});

    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(request, &response).ok());

    EXPECT_TRUE(response.success());
    // Not 100: the leader commits once a majority holds an entry, and we may
    // not be in that majority.
    EXPECT_EQ(commit_index(), 2u);
}

TEST_F(RaftServiceTest, HeartbeatAdvancesCommitIndex) {
    raft_log->append(1, {put_op(1, "a")});
    raft_log->append(1, {put_op(2, "b")});

    // A heartbeat is not a special case: no entries, but it still commits.
    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 2, 1, 2), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(commit_index(), 2u);
}

TEST_F(RaftServiceTest, CommitIndexNeverMovesBackwards) {
    for (std::uint64_t id = 1; id <= 5; ++id) raft_log->append(1, {put_op(id, "v")});
    {
        std::lock_guard lock(state->state_mutex);
        state->set_commit_index(5);
    }

    // A leader probing backwards sends a low prev_index while still carrying a
    // high leader_commit. Gating on leader_commit > commit_index would pass here
    // and then hand set_commit_index a bound of 1, below the current 5.
    raftpb::AppendEntriesResponse response;
    ASSERT_TRUE(append(make_request(1, 1, 1, 60), &response).ok());

    EXPECT_TRUE(response.success());
    EXPECT_EQ(commit_index(), 5u);
}

// --- RequestVote -----------------------------------------------------------

TEST_F(RaftServiceTest, GrantsAVoteToAnUpToDateCandidate) {
    raftpb::RequestVoteResponse response;
    ASSERT_TRUE(request_vote(1, LEADER_RAFT, 0, 0, &response).ok());

    EXPECT_TRUE(response.vote_granted());
    EXPECT_EQ(response.term(), 1u);
    EXPECT_GT(election_deadline(), std::chrono::steady_clock::now());
}

TEST_F(RaftServiceTest, DeniesAVoteToACandidateWhoseLogIsBehind) {
    raft_log->append(2, {put_op(1, "a")});

    // Candidate's last log term is behind ours, so it is not up to date.
    raftpb::RequestVoteResponse response;
    ASSERT_TRUE(request_vote(3, LEADER_RAFT, 1, 1, &response).ok());

    EXPECT_FALSE(response.vote_granted());
}

TEST_F(RaftServiceTest, DeniesASecondVoteInTheSameTerm) {
    raftpb::RequestVoteResponse first;
    ASSERT_TRUE(request_vote(1, LEADER_RAFT, 0, 0, &first).ok());
    ASSERT_TRUE(first.vote_granted());

    raftpb::RequestVoteResponse second;
    ASSERT_TRUE(request_vote(1, THIRD_RAFT, 0, 0, &second).ok());

    EXPECT_FALSE(second.vote_granted());
}

TEST_F(RaftServiceTest, RejectsAStaleTermRequestVote) {
    {
        std::lock_guard lock(state->state_mutex);
        state->advance_term(5);
    }

    raftpb::RequestVoteResponse response;
    ASSERT_TRUE(request_vote(2, LEADER_RAFT, 0, 0, &response).ok());

    EXPECT_FALSE(response.vote_granted());
    EXPECT_EQ(response.term(), 5u);
}

TEST_F(RaftServiceTest, RejectsACandidateOutsideTheClusterConfig) {
    raftpb::RequestVoteResponse response;
    EXPECT_EQ(request_vote(1, NodeAddress{"ghost", 9999}, 0, 0, &response).error_code(),
              grpc::StatusCode::INVALID_ARGUMENT);
}

} // namespace
