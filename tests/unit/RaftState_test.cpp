#include <gtest/gtest.h>

#include <Raft/RaftState.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace {

class TempDir {
public:
    TempDir() {
        path = std::filesystem::temp_directory_path() / "stoneleaf-raft-state-XXXXXX";
        std::string value = path.string();
        value.push_back('\0');
        path = ::mkdtemp(value.data());
    }

    ~TempDir() { std::filesystem::remove_all(path); }

    std::filesystem::path path;
};

const NodeAddress SELF_RAFT{"node-a", 5001};
const NodeAddress SELF_CLIENT{"node-a", 6001};
const NodeAddress PEER_B_RAFT{"node-b", 5001};
const NodeAddress PEER_B_CLIENT{"node-b", 6001};
const NodeAddress PEER_C_RAFT{"node-c", 5001};
const NodeAddress PEER_C_CLIENT{"node-c", 6001};
const NodeAddress PEER_D_RAFT{"node-d", 5001};
const NodeAddress PEER_D_CLIENT{"node-d", 6001};
const NodeAddress PEER_E_RAFT{"node-e", 5001};
const NodeAddress PEER_E_CLIENT{"node-e", 6001};

std::vector<ClusterMember> three_node_cluster() {
    return {
        {.raft = SELF_RAFT, .database_server = SELF_CLIENT},
        {.raft = PEER_B_RAFT, .database_server = PEER_B_CLIENT},
        {.raft = PEER_C_RAFT, .database_server = PEER_C_CLIENT},
    };
}

std::vector<ClusterMember> five_node_cluster() {
    auto cluster = three_node_cluster();
    cluster.push_back({.raft = PEER_D_RAFT, .database_server = PEER_D_CLIENT});
    cluster.push_back({.raft = PEER_E_RAFT, .database_server = PEER_E_CLIENT});
    return cluster;
}

TEST(RaftStateTest, ConstructorLoadsHardStateAndPartitionsCluster) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    store.persist(7, PEER_B_RAFT.to_string());

    const auto before = std::chrono::steady_clock::now();
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 4);
    const auto after = std::chrono::steady_clock::now();

    std::lock_guard lock(state.state_mutex);
    EXPECT_EQ(state.state(), State::Follower);
    EXPECT_EQ(state.current_term(), 7u);
    ASSERT_TRUE(state.voted_for().has_value());
    EXPECT_EQ(*state.voted_for(), PEER_B_RAFT);
    EXPECT_EQ(state.self_raft_address(), SELF_RAFT);
    EXPECT_EQ(state.peers(), (std::vector<NodeAddress>{PEER_B_RAFT, PEER_C_RAFT}));
    EXPECT_EQ(state.cluster_size(), 3u);
    EXPECT_EQ(state.commit_index(), 4u);
    EXPECT_EQ(state.last_applied(), 4u);
    EXPECT_TRUE(state.is_peer(PEER_B_RAFT));
    EXPECT_FALSE(state.is_peer(SELF_RAFT));
    EXPECT_FALSE(state.leader_raft_address().has_value());
    EXPECT_FALSE(state.leader_client_address().has_value());
    EXPECT_GE(state.election_deadline(), before + RaftState::ELECTION_TIMEOUT_MIN);
    EXPECT_LE(state.election_deadline(), after + RaftState::ELECTION_TIMEOUT_MAX);
}

TEST(RaftStateTest, ConstructorRejectsMissingSelfAddressAndSkipsDuplicateRaftRows) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());

    EXPECT_THROW(
        (void)RaftState(
            three_node_cluster(), NodeAddress{"missing", 6001}, store, 0),
        std::invalid_argument);

    auto cluster = three_node_cluster();
    cluster.push_back({.raft = PEER_B_RAFT, .database_server = NodeAddress{"duplicate", 6001}});
    RaftState state(std::move(cluster), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    EXPECT_EQ(state.cluster_size(), 3u);
    EXPECT_EQ(state.peers().size(), 2u);
}

TEST(RaftStateTest, AdvanceTermClearsVoteLeaderAndPersistsHardState) {
    TempDir dir;
    {
        RaftHardStateStore store;
        store.open(dir.path.string());
        RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

        std::lock_guard lock(state.state_mutex);
        state.become_follower(2, PEER_B_RAFT);
        ASSERT_TRUE(state.leader_raft_address().has_value());
        EXPECT_EQ(state.leader_client_address(), std::optional<NodeAddress>{PEER_B_CLIENT});

        state.advance_term(3);
        EXPECT_EQ(state.current_term(), 3u);
        EXPECT_FALSE(state.voted_for().has_value());
        EXPECT_FALSE(state.leader_raft_address().has_value());
        EXPECT_FALSE(state.leader_client_address().has_value());
        EXPECT_THROW(state.advance_term(3), std::invalid_argument);
    }

    RaftHardStateStore reopened;
    reopened.open(dir.path.string());
    const RaftHardState persisted = reopened.load();
    EXPECT_EQ(persisted.term, 3u);
    EXPECT_FALSE(persisted.voted_for.has_value());
}

TEST(RaftStateTest, GrantVoteChecksLogFreshnessAndIsIdempotent) {
    TempDir dir;
    {
        RaftHardStateStore store;
        store.open(dir.path.string());
        RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

        std::lock_guard lock(state.state_mutex);
        state.advance_term(4);
        EXPECT_FALSE(state.grant_vote(PEER_B_RAFT, 100, 3, 10, 4));
        EXPECT_FALSE(state.grant_vote(PEER_B_RAFT, 9, 4, 10, 4));
        EXPECT_TRUE(state.grant_vote(PEER_B_RAFT, 10, 4, 10, 4));
        EXPECT_TRUE(state.grant_vote(PEER_B_RAFT, 10, 4, 10, 4));
        EXPECT_FALSE(state.grant_vote(PEER_C_RAFT, 20, 5, 10, 4));
        ASSERT_TRUE(state.voted_for().has_value());
        EXPECT_EQ(*state.voted_for(), PEER_B_RAFT);
    }

    RaftHardStateStore reopened;
    reopened.open(dir.path.string());
    const RaftHardState persisted = reopened.load();
    EXPECT_EQ(persisted.term, 4u);
    EXPECT_EQ(persisted.voted_for, PEER_B_RAFT.to_string());
}

TEST(RaftStateTest, GrantVotePrefersNewerLastTermOverLongerLog) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.advance_term(4);
    EXPECT_TRUE(state.grant_vote(PEER_B_RAFT, 1, 5, 100, 4));
    EXPECT_EQ(state.voted_for(), std::optional<NodeAddress>{PEER_B_RAFT});
}

TEST(RaftStateTest, BecomeCandidateStartsFreshDurableCampaign) {
    TempDir dir;
    {
        RaftHardStateStore store;
        store.open(dir.path.string());
        RaftState state(five_node_cluster(), SELF_CLIENT, store, 0);

        std::lock_guard lock(state.state_mutex);
        state.become_follower(2, PEER_B_RAFT);
        EXPECT_FALSE(state.record_vote(PEER_B_RAFT));
        EXPECT_TRUE(state.record_vote(PEER_C_RAFT));

        state.become_candidate();
        EXPECT_EQ(state.state(), State::Candidate);
        EXPECT_EQ(state.current_term(), 3u);
        ASSERT_TRUE(state.voted_for().has_value());
        EXPECT_EQ(*state.voted_for(), SELF_RAFT);
        EXPECT_FALSE(state.leader_raft_address().has_value());
        EXPECT_FALSE(state.record_vote(PEER_B_RAFT));
    }

    RaftHardStateStore reopened;
    reopened.open(dir.path.string());
    const RaftHardState persisted = reopened.load();
    EXPECT_EQ(persisted.term, 3u);
    EXPECT_EQ(persisted.voted_for, SELF_RAFT.to_string());
}

TEST(RaftStateTest, BecomeLeaderRebuildsProgressAndRecordsSelfAsLeader) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    state.become_leader(10);
    EXPECT_EQ(state.state(), State::Leader);
    EXPECT_EQ(state.leader_raft_address(), std::optional<NodeAddress>{SELF_RAFT});
    EXPECT_EQ(state.leader_client_address(), std::optional<NodeAddress>{SELF_CLIENT});
    EXPECT_EQ(state.replicated_indexes().size(), 2u);
    EXPECT_EQ(state.send_next(PEER_B_RAFT), 11u);
    EXPECT_EQ(state.send_next(PEER_C_RAFT), 11u);
    EXPECT_EQ(state.replicated_index(PEER_B_RAFT), 0u);

    state.set_send_next(PEER_B_RAFT, 3);
    state.set_replicated_index(PEER_B_RAFT, 9);
    state.become_leader(20);
    EXPECT_EQ(state.send_next(PEER_B_RAFT), 21u);
    EXPECT_EQ(state.send_next(PEER_C_RAFT), 21u);
    EXPECT_EQ(state.replicated_index(PEER_B_RAFT), 0u);
    EXPECT_EQ(state.replicated_index(PEER_C_RAFT), 0u);
}

TEST(RaftStateTest, ReadRoundConfirmsOnceAMajorityOfPeersHasAcked) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    state.become_leader(10);
    EXPECT_EQ(state.read_round(), 0u);
    EXPECT_EQ(state.confirmed_read_round(), 0u);

    const std::uint64_t round = state.start_read_round();
    EXPECT_EQ(round, 1u);
    EXPECT_EQ(state.confirmed_read_round(), 0u);

    // Three nodes: the leader plus one peer is a majority.
    state.record_read_ack(PEER_B_RAFT, round);
    EXPECT_EQ(state.confirmed_read_round(), round);

    // A stale ack never moves confirmation backwards.
    const std::uint64_t next = state.start_read_round();
    state.record_read_ack(PEER_C_RAFT, round);
    EXPECT_EQ(state.confirmed_read_round(), round);
    state.record_read_ack(PEER_C_RAFT, next);
    EXPECT_EQ(state.confirmed_read_round(), next);
}

TEST(RaftStateTest, ReadRoundNeedsTwoPeerAcksInAFiveNodeCluster) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(five_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    state.become_leader(1);
    const std::uint64_t round = state.start_read_round();

    state.record_read_ack(PEER_B_RAFT, round);
    EXPECT_EQ(state.confirmed_read_round(), 0u) << "leader + 1 of 5 is not a majority";
    state.record_read_ack(PEER_C_RAFT, round);
    EXPECT_EQ(state.confirmed_read_round(), round);
}

TEST(RaftStateTest, BecomeLeaderDropsReadAcksFromAnEarlierLeadership) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    state.become_leader(1);
    const std::uint64_t round = state.start_read_round();
    state.record_read_ack(PEER_B_RAFT, round);
    ASSERT_EQ(state.confirmed_read_round(), round);

    // A later leadership cannot inherit a peer's acknowledgement: it proved
    // that peer recognized the earlier term, which says nothing about now.
    state.become_candidate();
    state.become_leader(1);
    EXPECT_EQ(state.confirmed_read_round(), 0u);
    EXPECT_EQ(state.acked_read_round(PEER_B_RAFT), 0u);
    EXPECT_EQ(state.leader_term_first_index(), 0u);
}

TEST(RaftStateTest, BecomeFollowerTracksCurrentLeaderAndPreservesSameTermVote) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    const std::uint64_t campaign_term = state.current_term();
    state.become_follower(campaign_term, PEER_C_RAFT);

    EXPECT_EQ(state.state(), State::Follower);
    EXPECT_EQ(state.current_term(), campaign_term);
    EXPECT_EQ(state.voted_for(), std::optional<NodeAddress>{SELF_RAFT});
    EXPECT_EQ(state.leader_raft_address(), std::optional<NodeAddress>{PEER_C_RAFT});
    EXPECT_EQ(state.leader_client_address(), std::optional<NodeAddress>{PEER_C_CLIENT});

    state.become_follower(campaign_term + 1, std::nullopt);
    EXPECT_EQ(state.current_term(), campaign_term + 1);
    EXPECT_FALSE(state.voted_for().has_value());
    EXPECT_FALSE(state.leader_raft_address().has_value());
}

TEST(RaftStateTest, RecordVoteCountsSelfAndDeduplicatesPeerReplies) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(five_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    state.become_candidate();
    EXPECT_FALSE(state.record_vote(PEER_B_RAFT));
    EXPECT_FALSE(state.record_vote(PEER_B_RAFT));
    EXPECT_TRUE(state.record_vote(PEER_C_RAFT));
}

TEST(RaftStateTest, ResetElectionTimerDrawsWithinConfiguredBounds) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    RaftState state(three_node_cluster(), SELF_CLIENT, store, 0);

    std::lock_guard lock(state.state_mutex);
    for (int draw = 0; draw < 20; ++draw) {
        const auto before = std::chrono::steady_clock::now();
        state.reset_election_timer();
        const auto after = std::chrono::steady_clock::now();
        EXPECT_GE(state.election_deadline(), before + RaftState::ELECTION_TIMEOUT_MIN);
        EXPECT_LE(state.election_deadline(), after + RaftState::ELECTION_TIMEOUT_MAX);
    }
}

} // namespace
