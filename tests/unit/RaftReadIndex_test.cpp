#include <gtest/gtest.h>

#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftReadIndex.h>
#include <Raft/RaftState.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

namespace {

class TempDir {
public:
    TempDir() {
        path = std::filesystem::temp_directory_path() / "stoneleaf-read-index-XXXXXX";
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

std::vector<ClusterMember> cluster(std::size_t size) {
    std::vector<ClusterMember> nodes{{.raft = SELF_RAFT, .database_server = SELF_CLIENT}};
    if (size > 1) nodes.push_back({.raft = PEER_B_RAFT, .database_server = PEER_B_CLIENT});
    if (size > 2) nodes.push_back({.raft = PEER_C_RAFT, .database_server = PEER_C_CLIENT});
    return nodes;
}

struct Fixture {
    explicit Fixture(std::size_t size) {
        store.open(dir.path.string());
        state = std::make_unique<RaftState>(cluster(size), SELF_CLIENT, store, 0);
    }
    // A leader that has committed and applied its leadership no-op.
    void lead_and_settle(std::uint64_t index = 1) {
        std::lock_guard lock(state->state_mutex);
        state->become_candidate();
        state->become_leader(index - 1);
        state->set_leader_term_first_index(index);
        state->set_commit_index(index);
        state->set_last_applied(index);
    }
    TempDir dir;
    RaftHardStateStore store;
    std::unique_ptr<RaftState> state;
};

TEST(RaftReadIndexTest, AFollowerRefusesToServeAConsistentRead) {
    Fixture fixture(3);
    RaftReadIndex read_index(*fixture.state);

    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::NotLeader);
}

TEST(RaftReadIndexTest, ASingleNodeLeaderIsItsOwnMajority) {
    Fixture fixture(1);
    fixture.lead_and_settle();
    RaftReadIndex read_index(*fixture.state);

    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::Ready);
}

TEST(RaftReadIndexTest, ReadsWaitForAPeerToConfirmTheRound) {
    Fixture fixture(3);
    fixture.lead_and_settle();
    RaftReadIndex read_index(*fixture.state);

    // Stands in for the replication thread: opens the requested round, as it
    // would when building its next AppendEntries, then credits the reply.
    std::thread peer([&] {
        for (int attempt = 0; attempt < 200; ++attempt) {
            {
                std::lock_guard lock(fixture.state->state_mutex);
                if (fixture.state->read_round_wanted()) {
                    const std::uint64_t round = fixture.state->open_read_round();
                    fixture.state->record_read_ack(PEER_B_RAFT, round);
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::Ready);
    peer.join();
}

TEST(RaftReadIndexTest, APartitionedLeaderTimesOutInsteadOfServing) {
    // No peer ever answers, so the round is never confirmed: exactly the
    // partitioned-leader case a consistent read must refuse.
    Fixture fixture(3);
    fixture.lead_and_settle();
    RaftReadIndex read_index(*fixture.state);

    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::Timeout);
    EXPECT_GE(std::chrono::steady_clock::now() - started, RaftReadIndex::READ_TIMEOUT);
}

TEST(RaftReadIndexTest, ReadsWaitForTheLeadershipNoOpToCommit) {
    Fixture fixture(3);
    {
        std::lock_guard lock(fixture.state->state_mutex);
        fixture.state->become_candidate();
        fixture.state->become_leader(0);
        fixture.state->set_leader_term_first_index(1);   // appended, not yet committed
    }
    RaftReadIndex read_index(*fixture.state);

    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::Timeout);
}

TEST(RaftReadIndexTest, LosingLeadershipWhileWaitingReportsNotLeader) {
    Fixture fixture(3);
    fixture.lead_and_settle();
    RaftReadIndex read_index(*fixture.state);

    std::thread deposer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard lock(fixture.state->state_mutex);
        fixture.state->become_follower(fixture.state->current_term() + 1, std::nullopt);
        fixture.state->read_cv.notify_all();
    });

    EXPECT_EQ(read_index.wait_until_readable(), ReadIndexStatus::NotLeader);
    deposer.join();
}

} // namespace
