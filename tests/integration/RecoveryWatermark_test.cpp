#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Raft/RaftApplier.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>
#include <Recovery.h>
#include <ValueCodec.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace {

Config recovery_wal_config() {
    return {.max_index_bytes = 1000 * Index::ENTRY_SIZE, .max_store_bytes = 16 * 1024 * 1024, .initial_lsn = 1};
}

// As server.cpp's setup_config: once cleanup has run, the log no longer
// starts at LSN 1, so reopen it at its smallest surviving segment.
Config reopened_wal_config(const std::string& db_file) {
    Config config = recovery_wal_config();
    const std::string wal_directory = db_file + ".wal";
    if (!std::filesystem::exists(wal_directory)) return config;
    std::optional<std::uint64_t> smallest_base;
    for (const auto& entry : std::filesystem::directory_iterator(wal_directory)) {
        const auto [base, is_store] = parse_wal_segment_name(entry.path().filename().string());
        if (is_store && (!smallest_base || base < *smallest_base)) smallest_base = base;
    }
    config.initial_lsn = smallest_base.value_or(1);
    return config;
}

MutationOp put_op(std::uint64_t id, const std::string& text) {
    return {.type = RaftMutationType::Put,
            .operation = PutMutation{KeyCodec::encode(KeyInput{id}).value(),
                                     ValueCodec::encode(ValueInput{text}).value()}};
}

const NodeAddress SELF_RAFT{"node-a", 5001};
const NodeAddress SELF_CLIENT{"node-a", 6001};
const NodeAddress PEER_RAFT{"node-b", 5001};
const NodeAddress PEER_CLIENT{"node-b", 6001};

// Brings up a node, applies Raft entries through RaftApplier, then tears
// everything down so recovery can be run over the WAL it left behind.
class RecoveryWatermarkTest : public ::testing::Test {
protected:
    void SetUp() override {
        const std::string suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() / ("stoneleafdb_watermark_" + suffix);
        std::filesystem::create_directories(temp_dir);
        db_file = (temp_dir / "test.db").string();
    }

    void TearDown() override {
        std::error_code error;
        std::filesystem::remove_all(temp_dir, error);
    }

    // Applies one entry per operation list and shuts the node down cleanly.
    void apply_entries(const std::vector<std::vector<MutationOp>>& entries,
                       bool leave_one_uncommitted = false) {
        KeyStore store;
        LockManager lock_manager;
        Log wal(recovery_wal_config());
        wal.open(db_file + ".wal");
        TransactionManager transaction_manager(wal, lock_manager, store);
        store.attach_transaction_manager(transaction_manager);
        ASSERT_EQ(store.open(db_file), KeyStoreStatus::Success);

        RaftLog raft_log(recovery_wal_config());
        raft_log.open(db_file + ".raft");
        RaftHardStateStore hard_state;
        hard_state.open((temp_dir / "hardstate").string());
        RaftState state(
            std::vector<ClusterMember>{
                {.raft = SELF_RAFT, .database_server = SELF_CLIENT},
                {.raft = PEER_RAFT, .database_server = PEER_CLIENT},
            },
            SELF_CLIENT, hard_state, 0);

        for (const std::vector<MutationOp>& operations : entries) {
            raft_log.append(1, operations);
        }
        {
            std::lock_guard lock(state.state_mutex);
            state.set_commit_index(raft_log.last_index());
        }

        RaftApplier applier(state, raft_log, store, transaction_manager, wal);
        std::size_t applied = 0;
        while (std::size_t batch = applier.apply_pending_batch()) applied += batch;
        EXPECT_EQ(applied, entries.size());

        if (leave_one_uncommitted) {
            // A transaction that wrote but never committed: ARIES rolls it back,
            // and it must not count toward the watermark.
            TransactionHandle orphan = transaction_manager.begin();
            ASSERT_EQ(
                store.put(orphan, KeyCodec::encode(KeyInput{std::uint64_t{99}}).value(),
                          ValueCodec::encode(ValueInput{std::string{"orphan"}}).value()),
                KeyStoreStatus::Success);
            wal.sync_through(orphan->last_lsn());
            // Return without closing: the transaction stays open and the node
            // goes away with it, which is what a crash leaves behind for
            // recovery. KeyStore::close() would refuse anyway while a
            // transaction is active.
            return;
        }

        ASSERT_EQ(store.close(), KeyStoreStatus::Success);
    }

    // Runs analysis + redo over the WAL left on disk and returns the watermark.
    std::uint64_t recovered_watermark() {
        Log wal(reopened_wal_config(db_file));
        wal.open(db_file + ".wal");
        std::unordered_map<TransactionId, Lsn> unresolved;
        std::uint64_t last_applied = 0;
        aries_recovery_redo(wal, db_file, unresolved, last_applied);
        return last_applied;
    }

    // One startup as server.cpp performs it: redo, undo through the live
    // KeyStore, then persist the watermark and delete finalized segments.
    void restart_like_server() {
        KeyStore store;
        LockManager lock_manager;
        Log wal(reopened_wal_config(db_file));
        TransactionManager transaction_manager(wal, lock_manager, store);
        store.attach_transaction_manager(transaction_manager);
        wal.open(db_file + ".wal");

        std::unordered_map<TransactionId, Lsn> unresolved;
        std::uint64_t last_applied = 0;
        aries_recovery_redo(wal, db_file, unresolved, last_applied);
        ASSERT_EQ(store.open(db_file), KeyStoreStatus::Success);
        aries_recovery_undo(wal, store, unresolved);
        finish_recovery(transaction_manager, db_file, last_applied);
        ASSERT_EQ(store.close(), KeyStoreStatus::Success);
    }

    std::filesystem::path temp_dir;
    std::string db_file;
};

TEST_F(RecoveryWatermarkTest, ReportsZeroForAFreshDatabase) {
    EXPECT_EQ(recovered_watermark(), 0u);
}

TEST_F(RecoveryWatermarkTest, ReportsTheHighestAppliedRaftIndex) {
    apply_entries({{put_op(1, "one")}, {put_op(2, "two")}, {put_op(3, "three")}});

    EXPECT_EQ(recovered_watermark(), 3u);
}

TEST_F(RecoveryWatermarkTest, IgnoresATransactionThatNeverCommitted) {
    apply_entries({{put_op(1, "one")}, {put_op(2, "two")}}, /*leave_one_uncommitted=*/true);

    EXPECT_EQ(recovered_watermark(), 2u);
}

TEST_F(RecoveryWatermarkTest, ClientTransactionsDoNotMoveTheWatermark) {
    apply_entries({{put_op(1, "one")}});

    {
        KeyStore store;
        LockManager lock_manager;
        Log wal(recovery_wal_config());
        wal.open(db_file + ".wal");
        TransactionManager transaction_manager(wal, lock_manager, store);
        store.attach_transaction_manager(transaction_manager);
        ASSERT_EQ(store.open(db_file), KeyStoreStatus::Success);

        // A client write: commits normally, carries no Raft index.
        TransactionHandle client = transaction_manager.begin();
        ASSERT_EQ(
            store.put(client, KeyCodec::encode(KeyInput{std::uint64_t{7}}).value(),
                      ValueCodec::encode(ValueInput{std::string{"client"}}).value()),
            KeyStoreStatus::Success);
        ASSERT_EQ(transaction_manager.commit(client), CommitStatus::Success);
        ASSERT_EQ(store.close(), KeyStoreStatus::Success);
    }

    EXPECT_EQ(recovered_watermark(), 1u);
}

TEST_F(RecoveryWatermarkTest, SurvivesCleanupOfEverySegmentThatCarriedIt) {
    // Client transactions write WAL records but carry no Raft index. Enough of
    // them after the applied entries leave the newest segment - the only one
    // cleanup keeps - with no commit record that knows the watermark.
    apply_entries({{put_op(1, "a")}, {put_op(2, "b")}, {put_op(3, "c")}});
    {
        KeyStore store;
        LockManager lock_manager;
        Log wal(reopened_wal_config(db_file));
        wal.open(db_file + ".wal");
        TransactionManager transaction_manager(wal, lock_manager, store);
        store.attach_transaction_manager(transaction_manager);
        ASSERT_EQ(store.open(db_file), KeyStoreStatus::Success);
        for (int i = 0; i < 1500; ++i) {
            TransactionHandle client = transaction_manager.begin();
            ASSERT_EQ(transaction_manager.commit(client, Durability::Defer), CommitStatus::Success);
        }
        ASSERT_EQ(store.close(), KeyStoreStatus::Success);
    }

    restart_like_server();
    EXPECT_EQ(recovered_watermark(), 3u);
    restart_like_server();
    EXPECT_EQ(recovered_watermark(), 3u);
}

} // namespace
