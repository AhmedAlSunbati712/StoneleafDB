#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Raft/RaftApplier.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>
#include <ValueCodec.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {

Config wal_config() {
    return {.max_index_bytes = 1000 * Index::ENTRY_SIZE, .max_store_bytes = 16 * 1024 * 1024, .initial_lsn = 1};
}

Key key_for(std::uint64_t id) {
    return KeyCodec::encode(KeyInput{id}).value();
}

Value value_for(const std::string& text) {
    return ValueCodec::encode(ValueInput{text}).value();
}

MutationOp put_op(std::uint64_t id, const std::string& text) {
    return {.type = RaftMutationType::Put, .operation = PutMutation{key_for(id), value_for(text)}};
}

MutationOp delete_op(std::uint64_t id) {
    return {.type = RaftMutationType::Delete, .operation = DeleteMutation{key_for(id)}};
}

const NodeAddress SELF_RAFT{"node-a", 5001};
const NodeAddress SELF_CLIENT{"node-a", 6001};
const NodeAddress PEER_RAFT{"node-b", 5001};
const NodeAddress PEER_CLIENT{"node-b", 6001};

class RaftApplierTest : public ::testing::Test {
protected:
    void SetUp() override {
        const std::string suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() / ("stoneleafdb_raft_applier_" + suffix);
        std::filesystem::create_directories(temp_dir);

        wal = std::make_unique<Log>(wal_config());
        wal->open((temp_dir / "wal").string());
        transaction_manager = std::make_unique<TransactionManager>(*wal, lock_manager, store);
        store.attach_transaction_manager(*transaction_manager);
        ASSERT_EQ(store.open((temp_dir / "test.db").string()), KeyStoreStatus::Success);

        raft_log = std::make_unique<RaftLog>(wal_config());
        raft_log->open((temp_dir / "test.raft").string());

        hard_state = std::make_unique<RaftHardStateStore>();
        hard_state->open((temp_dir / "hardstate").string());

        state = std::make_unique<RaftState>(
            std::vector<ClusterMember>{
                {.raft = SELF_RAFT, .database_server = SELF_CLIENT},
                {.raft = PEER_RAFT, .database_server = PEER_CLIENT},
            },
            SELF_CLIENT,
            *hard_state,
            0);
    }

    void TearDown() override {
        EXPECT_EQ(store.close(), KeyStoreStatus::Success);
        state.reset();
        hard_state.reset();
        raft_log.reset();
        transaction_manager.reset();
        wal.reset();

        std::error_code error;
        std::filesystem::remove_all(temp_dir, error);
    }

    // Appends one entry per operation list and marks everything committed.
    void append_and_commit(const std::vector<std::vector<MutationOp>>& entries) {
        for (const std::vector<MutationOp>& operations : entries) {
            raft_log->append(1, operations);
        }
        std::lock_guard lock(state->state_mutex);
        state->set_commit_index(raft_log->last_index());
    }

    std::uint64_t last_applied() {
        std::lock_guard lock(state->state_mutex);
        return state->last_applied();
    }

    void expect_value(std::uint64_t id, const std::string& text) {
        TransactionHandle transaction = transaction_manager->begin();
        const KeyStoreGetResult result = store.get(transaction, key_for(id));
        ASSERT_EQ(result.status, KeyStoreStatus::Success);
        ASSERT_TRUE(result.value.has_value());
        EXPECT_EQ(ValueCodec::decode(*result.value), std::optional<ValueInput>{ValueInput{text}});
        ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
    }

    void expect_missing(std::uint64_t id) {
        TransactionHandle transaction = transaction_manager->begin();
        EXPECT_EQ(store.get(transaction, key_for(id)).status, KeyStoreStatus::KeyNotFound);
        ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
    }

    std::filesystem::path temp_dir;
    KeyStore store;
    LockManager lock_manager;
    std::unique_ptr<Log> wal;
    std::unique_ptr<TransactionManager> transaction_manager;
    std::unique_ptr<RaftLog> raft_log;
    std::unique_ptr<RaftHardStateStore> hard_state;
    std::unique_ptr<RaftState> state;
};

TEST_F(RaftApplierTest, AppliesCommittedEntriesAndAdvancesWatermark) {
    append_and_commit({
        {put_op(1, "one")},
        {put_op(2, "two"), put_op(3, "three")},
    });

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    EXPECT_EQ(applier.apply_pending_batch(), 2u);

    EXPECT_EQ(last_applied(), 2u);
    expect_value(1, "one");
    expect_value(2, "two");
    expect_value(3, "three");
}

TEST_F(RaftApplierTest, AppliesNothingWhenCaughtUp) {
    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);

    EXPECT_EQ(applier.apply_pending_batch(), 0u);
    EXPECT_EQ(last_applied(), 0u);
}

TEST_F(RaftApplierTest, DeleteInALaterEntryRemovesAnEarlierPut) {
    append_and_commit({
        {put_op(1, "one")},
        {delete_op(1)},
    });

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    EXPECT_EQ(applier.apply_pending_batch(), 2u);

    EXPECT_EQ(last_applied(), 2u);
    expect_missing(1);
}

TEST_F(RaftApplierTest, DeleteOfAnAbsentKeyStillApplies) {
    append_and_commit({{delete_op(42)}});

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    EXPECT_EQ(applier.apply_pending_batch(), 1u);

    EXPECT_EQ(last_applied(), 1u);
    expect_missing(42);
}

TEST_F(RaftApplierTest, BacklogIsAppliedAcrossSeveralBatches) {
    append_and_commit({
        {put_op(1, "one")},
        {put_op(2, "two")},
        {put_op(3, "three")},
        {put_op(4, "four")},
        {put_op(5, "five")},
    });

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal, 2);
    EXPECT_EQ(applier.apply_pending_batch(), 2u);
    EXPECT_EQ(last_applied(), 2u);
    EXPECT_EQ(applier.apply_pending_batch(), 2u);
    EXPECT_EQ(last_applied(), 4u);
    EXPECT_EQ(applier.apply_pending_batch(), 1u);
    EXPECT_EQ(last_applied(), 5u);
    EXPECT_EQ(applier.apply_pending_batch(), 0u);
    EXPECT_EQ(last_applied(), 5u);
}

TEST_F(RaftApplierTest, AppliesEnoughDistinctKeysToSplitLeaves) {
    // Enough keys to split leaves through the apply path. Values stay one byte:
    // splits are triggered by key count, not bytes, so larger cells overflow a
    // page before it splits (no overflow pages yet).
    constexpr std::uint64_t key_count = 1000;
    std::vector<std::vector<MutationOp>> entries;
    for (std::uint64_t id = 0; id < key_count; ++id) {
        entries.push_back({put_op(id, std::string(1, static_cast<char>('a' + id % 26)))});
    }
    append_and_commit(entries);

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    std::size_t applied = 0;
    while (std::size_t batch = applier.apply_pending_batch()) applied += batch;

    EXPECT_EQ(applied, key_count);
    EXPECT_EQ(last_applied(), key_count);
    for (std::uint64_t id = 0; id < key_count; ++id) {
        expect_value(id, std::string(1, static_cast<char>('a' + id % 26)));
    }
}

TEST_F(RaftApplierTest, ApplyingDoesNotSyncTheWal) {
    // The Raft log, durable on a majority, is the durability point for an
    // applied entry. The WAL tail may stay unsynced: if a crash loses it,
    // recovery reports a lower watermark and the entries are applied again.
    append_and_commit({{put_op(1, "a")}, {put_op(2, "b")}});
    const Lsn durable_before = wal->durable_lsn();

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    EXPECT_EQ(applier.apply_pending_batch(), 2u);

    EXPECT_EQ(last_applied(), 2u);
    EXPECT_EQ(wal->durable_lsn(), durable_before);
    EXPECT_GT(wal->next_lsn() - 1, durable_before);
}

TEST_F(RaftApplierTest, AppliesAKeyAnotherTransactionHoldsExclusively) {
    // The leader's proposing session still holds X on this key while the entry
    // applies. With Locking::Acquire the applier would block on it forever.
    TransactionHandle holder = transaction_manager->begin();
    ASSERT_EQ(store.put(holder, key_for(1), value_for("from-session")), KeyStoreStatus::Success);

    append_and_commit({{put_op(1, "from-raft")}});

    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    EXPECT_EQ(applier.apply_pending_batch(), 1u);
    EXPECT_EQ(last_applied(), 1u);

    ASSERT_EQ(transaction_manager->commit(holder), CommitStatus::Success);
    expect_value(1, "from-raft");
}

TEST_F(RaftApplierTest, RunAppliesOnNotifyAndStopsOnShutdown) {
    RaftApplier applier(*state, *raft_log, store, *transaction_manager, *wal);
    std::thread worker([&applier] { applier.run(); });

    append_and_commit({{put_op(1, "one")}, {put_op(2, "two")}});
    state->apply_cv.notify_one();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (last_applied() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(last_applied(), 2u);

    {
        std::lock_guard lock(state->state_mutex);
        state->shutting_down = true;
    }
    state->apply_cv.notify_all();
    worker.join();

    expect_value(1, "one");
    expect_value(2, "two");
}

} // namespace
