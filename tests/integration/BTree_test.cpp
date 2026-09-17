#include <gtest/gtest.h>

#include <BTree.h>
#include <KeyCodec.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/PendingBTreeAction.h>
#include <Log/WalRecords.h>
#include <TransactionManager/TransactionManager.h>
#include <V2PageCodec.h>
#include <ValueCodec.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

std::string value_to_string(const Value &value) {
    return std::string(value.data.begin(), value.data.end());
}

Key make_key(std::uint64_t key) {
    return KeyCodec::make_uint64(key);
}

Config wal_config() {
    return {
        .max_index_bytes = 1000 * Index::ENTRY_SIZE,
        .max_store_bytes = 16 * 1024 * 1024,
        .initial_lsn = 1,
    };
}

std::array<char, V2_PAGE_SIZE> read_page(
    const std::filesystem::path& path,
    std::uint32_t page_num
) {
    std::array<char, V2_PAGE_SIZE> bytes{};
    std::ifstream input(path, std::ios::binary);
    input.seekg(static_cast<std::streamoff>(page_num) * V2_PAGE_SIZE);
    input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    return bytes;
}

class BTreeUndoExecutor final : public TransactionUndoExecutor {
public:
    BTree *tree = nullptr;

    void undo(
        Transaction &transaction,
        const UndoDescriptor &undo,
        CompensationAppender append_compensation
    ) override {
        if (!tree || !tree->apply_undo(
                transaction,
                undo,
                std::move(append_compensation))) {
            throw std::runtime_error("B-tree undo failed");
        }
    }
};

class BTreeHarness {
public:
    BTreeHarness(
        const std::filesystem::path &db_path,
        const std::filesystem::path &wal_path
    ) : log(wal_config()),
        transaction_manager(log, lock_manager, undo_executor),
        db_path(db_path) {
        log.open(wal_path.string());
        tree.attach_transaction_manager(transaction_manager);
        undo_executor.tree = &tree;
        if (tree.open(db_path.string()) != BTreeStatus::Success) {
            throw std::runtime_error("Failed to open B-tree test harness");
        }
    }

    ~BTreeHarness() {
        tree.close();
    }

    BTreeStatus insert(
        const TransactionHandle &transaction,
        const Key &key,
        Value value
    ) {
        PendingBTreeAction action(transaction->id(), transaction_manager.prepare_to_log(transaction));
        BTreeGetStatus previous = tree.get(key);
        if (previous.status == BTreeStatus::Success) {
            action.set_undo(UpdateUndo{key, previous.value});
        } else {
            action.set_undo(InsertUndo{key});
        }
        return tree.insert(transaction, key, value, action);
    }

    BTreeRemoveStatus remove(
        const TransactionHandle &transaction,
        const Key &key
    ) {
        BTreeGetStatus previous = tree.get(key);
        if (previous.status != BTreeStatus::Success) {
            return BTreeRemoveStatus{.status = previous.status};
        }
        PendingBTreeAction action(transaction->id(), transaction_manager.prepare_to_log(transaction));
        action.set_undo(DeleteUndo{key, previous.value});
        return tree.remove(transaction, key, action);
    }

    void reopen() {
        if (tree.close() != BTreeStatus::Success ||
            tree.open(db_path.string()) != BTreeStatus::Success) {
            throw std::runtime_error("Failed to reopen B-tree test harness");
        }
    }

    Log log;
    LockManager lock_manager;
    BTreeUndoExecutor undo_executor;
    TransactionManager transaction_manager;
    BTree tree;

private:
    std::filesystem::path db_path;
};

class BTreeIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto unique_suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() /
            ("stoneleafdb_btree_test_" + unique_suffix);
        db_path = temp_dir / "test.db";
        std::filesystem::create_directories(temp_dir);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(temp_dir, ec);
    }

    Value make_small_key_value(std::uint64_t key) {
        char payload = static_cast<char>('A' + (key % 26));
        return ValueCodec::make_char(std::string(1, payload));
    }

    void expect_get_value(
        BTree &tree,
        std::uint64_t key,
        const std::string &expected_payload
    ) {
        BTreeGetStatus get_result = tree.get(make_key(key));
        ASSERT_EQ(get_result.status, BTreeStatus::Success);
        EXPECT_EQ(value_to_string(get_result.value), expected_payload);
    }

    void expect_key_missing(BTree &tree, std::uint64_t key) {
        EXPECT_EQ(tree.get(make_key(key)).status, BTreeStatus::KeyNotInTree);
    }

    std::filesystem::path temp_dir;
    std::filesystem::path db_path;
};

TEST_F(BTreeIntegrationTest, GetOnFreshTreeReturnsKeyNotInTree) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    EXPECT_EQ(harness.tree.get(make_key(42)).status, BTreeStatus::KeyNotInTree);
}

TEST_F(BTreeIntegrationTest, InsertCommitAndReopenPreservesSingleKey) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(7), ValueCodec::make_char("A")),
        BTreeStatus::Success);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    harness.reopen();
    expect_get_value(harness.tree, 7, "A");
}

TEST_F(BTreeIntegrationTest, MutationActionCollectsCompleteDeduplicatedEffects) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    Key key = make_key(7);
    Value value = ValueCodec::make_char("A");
    PendingBTreeAction action(transaction->id(), harness.transaction_manager.prepare_to_log(transaction));
    action.set_undo(InsertUndo{key});

    ASSERT_EQ(
        harness.tree.insert(transaction, key, value, action),
        BTreeStatus::Success);
    ASSERT_EQ(action.effects().size(), 2U);

    const PageEffect *header_effect = nullptr;
    const PageEffect *root_effect = nullptr;
    for (const PageEffect& effect : action.effects()) {
        if (effect.page_num == 0) header_effect = &effect;
        if (effect.page_num == 1) root_effect = &effect;
    }
    ASSERT_NE(header_effect, nullptr);
    ASSERT_NE(root_effect, nullptr);
    EXPECT_EQ(header_effect->kind, PageEffectKind::Write);
    EXPECT_EQ(root_effect->kind, PageEffectKind::Allocate);
    EXPECT_EQ(V2PageCodec::page_num(header_effect->after_image), 0U);
    EXPECT_EQ(V2PageCodec::page_num(root_effect->after_image), 1U);
}

TEST_F(BTreeIntegrationTest, TransactionalInsertAppendsWalAndInstallsAssignedLsn) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    Key key = make_key(7);
    Value value = ValueCodec::make_char("A");
    PendingBTreeAction action(transaction->id(), harness.transaction_manager.prepare_to_log(transaction));
    action.set_undo(InsertUndo{key});

    ASSERT_EQ(
        harness.tree.insert(transaction, key, value, action),
        BTreeStatus::Success);
    const Lsn action_lsn = transaction->last_lsn();
    ASSERT_EQ(action_lsn, 2U);
    EXPECT_EQ(harness.log.read(action_lsn).type, WalRecordType::BTreeAction);
    EXPECT_EQ(
        std::get<BTreeActionPayload>(
            WalRecords::decode(harness.log.read(action_lsn))).effects.size(),
        2U);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    harness.reopen();
    const auto header = read_page(db_path, 0);
    const auto root = read_page(db_path, 1);
    EXPECT_EQ(V2PageCodec::page_lsn(header), action_lsn);
    EXPECT_EQ(V2PageCodec::page_lsn(root), action_lsn);
    EXPECT_EQ(V2PageCodec::validate(header), V2PageCodecResult::Success);
    EXPECT_EQ(V2PageCodec::validate(root), V2PageCodecResult::Success);
}

TEST_F(BTreeIntegrationTest, OverwriteCommitAndReopenPreservesUpdatedValue) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(11), ValueCodec::make_char("F")),
        BTreeStatus::Success);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(11), ValueCodec::make_char("T")),
        BTreeStatus::Success);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);
    harness.reopen();
    expect_get_value(harness.tree, 11, "T");
}

TEST_F(BTreeIntegrationTest, AbortDiscardsUncommittedInsert) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(25), ValueCodec::make_char("R")),
        BTreeStatus::Success);
    ASSERT_EQ(
        harness.transaction_manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);
    expect_key_missing(harness.tree, 25);
}

TEST_F(BTreeIntegrationTest, ManyInsertsSplitRootAndRemainReadableAcrossReopen) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    for (std::uint64_t key = 0; key < 260; key++) {
        ASSERT_EQ(
            harness.insert(transaction, make_key(key), make_small_key_value(key)),
            BTreeStatus::Success);
    }
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);
    harness.reopen();

    for (std::uint64_t key = 0; key < 260; key++) {
        expect_get_value(
            harness.tree,
            key,
            std::string(1, static_cast<char>('A' + (key % 26))));
    }
}

TEST_F(BTreeIntegrationTest, RemoveCommitAndReopenDeletesKey) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(10), ValueCodec::make_char("L")),
        BTreeStatus::Success);
    ASSERT_EQ(
        harness.insert(transaction, make_key(20), ValueCodec::make_char("R")),
        BTreeStatus::Success);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    transaction = harness.transaction_manager.begin();
    BTreeRemoveStatus removed = harness.remove(transaction, make_key(10));
    ASSERT_EQ(removed.status, BTreeStatus::Success);
    EXPECT_EQ(value_to_string(removed.value), "L");
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);
    harness.reopen();
    expect_key_missing(harness.tree, 10);
    expect_get_value(harness.tree, 20, "R");
}

TEST_F(BTreeIntegrationTest, AbortDiscardsRemove) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    ASSERT_EQ(
        harness.insert(transaction, make_key(55), ValueCodec::make_char("T")),
        BTreeStatus::Success);
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    transaction = harness.transaction_manager.begin();
    ASSERT_EQ(harness.remove(transaction, make_key(55)).status, BTreeStatus::Success);
    ASSERT_EQ(
        harness.transaction_manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);
    expect_get_value(harness.tree, 55, "T");
}

TEST_F(BTreeIntegrationTest, ManyDeletesRepairTreeAndPreserveRemainingKeys) {
    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    for (std::uint64_t key = 0; key < 260; key++) {
        ASSERT_EQ(
            harness.insert(transaction, make_key(key), make_small_key_value(key)),
            BTreeStatus::Success);
    }
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);

    transaction = harness.transaction_manager.begin();
    for (std::uint64_t key = 0; key < 180; key++) {
        BTreeRemoveStatus removed = harness.remove(transaction, make_key(key));
        ASSERT_EQ(removed.status, BTreeStatus::Success);
        EXPECT_EQ(
            value_to_string(removed.value),
            std::string(1, static_cast<char>('A' + (key % 26))));
    }
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);
    harness.reopen();

    for (std::uint64_t key = 0; key < 180; key++) {
        expect_key_missing(harness.tree, key);
    }
    for (std::uint64_t key = 180; key < 260; key++) {
        expect_get_value(
            harness.tree,
            key,
            std::string(1, static_cast<char>('A' + (key % 26))));
    }
}

TEST_F(BTreeIntegrationTest, HeterogeneousKeysRemainDistinctAcrossCommit) {
    std::vector<Key> keys = {
        KeyCodec::make_bool(true),
        KeyCodec::make_uint64(1),
        KeyCodec::make_int64(1),
        KeyCodec::make_string("1"),
        KeyCodec::make_bytes({'1'})
    };

    BTreeHarness harness(db_path, temp_dir / "wal");
    TransactionHandle transaction = harness.transaction_manager.begin();
    for (std::size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(
            harness.insert(transaction, keys[i], ValueCodec::make_varuint(i)),
            BTreeStatus::Success);
    }
    ASSERT_EQ(harness.transaction_manager.commit(transaction), CommitStatus::Success);
    harness.reopen();

    for (std::size_t i = 0; i < keys.size(); i++) {
        BTreeGetStatus result = harness.tree.get(keys[i]);
        ASSERT_EQ(result.status, BTreeStatus::Success);
        std::uint64_t decoded = 0;
        ASSERT_TRUE(ValueCodec::decode_varuint(result.value, &decoded));
        EXPECT_EQ(decoded, i);
    }
}

} // namespace
