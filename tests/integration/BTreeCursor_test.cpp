#include <gtest/gtest.h>

#include <BTreeCursor.h>
#include <KeyCodec.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/PendingBTreeAction.h>
#include <TransactionManager/TransactionManager.h>
#include <ValueCodec.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

class NoopUndoExecutor final : public TransactionUndoExecutor {
public:
    void undo(
        Transaction&,
        const UndoDescriptor&,
        CompensationAppender) override {
    }
};

Config wal_config() {
    return {
        .max_index_bytes = 1000 * Index::ENTRY_SIZE,
        .max_store_bytes = 16 * 1024 * 1024,
        .initial_lsn = 1,
    };
}

class BTreeCursorIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto unique_suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() /
            ("stoneleafdb_cursor_test_" + unique_suffix);
        db_path = temp_dir / "test.db";
        std::filesystem::create_directories(temp_dir);

        log = std::make_unique<Log>(wal_config());
        log->open((temp_dir / "wal").string());
        lock_manager = std::make_unique<LockManager>();
        undo_executor = std::make_unique<NoopUndoExecutor>();
        transaction_manager = std::make_unique<TransactionManager>(
            *log,
            *lock_manager,
            *undo_executor);
        tree = std::make_unique<BTree>();
        tree->attach_transaction_manager(*transaction_manager);
        ASSERT_EQ(tree->open(db_path.string()), BTreeStatus::Success);
    }

    void TearDown() override {
        if (tree) tree->close();
        tree.reset();
        transaction_manager.reset();
        undo_executor.reset();
        lock_manager.reset();
        log.reset();

        std::error_code ec;
        std::filesystem::remove_all(temp_dir, ec);
    }

    BTreeStatus insert(
        const TransactionHandle &transaction,
        const Key &key,
        Value value
    ) {
        PendingBTreeAction action(transaction->id(), transaction_manager->prepare_to_log(transaction));
        action.set_undo(InsertUndo{key});
        return tree->insert(transaction, key, value, action);
    }

    BTreeRemoveStatus remove(
        const TransactionHandle &transaction,
        const Key &key
    ) {
        BTreeGetStatus previous = tree->get(key);
        if (previous.status != BTreeStatus::Success) {
            return BTreeRemoveStatus{.status = previous.status};
        }
        PendingBTreeAction action(transaction->id(), transaction_manager->prepare_to_log(transaction));
        action.set_undo(DeleteUndo{key, previous.value});
        return tree->remove(transaction, key, action);
    }

    void insert_uint_keys(
        std::uint64_t begin,
        std::uint64_t end,
        std::uint64_t step = 1
    ) {
        TransactionHandle transaction = transaction_manager->begin();
        for (std::uint64_t key = begin; key < end; key += step) {
            ASSERT_EQ(
                insert(
                    transaction,
                    KeyCodec::make_uint64(key),
                    ValueCodec::make_varuint(key)),
                BTreeStatus::Success);
        }
        ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
    }

    void expect_current(BTreeCursor &cursor, const Key &key, std::uint64_t value) {
        BTreeCursorResult result = cursor.current();
        ASSERT_EQ(result.status, BTreeCursorStatus::Success);
        EXPECT_TRUE(KeyCodec::equal(result.key, key));

        std::uint64_t decoded_value = 0;
        ASSERT_TRUE(ValueCodec::decode_varuint(result.value, &decoded_value));
        EXPECT_EQ(decoded_value, value);
    }

    std::filesystem::path temp_dir;
    std::filesystem::path db_path;
    std::unique_ptr<Log> log;
    std::unique_ptr<LockManager> lock_manager;
    std::unique_ptr<NoopUndoExecutor> undo_executor;
    std::unique_ptr<TransactionManager> transaction_manager;
    std::unique_ptr<BTree> tree;
};

TEST_F(BTreeCursorIntegrationTest, EmptyTreeUsesStickyEndState) {
    BTreeCursor cursor = tree->open_cursor();
    EXPECT_FALSE(cursor.valid());
    EXPECT_EQ(cursor.current().status, BTreeCursorStatus::NotPositioned);
    EXPECT_EQ(cursor.next(), BTreeCursorStatus::NotPositioned);

    EXPECT_EQ(cursor.seek_first(), BTreeCursorStatus::EndOfTree);
    EXPECT_FALSE(cursor.valid());
    EXPECT_EQ(cursor.current().status, BTreeCursorStatus::EndOfTree);
    EXPECT_EQ(cursor.next(), BTreeCursorStatus::EndOfTree);
    EXPECT_EQ(cursor.seek(KeyCodec::make_uint64(10)), BTreeCursorStatus::EndOfTree);
}

TEST_F(BTreeCursorIntegrationTest, SeekAndNextNavigateSingleLeaf) {
    insert_uint_keys(10, 40, 10);
    BTreeCursor cursor = tree->open_cursor();
    ASSERT_EQ(cursor.seek_first(), BTreeCursorStatus::Success);
    expect_current(cursor, KeyCodec::make_uint64(10), 10);
    ASSERT_EQ(cursor.next(), BTreeCursorStatus::Success);
    expect_current(cursor, KeyCodec::make_uint64(20), 20);
    ASSERT_EQ(cursor.seek(KeyCodec::make_uint64(25)), BTreeCursorStatus::Success);
    expect_current(cursor, KeyCodec::make_uint64(30), 30);
    ASSERT_EQ(cursor.seek(KeyCodec::make_uint64(5)), BTreeCursorStatus::Success);
    expect_current(cursor, KeyCodec::make_uint64(10), 10);
    EXPECT_EQ(cursor.seek(KeyCodec::make_uint64(40)), BTreeCursorStatus::EndOfTree);
}

TEST_F(BTreeCursorIntegrationTest, SeekCrossesLeafBoundaryForMissingKey) {
    insert_uint_keys(0, 520, 2);
    BTreeCursor cursor = tree->open_cursor();
    ASSERT_EQ(cursor.seek(KeyCodec::make_uint64(199)), BTreeCursorStatus::Success);
    expect_current(cursor, KeyCodec::make_uint64(200), 200);
}

TEST_F(BTreeCursorIntegrationTest, ScanCrossesLeafBoundaryInSortedOrder) {
    insert_uint_keys(0, 260);
    BTreeCursor cursor = tree->open_cursor();
    ASSERT_EQ(cursor.seek_first(), BTreeCursorStatus::Success);
    for (std::uint64_t key = 0; key < 260; key++) {
        expect_current(cursor, KeyCodec::make_uint64(key), key);
        EXPECT_EQ(
            cursor.next(),
            key == 259
                ? BTreeCursorStatus::EndOfTree
                : BTreeCursorStatus::Success);
    }
    EXPECT_FALSE(cursor.valid());
    EXPECT_EQ(cursor.next(), BTreeCursorStatus::EndOfTree);
}

TEST_F(BTreeCursorIntegrationTest, ScanUsesHeterogeneousKeyOrdering) {
    std::vector<Key> keys = {
        KeyCodec::make_string("z"),
        KeyCodec::make_int64(3),
        KeyCodec::make_bool(true),
        KeyCodec::make_bytes({'a'}),
        KeyCodec::make_uint64(10),
        KeyCodec::make_string("a"),
        KeyCodec::make_bool(false),
        KeyCodec::make_int64(-5),
        KeyCodec::make_uint64(1)
    };

    TransactionHandle transaction = transaction_manager->begin();
    for (std::size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(
            insert(transaction, keys[i], ValueCodec::make_varuint(i)),
            BTreeStatus::Success);
    }
    ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);

    std::sort(keys.begin(), keys.end(), [](const Key &lhs, const Key &rhs) {
        return KeyCodec::compare(lhs, rhs) < 0;
    });
    BTreeCursor cursor = tree->open_cursor();
    ASSERT_EQ(cursor.seek_first(), BTreeCursorStatus::Success);
    for (std::size_t i = 0; i < keys.size(); i++) {
        EXPECT_TRUE(KeyCodec::equal(cursor.current().key, keys[i]));
        EXPECT_EQ(
            cursor.next(),
            i + 1 == keys.size()
                ? BTreeCursorStatus::EndOfTree
                : BTreeCursorStatus::Success);
    }
}

TEST_F(BTreeCursorIntegrationTest, CurrentReturnsIndependentCopies) {
    insert_uint_keys(7, 8);
    BTreeCursor cursor = tree->open_cursor();
    ASSERT_EQ(cursor.seek_first(), BTreeCursorStatus::Success);
    BTreeCursorResult copy = cursor.current();
    copy.key.data.clear();
    copy.value.data.clear();
    expect_current(cursor, KeyCodec::make_uint64(7), 7);
}

TEST_F(BTreeCursorIntegrationTest, ActiveCursorBlocksMutationsAndClose) {
    insert_uint_keys(10, 11);
    BTreeCursor cursor = tree->open_cursor();
    TransactionHandle transaction = transaction_manager->begin();
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(20), ValueCodec::make_varuint(20)),
        BTreeStatus::CursorActive);
    EXPECT_EQ(
        remove(transaction, KeyCodec::make_uint64(10)).status,
        BTreeStatus::CursorActive);
    EXPECT_EQ(tree->close(), BTreeStatus::CursorActive);

    ASSERT_EQ(cursor.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(20), ValueCodec::make_varuint(20)),
        BTreeStatus::Success);
    EXPECT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
}

TEST_F(BTreeCursorIntegrationTest, CloseIsIdempotentAndClosedCursorRejectsNavigation) {
    BTreeCursor cursor = tree->open_cursor();
    EXPECT_EQ(cursor.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(cursor.close(), BTreeCursorStatus::Success);
    EXPECT_FALSE(cursor.valid());
    EXPECT_EQ(cursor.seek_first(), BTreeCursorStatus::Closed);
    EXPECT_EQ(cursor.next(), BTreeCursorStatus::Closed);
    EXPECT_EQ(cursor.current().status, BTreeCursorStatus::Closed);
}

TEST_F(BTreeCursorIntegrationTest, AllCursorsMustCloseBeforeWritesResume) {
    BTreeCursor first = tree->open_cursor();
    BTreeCursor second = tree->open_cursor();
    TransactionHandle transaction = transaction_manager->begin();
    ASSERT_EQ(first.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(1), ValueCodec::make_varuint(1)),
        BTreeStatus::CursorActive);
    ASSERT_EQ(second.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(1), ValueCodec::make_varuint(1)),
        BTreeStatus::Success);
    EXPECT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
}

TEST_F(BTreeCursorIntegrationTest, DestructorUnregistersCursor) {
    {
        BTreeCursor cursor = tree->open_cursor();
        EXPECT_EQ(tree->close(), BTreeStatus::CursorActive);
    }
    EXPECT_EQ(tree->close(), BTreeStatus::Success);
}

TEST_F(BTreeCursorIntegrationTest, MoveConstructionTransfersRegistrationAndPosition) {
    insert_uint_keys(5, 6);
    BTreeCursor source = tree->open_cursor();
    ASSERT_EQ(source.seek_first(), BTreeCursorStatus::Success);
    BTreeCursor moved(std::move(source));
    EXPECT_EQ(source.current().status, BTreeCursorStatus::Closed);
    expect_current(moved, KeyCodec::make_uint64(5), 5);

    TransactionHandle transaction = transaction_manager->begin();
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(6), ValueCodec::make_varuint(6)),
        BTreeStatus::CursorActive);
    ASSERT_EQ(moved.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(
        insert(transaction, KeyCodec::make_uint64(6), ValueCodec::make_varuint(6)),
        BTreeStatus::Success);
    EXPECT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
}

TEST_F(BTreeCursorIntegrationTest, MoveAssignmentTransfersRegistrationAcrossTrees) {
    std::filesystem::path second_db_path = temp_dir / "second.db";
    BTree second_tree;
    second_tree.attach_transaction_manager(*transaction_manager);
    ASSERT_EQ(second_tree.open(second_db_path.string()), BTreeStatus::Success);

    BTreeCursor source = tree->open_cursor();
    BTreeCursor destination = second_tree.open_cursor();
    destination = std::move(source);
    EXPECT_EQ(source.current().status, BTreeCursorStatus::Closed);
    EXPECT_EQ(second_tree.close(), BTreeStatus::Success);
    EXPECT_EQ(tree->close(), BTreeStatus::CursorActive);
    EXPECT_EQ(destination.close(), BTreeCursorStatus::Success);
    EXPECT_EQ(tree->close(), BTreeStatus::Success);
}

} // namespace
