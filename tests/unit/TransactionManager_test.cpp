#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/PendingBTreeAction.h>
#include <Log/WalRecords.h>
#include <TransactionManager/TransactionManager.h>
#include <V2PageCodec.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

class TempDir {
public:
    TempDir() {
        path = std::filesystem::temp_directory_path() /
            std::filesystem::path("stoneleaf-transaction-manager-XXXXXX");
        std::string value = path.string();
        value.push_back('\0');
        path = ::mkdtemp(value.data());
    }

    ~TempDir() { std::filesystem::remove_all(path); }

    std::filesystem::path path;
};

Config config() {
    return {
        .max_index_bytes = 100 * Index::ENTRY_SIZE,
        .max_store_bytes = 1024 * 1024,
        .initial_lsn = 1,
    };
}

PageEffect effect(std::uint32_t page_num, char marker) {
    PageEffect result{
        .kind = PageEffectKind::Write,
        .page_num = page_num,
    };
    V2PageCodec::initialize(
        result.after_image,
        page_num,
        V2PageKind::BTreeLeaf);
    result.after_image[V2_PAGE_HEADER_SIZE] = marker;
    return result;
}

class RecordingUndoExecutor final : public TransactionUndoExecutor {
public:
    void undo(
        Transaction& transaction,
        const UndoDescriptor& undo,
        CompensationAppender append_compensation) override {
        transaction_ids.push_back(transaction.id());
        undos.push_back(undo);
        compensation_lsns.push_back(append_compensation({effect(9, 'u')}));
    }

    std::vector<TransactionId> transaction_ids;
    std::vector<UndoDescriptor> undos;
    std::vector<Lsn> compensation_lsns;
};

struct ManagerFixture {
    ManagerFixture()
        : log(config()),
          manager(log, lock_manager, undo_executor) {
        log.open(directory.path.string());
    }

    TempDir directory;
    Log log;
    LockManager lock_manager;
    RecordingUndoExecutor undo_executor;
    TransactionManager manager;
};

TEST(TransactionManagerTest, BeginCreatesOwnedTransactionAndWalRecord) {
    ManagerFixture fixture;

    const TransactionHandle transaction = fixture.manager.begin();

    EXPECT_EQ(transaction->id(), 1u);
    EXPECT_EQ(transaction->state(), TransactionState::Active);
    EXPECT_EQ(transaction->last_lsn(), 1u);
    EXPECT_EQ(fixture.manager.find(transaction->id()), transaction);

    const WalRecord begin = fixture.log.read(transaction->last_lsn());
    EXPECT_EQ(begin.type, WalRecordType::TxnBegin);
    EXPECT_EQ(begin.transaction_id, transaction->id());
    EXPECT_EQ(begin.prev_lsn, 0u);

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);
}

TEST(TransactionManagerTest, CommitMakesDecisionDurableAndRemovesTransaction) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();
    PendingBTreeAction action(transaction->id(), transaction->last_lsn());
    action.set_undo(InsertUndo{KeyCodec::make_string("key")});
    action.add_effect(effect(7, 'a'));
    const Lsn action_lsn = fixture.manager.append_action(transaction, action.build());

    EXPECT_EQ(fixture.manager.commit(transaction), CommitStatus::Success);

    EXPECT_EQ(transaction->state(), TransactionState::Committed);
    EXPECT_FALSE(fixture.manager.find(transaction->id()));
    EXPECT_GE(fixture.log.durable_lsn(), transaction->last_lsn());

    const WalRecord commit = fixture.log.read(transaction->last_lsn());
    EXPECT_EQ(commit.type, WalRecordType::TxnCommit);
    EXPECT_EQ(commit.prev_lsn, action_lsn);
    EXPECT_EQ(
        fixture.manager.commit(transaction),
        CommitStatus::TransactionNotFound);
}

TEST(TransactionManagerTest, AbortWritesDecisionAndEnd) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::StatementFailure),
        AbortStatus::Success);

    EXPECT_EQ(transaction->state(), TransactionState::Aborted);
    EXPECT_FALSE(fixture.manager.find(transaction->id()));

    const std::vector<WalRecord> records = fixture.log.scan();
    ASSERT_EQ(records.size(), 3u);
    EXPECT_EQ(records[0].type, WalRecordType::TxnBegin);
    EXPECT_EQ(records[1].type, WalRecordType::TxnAbort);
    EXPECT_EQ(records[2].type, WalRecordType::TxnEnd);
    EXPECT_EQ(records[2].prev_lsn, records[1].lsn);
    EXPECT_EQ(
        std::get<AbortPayload>(WalRecords::decode(records[1])).reason,
        AbortReason::StatementFailure);
}

// A transaction that logged no action has nothing to redo or undo, so its
// decision need not be durable: after a crash recovery ends it and nothing
// else. Skipping the sync is what keeps a read off the disk.
TEST(TransactionManagerTest, CommitOfATransactionThatWroteNothingDoesNotSync) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    EXPECT_EQ(fixture.manager.commit(transaction), CommitStatus::Success);

    EXPECT_EQ(transaction->state(), TransactionState::Committed);
    EXPECT_LT(fixture.log.durable_lsn(), transaction->last_lsn());
}

TEST(TransactionManagerTest, AbortOfATransactionThatWroteNothingDoesNotSync) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);

    EXPECT_EQ(transaction->state(), TransactionState::Aborted);
    EXPECT_LT(fixture.log.durable_lsn(), transaction->last_lsn());
}

// A Raft index in the commit record is state recovery depends on - it is the
// applied watermark - so it syncs even with no action.
TEST(TransactionManagerTest, CommitCarryingARaftIndexSyncsEvenWithoutActions) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    EXPECT_EQ(fixture.manager.commit(transaction, Durability::Sync, 7), CommitStatus::Success);

    EXPECT_GE(fixture.log.durable_lsn(), transaction->last_lsn());
}

TEST(TransactionManagerTest, AbortAfterAnActionMakesTheEndDurable) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();
    PendingBTreeAction action(transaction->id(), transaction->last_lsn());
    action.set_undo(InsertUndo{KeyCodec::make_string("key")});
    action.add_effect(effect(7, 'a'));
    fixture.manager.append_action(transaction, action.build());

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);

    EXPECT_GE(fixture.log.durable_lsn(), transaction->last_lsn());
}

TEST(TransactionManagerTest, AbortUndoesActionsAndChainsCompensationRecord) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    PendingBTreeAction action(transaction->id(), transaction->last_lsn());
    action.set_undo(InsertUndo{KeyCodec::make_string("key")});
    action.add_effect(effect(7, 'a'));
    const Lsn action_lsn = fixture.manager.append_action(transaction, action.build());

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);

    ASSERT_EQ(fixture.undo_executor.transaction_ids.size(), 1u);
    EXPECT_EQ(fixture.undo_executor.transaction_ids[0], transaction->id());
    EXPECT_TRUE(std::holds_alternative<InsertUndo>(fixture.undo_executor.undos[0]));
    ASSERT_EQ(fixture.undo_executor.compensation_lsns.size(), 1u);

    const std::vector<WalRecord> records = fixture.log.scan();
    ASSERT_EQ(records.size(), 5u);
    EXPECT_EQ(records[1].lsn, action_lsn);
    EXPECT_EQ(records[2].type, WalRecordType::TxnAbort);
    EXPECT_EQ(records[3].type, WalRecordType::Compensation);
    EXPECT_EQ(records[4].type, WalRecordType::TxnEnd);

    const auto compensation =
        std::get<CompensationPayload>(WalRecords::decode(records[3]));
    EXPECT_EQ(compensation.undo_of_lsn, action_lsn);
    EXPECT_EQ(compensation.undo_next_lsn, records[0].lsn);
    EXPECT_EQ(records[3].prev_lsn, records[2].lsn);
    EXPECT_EQ(records[4].prev_lsn, records[3].lsn);
}

TEST(TransactionManagerTest, AppendActionValidatesTransactionWalChain) {
    ManagerFixture fixture;
    const TransactionHandle transaction = fixture.manager.begin();

    PendingBTreeAction action(transaction->id(), transaction->last_lsn());
    action.set_undo(InsertUndo{KeyCodec::make_string("key")});
    action.add_effect(effect(7, 'a'));
    PendingWalRecord pending = action.build();
    pending.prev_lsn += 1;

    EXPECT_THROW(
        fixture.manager.append_action(transaction, std::move(pending)),
        std::invalid_argument);
    EXPECT_EQ(transaction->last_lsn(), 1u);
    EXPECT_EQ(fixture.log.next_lsn(), 2u);

    EXPECT_EQ(
        fixture.manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);
}

TEST(TransactionManagerTest, WaitRegistrationDetectsCyclesAndMissingTransactions) {
    ManagerFixture fixture;
    const TransactionHandle first = fixture.manager.begin();
    const TransactionHandle second = fixture.manager.begin();

    const std::vector<TransactionId> second_blocker = {second->id()};
    EXPECT_EQ(
        fixture.manager.register_wait(first->id(), second_blocker),
        WaitRegistrationStatus::Registered);

    const std::vector<TransactionId> first_blocker = {first->id()};
    EXPECT_EQ(
        fixture.manager.register_wait(second->id(), first_blocker),
        WaitRegistrationStatus::Deadlock);

    const std::vector<TransactionId> missing_blocker = {99};
    EXPECT_EQ(
        fixture.manager.register_wait(first->id(), missing_blocker),
        WaitRegistrationStatus::BlockerNotFound);
    EXPECT_EQ(
        fixture.manager.register_wait(99, first_blocker),
        WaitRegistrationStatus::TransactionNotFound);

    fixture.manager.remove_wait(first->id());
    EXPECT_EQ(
        fixture.manager.register_wait(second->id(), first_blocker),
        WaitRegistrationStatus::Registered);

    EXPECT_EQ(
        fixture.manager.abort(second, AbortReason::ClientRequest),
        AbortStatus::Success);
    EXPECT_EQ(
        fixture.manager.abort(first, AbortReason::ClientRequest),
        AbortStatus::Success);
}

TEST(TransactionManagerTest, RejectsHandleOwnedByAnotherManager) {
    ManagerFixture first;
    ManagerFixture second;
    const TransactionHandle foreign = first.manager.begin();

    EXPECT_EQ(
        second.manager.commit(foreign),
        CommitStatus::TransactionNotFound);
    EXPECT_EQ(
        first.manager.abort(foreign, AbortReason::ClientRequest),
        AbortStatus::Success);
}

TEST(TransactionManagerTest, ReopenContinuesAfterGreatestTransactionId) {
    TempDir directory;
    LockManager first_lock_manager;
    RecordingUndoExecutor first_undo_executor;

    {
        Log log(config());
        log.open(directory.path.string());
        TransactionManager manager(log, first_lock_manager, first_undo_executor);
        const TransactionHandle transaction = manager.begin();
        EXPECT_EQ(transaction->id(), 1u);
        EXPECT_EQ(manager.commit(transaction), CommitStatus::Success);
        log.close();
    }

    LockManager second_lock_manager;
    RecordingUndoExecutor second_undo_executor;
    Log reopened(config());
    reopened.open(directory.path.string());
    TransactionManager manager(reopened, second_lock_manager, second_undo_executor);

    const TransactionHandle transaction = manager.begin();
    EXPECT_EQ(transaction->id(), 2u);
    EXPECT_EQ(
        manager.abort(transaction, AbortReason::ClientRequest),
        AbortStatus::Success);
}

TEST(TransactionManagerTest, CommitReleasesRecordedLogicalLocks) {
    ManagerFixture fixture;
    const Key key = KeyCodec::make_string("key");
    const TransactionHandle reader = fixture.manager.begin();

    EXPECT_EQ(
        fixture.lock_manager.lock_shared(reader->id(), key),
        LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(reader), CommitStatus::Success);

    const TransactionHandle writer = fixture.manager.begin();
    EXPECT_EQ(
        fixture.lock_manager.lock_exclusive(writer->id(), key),
        LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(writer), CommitStatus::Success);
}

TEST(TransactionManagerTest, GrantRemovesWaitEdgesAndRecordsDelayedLock) {
    ManagerFixture fixture;
    const Key key = KeyCodec::make_string("key");
    const TransactionHandle writer = fixture.manager.begin();
    const TransactionHandle reader = fixture.manager.begin();

    ASSERT_EQ(
        fixture.lock_manager.lock_exclusive(writer->id(), key),
        LockManagerStatus::Success);

    std::promise<void> reader_started;
    std::future<void> started = reader_started.get_future();
    std::future<LockManagerStatus> waiting_reader = std::async(
        std::launch::async,
        [&fixture, &key, reader_id = reader->id(),
         signal = std::move(reader_started)]() mutable {
            signal.set_value();
            return fixture.lock_manager.lock_shared(reader_id, key);
        });

    started.wait();
    EXPECT_EQ(waiting_reader.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(fixture.manager.commit(writer), CommitStatus::Success);

    ASSERT_EQ(waiting_reader.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(waiting_reader.get(), LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(reader), CommitStatus::Success);

    const TransactionHandle next_writer = fixture.manager.begin();
    EXPECT_EQ(
        fixture.lock_manager.lock_exclusive(next_writer->id(), key),
        LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(next_writer), CommitStatus::Success);
}

TEST(TransactionManagerTest, SharedRequestReportsCrossKeyDeadlock) {
    ManagerFixture fixture;
    const Key first_key = KeyCodec::make_string("first");
    const Key second_key = KeyCodec::make_string("second");
    const TransactionHandle first = fixture.manager.begin();
    const TransactionHandle second = fixture.manager.begin();

    ASSERT_EQ(
        fixture.lock_manager.lock_exclusive(first->id(), first_key),
        LockManagerStatus::Success);
    ASSERT_EQ(
        fixture.lock_manager.lock_exclusive(second->id(), second_key),
        LockManagerStatus::Success);

    std::promise<void> first_started;
    std::future<void> started = first_started.get_future();
    std::future<LockManagerStatus> first_wait = std::async(
        std::launch::async,
        [&fixture, &second_key, first_id = first->id(),
         signal = std::move(first_started)]() mutable {
            signal.set_value();
            return fixture.lock_manager.lock_shared(first_id, second_key);
        });

    started.wait();
    EXPECT_EQ(first_wait.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(
        fixture.lock_manager.lock_shared(second->id(), first_key),
        LockManagerStatus::Deadlock);

    EXPECT_EQ(
        fixture.manager.abort(second, AbortReason::DeadlockVictim),
        AbortStatus::Success);
    ASSERT_EQ(first_wait.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(first_wait.get(), LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(first), CommitStatus::Success);
}

TEST(TransactionManagerTest, SharedOwnerPromotesAfterOtherReaderFinishes) {
    ManagerFixture fixture;
    const Key key = KeyCodec::make_string("key");
    const TransactionHandle promoter = fixture.manager.begin();
    const TransactionHandle reader = fixture.manager.begin();

    ASSERT_EQ(
        fixture.lock_manager.lock_shared(promoter->id(), key),
        LockManagerStatus::Success);
    ASSERT_EQ(
        fixture.lock_manager.lock_shared(reader->id(), key),
        LockManagerStatus::Success);

    std::promise<void> promotion_started;
    std::future<void> started = promotion_started.get_future();
    std::future<LockManagerStatus> promotion = std::async(
        std::launch::async,
        [&fixture, &key, txn_id = promoter->id(),
         signal = std::move(promotion_started)]() mutable {
            signal.set_value();
            return fixture.lock_manager.lock_exclusive(txn_id, key);
        });

    started.wait();
    EXPECT_EQ(promotion.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(fixture.manager.commit(reader), CommitStatus::Success);

    ASSERT_EQ(promotion.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(promotion.get(), LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(promoter), CommitStatus::Success);
}

TEST(TransactionManagerTest, SecondSharedPromotionDetectsDeadlock) {
    ManagerFixture fixture;
    const Key key = KeyCodec::make_string("key");
    const TransactionHandle first = fixture.manager.begin();
    const TransactionHandle second = fixture.manager.begin();

    ASSERT_EQ(
        fixture.lock_manager.lock_shared(first->id(), key),
        LockManagerStatus::Success);
    ASSERT_EQ(
        fixture.lock_manager.lock_shared(second->id(), key),
        LockManagerStatus::Success);

    std::promise<void> first_started;
    std::future<void> started = first_started.get_future();
    std::future<LockManagerStatus> first_promotion = std::async(
        std::launch::async,
        [&fixture, &key, txn_id = first->id(),
         signal = std::move(first_started)]() mutable {
            signal.set_value();
            return fixture.lock_manager.lock_exclusive(txn_id, key);
        });

    started.wait();
    EXPECT_EQ(first_promotion.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(
        fixture.lock_manager.lock_exclusive(second->id(), key),
        LockManagerStatus::Deadlock);

    EXPECT_EQ(
        fixture.manager.abort(second, AbortReason::DeadlockVictim),
        AbortStatus::Success);
    ASSERT_EQ(first_promotion.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(first_promotion.get(), LockManagerStatus::Success);
    EXPECT_EQ(fixture.manager.commit(first), CommitStatus::Success);
}

TEST(TransactionManagerTest, UnknownTransactionDoesNotRetainOwnerOrWaiter) {
    ManagerFixture fixture;
    const Key immediate_key = KeyCodec::make_string("immediate");
    const Key contended_key = KeyCodec::make_string("contended");
    const TransactionHandle owner = fixture.manager.begin();

    EXPECT_EQ(
        fixture.lock_manager.lock_shared(999, immediate_key),
        LockManagerStatus::TransactionNotFound);
    EXPECT_EQ(
        fixture.lock_manager.lock_exclusive(owner->id(), immediate_key),
        LockManagerStatus::Success);

    EXPECT_EQ(
        fixture.lock_manager.lock_exclusive(owner->id(), contended_key),
        LockManagerStatus::Success);
    EXPECT_EQ(
        fixture.lock_manager.lock_shared(999, contended_key),
        LockManagerStatus::TransactionNotFound);

    EXPECT_EQ(fixture.manager.commit(owner), CommitStatus::Success);
}

TEST(TransactionManagerTest, MissingBlockerRemovesNewQueueEntry) {
    TempDir directory;
    Log log(config());
    log.open(directory.path.string());
    LockManager lock_manager;
    RecordingUndoExecutor undo_executor;
    const Key key = KeyCodec::make_string("key");

    // Standalone mode permits direct lock-table testing without transaction
    // tracking, which lets this test construct a stale blocker explicitly.
    ASSERT_EQ(
        lock_manager.lock_exclusive(999, key),
        LockManagerStatus::Success);

    TransactionManager manager(log, lock_manager, undo_executor);
    const TransactionHandle transaction = manager.begin();
    EXPECT_EQ(
        lock_manager.lock_shared(transaction->id(), key),
        LockManagerStatus::TransactionNotFound);

    EXPECT_EQ(
        lock_manager.unlock_exclusive(999, key),
        LockManagerStatus::Success);
    EXPECT_EQ(
        lock_manager.lock_shared(transaction->id(), key),
        LockManagerStatus::Success);
    EXPECT_EQ(manager.commit(transaction), CommitStatus::Success);
}

} // namespace
