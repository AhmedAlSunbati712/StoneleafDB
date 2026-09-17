#include <TransactionManager/TransactionManager.h>

#include <LockManager/KeyHash.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/WalRecords.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

TransactionManager::TransactionManager(
    Log& log,
    LockManager& lock_manager,
    TransactionUndoExecutor& undo_executor)
    : log_(log),
      lock_manager_(lock_manager),
      undo_executor_(undo_executor) {
    lock_manager_.attach_txn_manager(*this);
}

TransactionManager::~TransactionManager() noexcept {
    lock_manager_.detach_txn_manager(*this);
}

TransactionHandle TransactionManager::begin() {
    std::unique_lock lock(transactions_mutex_);
    initialize_transaction_ids();
    if (next_transaction_id_ == 0) {
        throw std::overflow_error("Transaction IDs are exhausted");
    }

    const TransactionId txn_id = next_transaction_id_;
    TransactionHandle transaction = std::make_shared<Transaction>(txn_id);

    // TXN_BEGIN is not written here: append_action() writes it before the
    // transaction's first action. Most transactions never log one - reads, and
    // the replicated path's sessions, which only hold locks - and every WAL
    // append they skipped would otherwise stall behind the WAL's fsync.
    active_transactions_.emplace(txn_id, transaction);
    try {
        wait_for_graph_.add_node(txn_id);
    } catch (...) {
        wait_for_graph_.remove(txn_id);
        active_transactions_.erase(txn_id);
        throw;
    }

    ++next_transaction_id_;
    return transaction;
}

TransactionHandle TransactionManager::find(TransactionId txn_id) const {
    std::shared_lock lock(transactions_mutex_);
    auto transaction = active_transactions_.find(txn_id);
    if (transaction == active_transactions_.end()) return {};
    return transaction->second;
}

bool TransactionManager::has_active_transactions() const noexcept {
    std::shared_lock lock(transactions_mutex_);
    return !active_transactions_.empty();
}

Lsn TransactionManager::prepare_to_log(const TransactionHandle& transaction) {
    std::shared_lock lock(transactions_mutex_);
    if (!owns_handle_locked(transaction)) {
        throw std::invalid_argument("Transaction is not owned by this manager");
    }
    if (transaction->state_ != TransactionState::Active) {
        throw std::logic_error("Transaction is not active");
    }
    // Like append_action, this mutates only the owning thread's transaction.
    if (transaction->last_lsn_ == 0) {
        transaction->last_lsn_ = log_.append(WalRecords::begin(transaction->id_));
    }
    return transaction->last_lsn_;
}

Lsn TransactionManager::append_action(const TransactionHandle& transaction, PendingWalRecord action) {
    std::shared_lock lock(transactions_mutex_);
    if (!owns_handle_locked(transaction)) {
        throw std::invalid_argument("Transaction is not owned by this manager");
    }
    if (transaction->state_ != TransactionState::Active) {
        throw std::logic_error("Transaction is not active");
    }

    // Only completed user actions may enter the transaction chain through
    // this boundary. Lifecycle records and CLRs remain manager-owned.
    if (action.type != WalRecordType::BTreeAction ||
        action.transaction_id != transaction->id_ ||
        action.prev_lsn != transaction->last_lsn_) {
        throw std::invalid_argument("B-tree action does not match transaction WAL state");
    }

    const Lsn action_lsn = log_.append(std::move(action));
    transaction->last_lsn_ = action_lsn;
    transaction->logged_action_ = true;
    return action_lsn;
}

CommitStatus TransactionManager::commit(const TransactionHandle& transaction,
                                        Durability durability,
                                        std::uint64_t raft_index) {
    {
        std::unique_lock lock(transactions_mutex_);
        if (!owns_handle_locked(transaction)) return CommitStatus::TransactionNotFound;
        if (transaction->state_ != TransactionState::Active) return CommitStatus::InvalidState;

        // Claim the lifecycle transition before performing I/O so commit and
        // abort cannot both make a decision for the same transaction.
        transaction->state_ = TransactionState::Committing;
    }

    // A transaction that logged no action changed nothing, so it needs no
    // decision record at all - unless the record carries a Raft index, which
    // recovery reads back as the applied watermark. Every commit that is
    // written is acknowledged only after it is durable.
    // A begin logged ahead of an action that never came (a delete of an absent
    // key) still gets its commit, so recovery does not treat it as a loser.
    const bool decision_matters = transaction->logged_action_ || raft_index != 0;
    if (decision_matters || transaction->last_lsn_ != 0) {
        if (transaction->last_lsn_ == 0) {
            transaction->last_lsn_ = log_.append(WalRecords::begin(transaction->id_));
        }
        const Lsn commit_lsn = log_.append(
            WalRecords::commit(transaction->id_, transaction->last_lsn_, raft_index));
        transaction->last_lsn_ = commit_lsn;
        if (durability == Durability::Sync && decision_matters) log_.sync_through(commit_lsn);
    }

    {
        std::unique_lock lock(transactions_mutex_);
        transaction->state_ = TransactionState::Committed;
    }

    // Strict two-phase locks remain held until the commit decision is durable.
    release_locks(*transaction);
    wait_for_graph_.remove(transaction->id_);
    remove_transaction(transaction->id_);
    return CommitStatus::Success;
}

AbortStatus TransactionManager::abort(const TransactionHandle& transaction, AbortReason reason) {
    if (reason < AbortReason::ClientRequest || reason > AbortReason::InternalError) {
        throw std::invalid_argument("Unknown transaction abort reason");
    }

    Lsn undo_lsn = 0;
    {
        std::unique_lock lock(transactions_mutex_);
        if (!owns_handle_locked(transaction)) return AbortStatus::TransactionNotFound;
        if (transaction->state_ != TransactionState::Active &&
            transaction->state_ != TransactionState::AbortRequested) {
            return AbortStatus::InvalidState;
        }

        // Retain the last action LSN before adding the abort decision because
        // rollback begins with the last record that may require undo.
        transaction->state_ = TransactionState::Aborting;
        undo_lsn = transaction->last_lsn_;
    }

    // Nothing logged means nothing to undo and no chain to close: the WAL
    // never learned this transaction existed.
    if (undo_lsn == 0) {
        {
            std::unique_lock lock(transactions_mutex_);
            transaction->state_ = TransactionState::Aborted;
        }
        release_locks(*transaction);
        wait_for_graph_.remove(transaction->id_);
        remove_transaction(transaction->id_);
        return AbortStatus::Success;
    }

    const Lsn abort_lsn = log_.append(WalRecords::abort(transaction->id_, transaction->last_lsn_, reason));
    transaction->last_lsn_ = abort_lsn;

    // Follow the transaction's WAL chain backward. Every completed logical
    // inverse receives a CLR before rollback advances to the preceding record.
    while (undo_lsn != 0) {
        const WalRecord record = log_.read(undo_lsn);
        if (record.transaction_id != transaction->id_) {
            throw std::runtime_error("Transaction WAL chain crosses transaction IDs");
        }

        if (record.type == WalRecordType::TxnBegin) {
            if (record.prev_lsn != 0) {
                throw std::runtime_error("Transaction begin record has a prevLSN");
            }
            break;
        }

        if (record.type == WalRecordType::Compensation) {
            const auto payload = std::get<CompensationPayload>(WalRecords::decode(record));
            undo_lsn = payload.undo_next_lsn;
            continue;
        }

        if (record.type != WalRecordType::BTreeAction) {
            throw std::runtime_error("Transaction WAL chain contains an invalid undo record");
        }

        const auto payload = std::get<BTreeActionPayload>(WalRecords::decode(record));
        bool compensation_appended = false;

        // The undo executor invokes this while it still owns every page latch
        // protecting the inverse's physical changes. The callback is one-shot
        // so one logical inverse can enter the WAL chain only once.
        auto append_compensation = [&](std::vector<PageEffect> effects) -> Lsn {
            if (compensation_appended) {
                throw std::logic_error("Compensation callback invoked more than once");
            }
            const CompensationPayload compensation{
                .undo_of_lsn = record.lsn,
                .undo_next_lsn = record.prev_lsn,
                .effects = std::move(effects),
            };
            transaction->last_lsn_ = log_.append(WalRecords::compensation(
                transaction->id_,
                transaction->last_lsn_,
                compensation));
            compensation_appended = true;
            return transaction->last_lsn_;
        };

        undo_executor_.undo(
            *transaction,
            payload.undo,
            append_compensation);
        if (!compensation_appended) {
            throw std::runtime_error("Undo completed without appending compensation");
        }
        undo_lsn = record.prev_lsn;
    }

    // TXN_END makes completion of every logical inverse durable before strict
    // two-phase locks are released and the transaction disappears.
    const Lsn end_lsn = log_.append(WalRecords::end(transaction->id_, transaction->last_lsn_));
    transaction->last_lsn_ = end_lsn;
    // A begin with no action had nothing to undo; recovery would end it the
    // same way, so only an end that closes real undo work must be durable.
    if (transaction->logged_action_) log_.sync_through(end_lsn);

    {
        std::unique_lock lock(transactions_mutex_);
        transaction->state_ = TransactionState::Aborted;
    }

    release_locks(*transaction);
    wait_for_graph_.remove(transaction->id_);
    remove_transaction(transaction->id_);
    return AbortStatus::Success;
}

WaitRegistrationStatus TransactionManager::register_wait(TransactionId waiting_txn, std::span<const TransactionId> blockers) {
    std::shared_lock lock(transactions_mutex_);
    auto waiting = active_transactions_.find(waiting_txn);
    if (waiting == active_transactions_.end() || waiting->second->state_ != TransactionState::Active) {
        return WaitRegistrationStatus::TransactionNotFound;
    }

    // Validate every blocker while the active table is stable so a false
    // add_edges result unambiguously means that the request creates a cycle.
    for (TransactionId blocker : blockers) {
        if (active_transactions_.find(blocker) == active_transactions_.end()) {
            return WaitRegistrationStatus::BlockerNotFound;
        }
    }

    if (!wait_for_graph_.add_edges(waiting_txn, blockers)) {
        return WaitRegistrationStatus::Deadlock;
    }
    return WaitRegistrationStatus::Registered;
}

void TransactionManager::remove_wait(TransactionId waiting_txn) {
    wait_for_graph_.remove_outgoing(waiting_txn);
}

bool TransactionManager::record_lock(
    TransactionId txn_id,
    const Key& key,
    LockMode mode) {
    std::unique_lock lock(transactions_mutex_);
    auto transaction = active_transactions_.find(txn_id);
    if (transaction == active_transactions_.end() ||
        transaction->second->state_ != TransactionState::Active) {
        return false;
    }

    auto existing = std::find_if(
        transaction->second->held_key_locks_.begin(),
        transaction->second->held_key_locks_.end(),
        [&](const HeldKeyLock& held) {
            return KeyEqual{}(held.key, key);
        });

    // Keep only the strongest mode held for each logical key.
    if (existing != transaction->second->held_key_locks_.end()) {
        if (mode == LockMode::Exclusive) existing->mode = LockMode::Exclusive;
        return true;
    }

    transaction->second->held_key_locks_.push_back({key, mode});
    return true;
}

bool TransactionManager::owns_handle_locked(const TransactionHandle& transaction) const {
    if (!transaction) return false;
    auto owned = active_transactions_.find(transaction->id_);
    return owned != active_transactions_.end() && owned->second == transaction;
}

void TransactionManager::release_locks(Transaction& transaction) {
    // Release in reverse acquisition order to make cleanup predictable when a
    // transaction touched several independent keys.
    for (auto lock = transaction.held_key_locks_.rbegin();
         lock != transaction.held_key_locks_.rend();
         ++lock) {
        const LockManagerStatus status = lock->mode == LockMode::Shared
            ? lock_manager_.unlock_shared(transaction.id_, lock->key)
            : lock_manager_.unlock_exclusive(transaction.id_, lock->key);
        if (status != LockManagerStatus::Success) {
            throw std::runtime_error("Transaction manager failed to release a key lock");
        }
    }
    transaction.held_key_locks_.clear();
}

void TransactionManager::initialize_transaction_ids() {
    if (transaction_ids_initialized_) return;

    // Transaction IDs remain unique across reopen while the standalone WAL
    // retains complete history and has no checkpoint-owned ID allocator yet.
    TransactionId greatest_transaction_id = 0;
    for (const WalRecord& record : log_.scan()) {
        if (record.transaction_id > greatest_transaction_id) {
            greatest_transaction_id = record.transaction_id;
        }
    }

    next_transaction_id_ = greatest_transaction_id == std::numeric_limits<TransactionId>::max() ? 0 : greatest_transaction_id + 1;
    transaction_ids_initialized_ = true;
}

void TransactionManager::remove_transaction(TransactionId txn_id) {
    std::unique_lock lock(transactions_mutex_);
    active_transactions_.erase(txn_id);
}
