#pragma once

#include <Log/WalPayload.h>
#include <TransactionManager/Transaction.h>
#include <TransactionManager/WaitForGraph.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <unordered_map>
#include <vector>

class LockManager;
class Log;

using TransactionHandle = std::shared_ptr<Transaction>;

enum class CommitStatus : std::uint8_t {
    Success = 0,
    TransactionNotFound,
    InvalidState,
};

// Whether commit() makes its commit record durable before returning. Sync is
// the only correct choice for client transactions: commit() releases the
// transaction's locks, so with Defer those locks would be released while the
// commit is still not durable. Defer exists for the Raft apply loop, which
// commits one transaction per entry and syncs once for the whole batch.
enum class Durability : std::uint8_t {
    Sync = 0,
    Defer,
};

enum class AbortStatus : std::uint8_t {
    Success = 0,
    TransactionNotFound,
    InvalidState,
};

enum class WaitRegistrationStatus : std::uint8_t {
    Registered = 0,
    Deadlock,
    TransactionNotFound,
    BlockerNotFound,
};

// Executes one logical inverse against the current B+ tree and returns every
// physical page effect produced by that inverse. TransactionManager uses the
// returned effects to append the corresponding compensation record.
class TransactionUndoExecutor {
public:
    using CompensationAppender =
        std::function<Lsn(std::vector<PageEffect>)>;

    virtual ~TransactionUndoExecutor() = default;

    virtual void undo(
        Transaction& transaction,
        const UndoDescriptor& undo,
        CompensationAppender append_compensation) = 0;
};

class TransactionManager {
public:
    TransactionManager(Log& log, LockManager& lock_manager, TransactionUndoExecutor& undo_executor);

    ~TransactionManager() noexcept;

    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;
    TransactionManager(TransactionManager&&) = delete;
    TransactionManager& operator=(TransactionManager&&) = delete;

    // Creates an active transaction, appends its begin record, and returns a
    // shared handle owned by both the manager and the calling session.
    TransactionHandle begin();

    // Returns an empty handle when the transaction is not active.
    TransactionHandle find(TransactionId txn_id) const;

    // True while any transaction this manager owns has not yet committed or
    // aborted. Storage owners use this to refuse tearing down shared state
    // out from under a transaction that may still reference it.
    bool has_active_transactions() const noexcept;

    // Writes the transaction's deferred TXN_BEGIN if it has none yet, and
    // returns the LSN its next action must chain to. begin() logs nothing, so
    // a caller about to build a PendingBTreeAction calls this first.
    Lsn prepare_to_log(const TransactionHandle& transaction);

    // Appends one completed B-tree action and advances the transaction's WAL
    // chain only after Log assigns the record an LSN.
    Lsn append_action(const TransactionHandle& transaction, PendingWalRecord action);

    // Commit and abort own the complete lifecycle boundary. I/O and WAL
    // corruption failures continue to use the logger's exception convention.
    // raft_index is the Raft entry this transaction applied, recorded in its
    // commit record so recovery can rebuild last_applied. Only the Raft apply
    // loop passes one; client transactions apply no entry and leave it 0.
    CommitStatus commit(const TransactionHandle& transaction,
                        Durability durability = Durability::Sync,
                        std::uint64_t raft_index = 0);
    AbortStatus abort(const TransactionHandle& transaction, AbortReason reason);

    // Registers one complete blocker set through WaitForGraph::add_edges().
    WaitRegistrationStatus register_wait(TransactionId waiting_txn, std::span<const TransactionId> blockers);

    // Removes all outgoing dependencies for a granted or cancelled request.
    void remove_wait(TransactionId waiting_txn);

    // Records a lock after LockManager installs ownership. Returns false when
    // the transaction is no longer active.
    bool record_lock(
        TransactionId txn_id,
        const Key& key,
        LockMode mode);

    Log& log() noexcept { return log_; }
    LockManager& lock_manager() noexcept { return lock_manager_; }

private:
    Log& log_;
    LockManager& lock_manager_;
    TransactionUndoExecutor& undo_executor_;

    mutable std::shared_mutex transactions_mutex_;
    std::unordered_map<TransactionId, TransactionHandle> active_transactions_;
    TransactionId next_transaction_id_ = 1;
    bool transaction_ids_initialized_ = false;

    WaitForGraph wait_for_graph_;

    // The caller must hold transactions_mutex_.
    bool owns_handle_locked(const TransactionHandle& transaction) const;

    void release_locks(Transaction& transaction);
    void initialize_transaction_ids();
    void remove_transaction(TransactionId txn_id);
};
