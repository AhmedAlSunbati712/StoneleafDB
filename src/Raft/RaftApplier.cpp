#include <Raft/RaftApplier.h>

#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <string>
#include <variant>

namespace {

std::string describe(std::uint64_t index, const char* what) {
    return "Raft apply failed at index " + std::to_string(index) + ": " + what;
}

} // namespace

RaftApplier::RaftApplier(RaftState& state,
                         RaftLog& raft_log,
                         KeyStore& key_store,
                         TransactionManager& transaction_manager,
                         Log& wal,
                         std::size_t max_apply_batch_size)
    : state_(state),
      raft_log_(raft_log),
      key_store_(key_store),
      transaction_manager_(transaction_manager),
      wal_(wal),
      max_apply_batch_size_(std::max<std::size_t>(max_apply_batch_size, 1)) {}

void RaftApplier::run() {
    while (true) {
        {
            std::unique_lock lock(state_.state_mutex);
            state_.apply_cv.wait(lock, [this] {
                return state_.shutting_down ||
                       state_.commit_index() > state_.last_applied();
            });
            if (state_.shutting_down) return;
        }

        apply_pending_batch();
    }
}

std::size_t RaftApplier::apply_pending_batch() {
    std::uint64_t first_index = 0;
    std::uint64_t last_index = 0;
    {
        // Copy the batch bounds out and release the lock: the B-tree work and
        // the fsync below must not run under state_mutex.
        std::lock_guard lock(state_.state_mutex);
        if (state_.commit_index() <= state_.last_applied()) return 0;

        first_index = state_.last_applied() + 1;
        last_index = std::min(
            state_.commit_index(),
            first_index + max_apply_batch_size_ - 1);
    }

    Lsn batch_commit_lsn = 0;
    for (std::uint64_t index = first_index; index <= last_index; ++index) {
        const RaftMutationEntry entry = raft_log_.read(index);

        // A fresh transaction per entry, even for a no-op entry with no
        // operations. The transaction is the WAL identity, so a crash midway
        // through an entry leaves no TxnCommit record and ARIES undoes the
        // partial entry rather than leaving a state no Raft index describes.
        TransactionHandle transaction = transaction_manager_.begin();
        if (!transaction) throw std::runtime_error(describe(index, "could not begin a transaction"));

        for (const MutationOp& operation : entry.operations) {
            switch (operation.type) {
                case RaftMutationType::Put: {
                    const PutMutation& put = std::get<PutMutation>(operation.operation);
                    const KeyStoreStatus status = key_store_.put(
                        transaction, put.key, put.value, Locking::Skip);
                    if (status != KeyStoreStatus::Success) {
                        transaction_manager_.abort(transaction, AbortReason::InternalError);
                        throw std::runtime_error(describe(index, "put failed"));
                    }
                    break;
                }
                case RaftMutationType::Delete: {
                    const DeleteMutation& remove = std::get<DeleteMutation>(operation.operation);
                    const KeyStoreRemoveResult result = key_store_.remove(
                        transaction, remove.key, Locking::Skip);
                    // A tombstone for a key this node does not hold is not a
                    // failure: the entry still applied, and every replica ends
                    // in the same state.
                    if (result.status != KeyStoreStatus::Success &&
                        result.status != KeyStoreStatus::KeyNotFound) {
                        transaction_manager_.abort(transaction, AbortReason::InternalError);
                        throw std::runtime_error(describe(index, "delete failed"));
                    }
                    break;
                }
            }
        }

        // Defer the sync: one fsync covers the whole batch below.
        if (transaction_manager_.commit(transaction, Durability::Defer) != CommitStatus::Success) {
            throw std::runtime_error(describe(index, "commit failed"));
        }
        batch_commit_lsn = transaction->last_lsn();
    }

    // Durable before the watermark moves: sessions wake on last_applied and
    // immediately report success to their client.
    wal_.sync_through(batch_commit_lsn);

    {
        std::lock_guard lock(state_.state_mutex);
        state_.set_last_applied(last_index);
        state_.applied_cv.notify_all();
    }

    return static_cast<std::size_t>(last_index - first_index + 1);
}
