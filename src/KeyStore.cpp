#include <KeyStore.h>

#include <KeyCodec.h>
#include <LockManager/LockManager.h>
#include <Log/PendingBTreeAction.h>
#include <ValueCodec.h>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <utility>

/**
 * KeyStore is the validated key-value layer above BTree.
 *
 * The BTree deliberately works with encoded Key and Value objects and exposes
 * storage-oriented status values. KeyStore is responsible for the opposite
 * side of that boundary:
 *
 * - validate encoded keys and values before they reach the BTree;
 * - translate BTree and BTreeCursor failures into public KeyStore statuses;
 * - decide whether a successful write is committed immediately or belongs to
 *   an explicit transaction;
 * - keep a failed explicit transaction unusable until rollback; and
 * - attach range and prefix bounds to otherwise unbounded BTree cursors.
 *
 * KeyStore does not add another cache or transaction mechanism. Durability,
 * rollback journaling, page ownership, and cross-process locks remain owned by
 * the BTree/Pager layers below it.
 */

// ================================= KeyStoreCursor =================================

KeyStoreCursor::KeyStoreCursor(
    BTreeCursor cursor,
    std::optional<Key> exclusive_end,
    std::optional<Key> required_prefix
) : cursor(std::move(cursor)),
    exclusive_end(std::move(exclusive_end)),
    required_prefix(std::move(required_prefix)) {
    /**
     * The BTree cursor arrives already positioned by KeyStore::scan*.
     *
     * A seek can succeed while landing directly on the exclusive end key or on
     * the first key after a requested prefix. Record that as a sticky logical
     * end immediately; the underlying cursor remains alive until close so its
     * pinned page and BTree registration still have one clear owner.
     */
    if (!this->cursor.valid()) return;

    BTreeCursorResult result = this->cursor.current();
    if (
        result.status == BTreeCursorStatus::Success &&
        !current_key_is_in_bounds(result.key)
    ) {
        exhausted_by_bound = true;
    }
}

KeyStoreCursorResult KeyStoreCursor::current() const {
    /**
     * Return the current logical entry without moving the scan.
     *
     * Bound exhaustion is checked before decoding so a range never exposes its
     * end key and a prefix scan never exposes the first nonmatching key. Raw
     * cursor failures stay storage errors, while a well-read but malformed
     * persisted key/value pair is reported as DecodeFailed.
     */
    KeyStoreCursorResult result{};
    if (exhausted_by_bound) {
        result.status = KeyStoreCursorStatus::EndOfScan;
        return result;
    }

    BTreeCursorResult storage_result = cursor.current();
    result.status = translate_cursor_status(storage_result.status);
    if (storage_result.status != BTreeCursorStatus::Success) return result;

    if (!current_key_is_in_bounds(storage_result.key)) {
        result.status = KeyStoreCursorStatus::EndOfScan;
        return result;
    }

    if (
        !KeyCodec::validate_key(storage_result.key) ||
        !ValueCodec::validate_value(storage_result.value)
    ) {
        result.status = KeyStoreCursorStatus::DecodeFailed;
        return result;
    }

    result.status = KeyStoreCursorStatus::Success;
    result.entry = KeyStoreEntry{
        std::move(storage_result.key),
        std::move(storage_result.value)
    };
    return result;
}

KeyStoreCursorStatus KeyStoreCursor::next() {
    /**
     * Advance by one physical BTree key, then apply the logical scan bound.
     *
     * Once a range or prefix bound is crossed, EndOfScan is sticky. This mirrors
     * BTreeCursor's sticky EndOfTree behavior and prevents a caller from
     * advancing out of one logical scan into a later key family.
     */
    if (exhausted_by_bound) return KeyStoreCursorStatus::EndOfScan;

    BTreeCursorStatus next_status = cursor.next();
    if (next_status != BTreeCursorStatus::Success) {
        return translate_cursor_status(next_status);
    }

    BTreeCursorResult storage_result = cursor.current();
    if (storage_result.status != BTreeCursorStatus::Success) {
        return translate_cursor_status(storage_result.status);
    }

    if (!current_key_is_in_bounds(storage_result.key)) {
        exhausted_by_bound = true;
        return KeyStoreCursorStatus::EndOfScan;
    }

    return KeyStoreCursorStatus::Success;
}

bool KeyStoreCursor::valid() const {
    // BTree validity alone is insufficient: a positioned physical cursor can
    // already be outside this wrapper's exclusive-end or prefix bound.
    if (exhausted_by_bound || !cursor.valid()) return false;

    BTreeCursorResult result = cursor.current();
    return result.status == BTreeCursorStatus::Success &&
        current_key_is_in_bounds(result.key);
}

KeyStoreCursorStatus KeyStoreCursor::close() {
    // BTreeCursor::close is idempotent and owns the actual page unpin plus BTree
    // unregister operation. Clear the wrapper-only end marker after it succeeds
    // so later current() calls correctly report Closed instead of EndOfScan.
    BTreeCursorStatus close_status = cursor.close();
    if (close_status == BTreeCursorStatus::Success) exhausted_by_bound = false;
    return translate_cursor_status(close_status);
}

bool KeyStoreCursor::current_key_is_in_bounds(const Key &key) const {
    // Range scans are half-open: the encoded end key is never part of the result.
    if (
        exclusive_end.has_value() &&
        KeyCodec::compare(key, *exclusive_end) >= 0
    ) {
        return false;
    }

    if (required_prefix.has_value()) {
        // Key types participate in global ordering. Matching payload bytes is
        // not enough: string "a" and byte-vector {'a'} are different families.
        if (key.type != required_prefix->type) return false;
        if (key.data.size() < required_prefix->data.size()) return false;
        if (!std::equal(
            required_prefix->data.begin(),
            required_prefix->data.end(),
            key.data.begin()
        )) {
            return false;
        }
    }

    return true;
}

KeyStoreCursorStatus KeyStoreCursor::translate_cursor_status(
    BTreeCursorStatus status
) {
    // Hide storage-specific failure details from the caller-facing cursor API while
    // preserving the distinctions a caller can act on.
    switch (status) {
        case BTreeCursorStatus::Success:
            return KeyStoreCursorStatus::Success;
        case BTreeCursorStatus::EndOfTree:
            return KeyStoreCursorStatus::EndOfScan;
        case BTreeCursorStatus::NotPositioned:
            return KeyStoreCursorStatus::NotPositioned;
        case BTreeCursorStatus::Closed:
            return KeyStoreCursorStatus::Closed;
        case BTreeCursorStatus::FailedToRead:
        case BTreeCursorStatus::FailedToReleasePage:
            return KeyStoreCursorStatus::StorageError;
    }

    return KeyStoreCursorStatus::StorageError;
}

// ==================================== KeyStore ====================================

void KeyStore::attach_transaction_manager(
    TransactionManager &manager
) noexcept {
    transaction_manager = &manager;
    tree.attach_transaction_manager(manager);
}

void KeyStore::undo(
    Transaction &transaction,
    const UndoDescriptor &undo,
    CompensationAppender append_compensation
) {
    // BTree invokes the callback before its operation object releases the
    // logical page latches protecting the inverse and its physical effects.
    if (!tree.apply_undo(
            transaction,
            undo,
            std::move(append_compensation))) {
        throw std::runtime_error("Failed to apply logical transaction undo");
    }
}

KeyStore::~KeyStore() {
    if (!is_open) return;
    tree.close();
    is_open = false;
}

KeyStoreStatus KeyStore::open(const std::string &db_file) {
    // One KeyStore object owns at most one live BTree/Pager session. Reopening
    // the same object is allowed only after a successful close.
    if (is_open) return KeyStoreStatus::AlreadyOpen;

    BTreeStatus open_status = tree.open(db_file);
    if (open_status != BTreeStatus::Success) return KeyStoreStatus::FailedToOpen;

    is_open = true;
    return KeyStoreStatus::Success;
}

KeyStoreStatus KeyStore::close() {
    if (!is_open) return KeyStoreStatus::Success;

    // A transaction may still hold locks and an undo chain that reference this
    // store's BTree. Tearing the pager down underneath it would leave a later
    // abort/undo dereferencing a closed session.
    if (transaction_manager && transaction_manager->has_active_transactions()) {
        return KeyStoreStatus::WriteTransactionActive;
    }

    BTreeStatus close_status = tree.close();
    if (close_status == BTreeStatus::CursorActive) {
        return KeyStoreStatus::CursorActive;
    }

    // BTree::close tears down its pager even when that teardown reports failure.
    is_open = false;
    if (close_status != BTreeStatus::Success) {
        return KeyStoreStatus::FailedToClose;
    }

    return KeyStoreStatus::Success;
}

KeyStoreStatus KeyStore::flush() {
    if (!is_open) return KeyStoreStatus::NotOpen;
    return tree.flush() == BTreeStatus::Success
        ? KeyStoreStatus::Success
        : KeyStoreStatus::WriteFailed;
}

KeyStoreGetResult KeyStore::get(
    const TransactionHandle &transaction,
    const Key &key
) {
    KeyStoreGetResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }
    if (!transaction_manager || !transaction) {
        result.status = KeyStoreStatus::TransactionNotFound;
        return result;
    }
    if (!KeyCodec::validate_key(key)) {
        result.status = KeyStoreStatus::InvalidKey;
        return result;
    }

    // Logical locks are always acquired before entering the B-tree and taking
    // any page latch. A lock already owned by this transaction is reusable.
    LockManagerStatus lock_status = transaction_manager->lock_manager().lock_shared(
        transaction->id(),
        key);
    if (lock_status == LockManagerStatus::Deadlock) {
        result.status = KeyStoreStatus::Deadlock;
        return result;
    }
    if (lock_status == LockManagerStatus::TransactionNotFound) {
        result.status = KeyStoreStatus::TransactionNotFound;
        return result;
    }
    if (lock_status != LockManagerStatus::Success &&
        lock_status != LockManagerStatus::TxnHoldsShared &&
        lock_status != LockManagerStatus::TxnHoldsExclusive) {
        result.status = KeyStoreStatus::ReadFailed;
        return result;
    }

    BTreeGetStatus get_result = tree.get(key);
    if (get_result.status == BTreeStatus::KeyNotInTree) {
        result.status = KeyStoreStatus::KeyNotFound;
        return result;
    }
    if (get_result.status != BTreeStatus::Success) {
        result.status = KeyStoreStatus::ReadFailed;
        return result;
    }
    if (!ValueCodec::validate_value(get_result.value)) {
        result.status = KeyStoreStatus::DecodeFailed;
        return result;
    }

    result.value = std::move(get_result.value);
    return result;
}

KeyStoreStatus KeyStore::put(
    const TransactionHandle &transaction,
    const Key &key,
    const Value &value,
    Locking locking
) {
    if (!is_open) return KeyStoreStatus::NotOpen;
    if (!transaction_manager || !transaction) return KeyStoreStatus::TransactionNotFound;
    if (!KeyCodec::validate_key(key)) return KeyStoreStatus::InvalidKey;
    if (!ValueCodec::validate_value(value)) return KeyStoreStatus::InvalidValue;

    // Writers take the exclusive logical key lock immediately. This avoids a
    // read-then-promote cycle inside KeyStore while still allowing replacement
    // undo to read the old value under the exclusive lock.
    if (locking == Locking::Acquire) {
        LockManagerStatus lock_status = transaction_manager->lock_manager().lock_exclusive(
            transaction->id(),
            key);
        if (lock_status == LockManagerStatus::Deadlock) return KeyStoreStatus::Deadlock;
        if (lock_status == LockManagerStatus::TransactionNotFound) {
            return KeyStoreStatus::TransactionNotFound;
        }
        if (lock_status != LockManagerStatus::Success &&
            lock_status != LockManagerStatus::TxnHoldsExclusive) {
            return KeyStoreStatus::WriteFailed;
        }
    }

    BTreeGetStatus previous = tree.get(key);
    if (previous.status != BTreeStatus::Success &&
        previous.status != BTreeStatus::KeyNotInTree) {
        return KeyStoreStatus::ReadFailed;
    }

    PendingBTreeAction action(transaction->id(), transaction_manager->prepare_to_log(transaction));
    if (previous.status == BTreeStatus::Success) {
        action.set_undo(UpdateUndo{key, previous.value});
    } else {
        action.set_undo(InsertUndo{key});
    }

    Value stored_value = value;
    BTreeStatus write_status = tree.insert(
        transaction,
        key,
        stored_value,
        action);
    return write_status == BTreeStatus::Success
        ? KeyStoreStatus::Success
        : KeyStoreStatus::WriteFailed;
}

KeyStoreRemoveResult KeyStore::remove(
    const TransactionHandle &transaction,
    const Key &key,
    Locking locking
) {
    KeyStoreRemoveResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }
    if (!transaction_manager || !transaction) {
        result.status = KeyStoreStatus::TransactionNotFound;
        return result;
    }
    if (!KeyCodec::validate_key(key)) {
        result.status = KeyStoreStatus::InvalidKey;
        return result;
    }

    if (locking == Locking::Acquire) {
        LockManagerStatus lock_status = transaction_manager->lock_manager().lock_exclusive(
            transaction->id(),
            key);
        if (lock_status == LockManagerStatus::Deadlock) {
            result.status = KeyStoreStatus::Deadlock;
            return result;
        }
        if (lock_status == LockManagerStatus::TransactionNotFound) {
            result.status = KeyStoreStatus::TransactionNotFound;
            return result;
        }
        if (lock_status != LockManagerStatus::Success &&
            lock_status != LockManagerStatus::TxnHoldsExclusive) {
            result.status = KeyStoreStatus::WriteFailed;
            return result;
        }
    }

    BTreeGetStatus previous = tree.get(key);
    if (previous.status == BTreeStatus::KeyNotInTree) {
        result.status = KeyStoreStatus::KeyNotFound;
        return result;
    }
    if (previous.status != BTreeStatus::Success) {
        result.status = KeyStoreStatus::ReadFailed;
        return result;
    }

    PendingBTreeAction action(transaction->id(), transaction_manager->prepare_to_log(transaction));
    action.set_undo(DeleteUndo{key, previous.value});
    BTreeRemoveStatus remove_result = tree.remove(transaction, key, action);
    if (remove_result.status != BTreeStatus::Success) {
        result.status = KeyStoreStatus::WriteFailed;
        return result;
    }

    result.value = std::move(remove_result.value);
    return result;
}

// ====================================== Scans =====================================

KeyStoreScanResult KeyStore::scan() {
    /**
     * Open a full ordered scan.
     *
     * BTree cursors start unpositioned, so KeyStore positions on the leftmost
     * key before transferring ownership to KeyStoreCursor. EndOfTree is not a
     * scan-construction failure; it creates a valid empty logical cursor.
     */
    KeyStoreScanResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }

    BTreeCursor cursor = tree.open_cursor();
    BTreeCursorStatus seek_status = cursor.seek_first();
    if (
        seek_status != BTreeCursorStatus::Success &&
        seek_status != BTreeCursorStatus::EndOfTree
    ) {
        result.status = KeyStoreStatus::ScanFailed;
        return result;
    }

    // Construct the private wrapper here, then move it into optional. The
    // wrapper now owns the BTree cursor registration and any pinned leaf.
    result.cursor.emplace(KeyStoreCursor(std::move(cursor)));
    result.status = KeyStoreStatus::Success;
    return result;
}

KeyStoreScanResult KeyStore::scan_from(const Key &start) {
    /**
     * Open a lower-bound scan over [start, end of tree).
     *
     * BTreeCursor::seek already implements "first key >= target", including
     * crossing a leaf boundary when the target is absent from its search leaf.
     */
    KeyStoreScanResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }

    if (!KeyCodec::validate_key(start)) {
        result.status = KeyStoreStatus::InvalidKey;
        return result;
    }

    BTreeCursor cursor = tree.open_cursor();
    BTreeCursorStatus seek_status = cursor.seek(start);
    if (
        seek_status != BTreeCursorStatus::Success &&
        seek_status != BTreeCursorStatus::EndOfTree
    ) {
        result.status = KeyStoreStatus::ScanFailed;
        return result;
    }

    result.cursor.emplace(KeyStoreCursor(std::move(cursor)));
    result.status = KeyStoreStatus::Success;
    return result;
}

KeyStoreScanResult KeyStore::scan_range(
    const Key &start,
    const Key &end
) {
    /**
     * Open a half-open ordered range [start, end).
     *
     * KeyCodec defines a total order across key families, so heterogeneous
     * ranges are valid. Only start > end is invalid; start == end is a valid
     * empty range and is represented by a bound-exhausted cursor.
     */
    KeyStoreScanResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }

    if (!KeyCodec::validate_key(start) || !KeyCodec::validate_key(end)) {
        result.status = KeyStoreStatus::InvalidKey;
        return result;
    }
    if (KeyCodec::compare(start, end) > 0) {
        result.status = KeyStoreStatus::InvalidRange;
        return result;
    }

    BTreeCursor cursor = tree.open_cursor();
    BTreeCursorStatus seek_status = cursor.seek(start);
    if (
        seek_status != BTreeCursorStatus::Success &&
        seek_status != BTreeCursorStatus::EndOfTree
    ) {
        result.status = KeyStoreStatus::ScanFailed;
        return result;
    }

    // The physical cursor is positioned only at the lower bound. The wrapper
    // owns the exclusive upper-bound check on current() and next().
    result.cursor.emplace(KeyStoreCursor(
        std::move(cursor),
        end
    ));
    result.status = KeyStoreStatus::Success;
    return result;
}

KeyStoreScanResult KeyStore::scan_prefix(const Key &prefix) {
    /**
     * Open a prefix scan for string or byte-vector keys.
     *
     * Seeking to the encoded prefix finds the first possible match because both
     * variable-length key families use lexicographic payload ordering. The
     * wrapper stores the encoded prefix and ends the scan as soon as either the
     * key family changes or the payload no longer begins with those bytes.
     */
    KeyStoreScanResult result{};
    if (!is_open) {
        result.status = KeyStoreStatus::NotOpen;
        return result;
    }

    if (
        !KeyCodec::validate_key(prefix) ||
        (prefix.type != KeyType::String && prefix.type != KeyType::Bytes)
    ) {
        result.status = KeyStoreStatus::InvalidKey;
        return result;
    }

    BTreeCursor cursor = tree.open_cursor();
    BTreeCursorStatus seek_status = cursor.seek(prefix);
    if (
        seek_status != BTreeCursorStatus::Success &&
        seek_status != BTreeCursorStatus::EndOfTree
    ) {
        result.status = KeyStoreStatus::ScanFailed;
        return result;
    }

    result.cursor.emplace(KeyStoreCursor(
        std::move(cursor),
        std::nullopt,
        prefix
    ));
    result.status = KeyStoreStatus::Success;
    return result;
}
