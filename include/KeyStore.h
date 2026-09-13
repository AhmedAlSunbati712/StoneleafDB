#pragma once

#include <BTree.h>
#include <BTreeCursor.h>
#include <Key.h>
#include <Value.h>
#include <TransactionManager/TransactionManager.h>

#include <cstdint>
#include <optional>
#include <string>

enum class KeyStoreStatus : std::uint8_t {
    Success = 0,
    FailedToOpen,
    FailedToClose,
    KeyNotFound,
    InvalidKey,
    InvalidValue,
    InvalidRange,
    NoActiveTransaction,
    TransactionAlreadyActive,
    TransactionNeedsRollback,
    WriteTransactionActive,
    CursorActive,
    WriteFailed,
    CommitFailed,
    RollbackFailed,
    ScanFailed,
    // Appended to preserve the numeric values of the original public statuses.
    NotOpen,
    AlreadyOpen,
    ReadFailed,
    DecodeFailed,
    Deadlock,
    TransactionNotFound,
    // Appended, like the block above: these values go on the wire in the
    // command protocol, so inserting anywhere else would silently reinterpret
    // every existing status.
    //
    // NotLeader: this node cannot accept the write; the client must find the
    // leader. CommitUnknown: the entry was proposed but its fate was not
    // established before COMMIT_TIMEOUT - it may still commit, so unlike a
    // failure it is NOT safe to blindly retry.
    NotLeader,
    CommitUnknown
};

// Whether a write takes the logical key lock. Acquire is the only correct
// choice for client transactions. Skip exists for the Raft apply loop: on a
// leader the proposing session already holds the exclusive lock and is waiting
// on last_applied, so taking it again would deadlock against that session.
enum class Locking : std::uint8_t {
    Acquire = 0,
    Skip,
};

enum class KeyStoreCursorStatus : std::uint8_t {
    Success = 0,
    EndOfScan,
    NotPositioned,
    Closed,
    DecodeFailed,
    StorageError
};

struct KeyStoreGetResult {
    KeyStoreStatus status = KeyStoreStatus::Success;
    std::optional<Value> value;
};

struct KeyStoreRemoveResult {
    KeyStoreStatus status = KeyStoreStatus::Success;
    std::optional<Value> value;
};

struct KeyStoreEntry {
    Key key;
    Value value;
};

struct KeyStoreCursorResult {
    KeyStoreCursorStatus status = KeyStoreCursorStatus::NotPositioned;
    std::optional<KeyStoreEntry> entry;
};

class KeyStore;

class KeyStoreCursor {
    public:
        ~KeyStoreCursor() = default;

        KeyStoreCursor(const KeyStoreCursor &) = delete;
        KeyStoreCursor &operator=(const KeyStoreCursor &) = delete;
        KeyStoreCursor(KeyStoreCursor &&) noexcept = default;
        KeyStoreCursor &operator=(KeyStoreCursor &&) noexcept = default;

        KeyStoreCursorResult current() const;
        KeyStoreCursorStatus next();
        bool valid() const;
        KeyStoreCursorStatus close();

    private:
        friend class KeyStore;

        explicit KeyStoreCursor(
            BTreeCursor cursor,
            std::optional<Key> exclusive_end = std::nullopt,
            std::optional<Key> required_prefix = std::nullopt
        );

        bool current_key_is_in_bounds(const Key &key) const;
        static KeyStoreCursorStatus translate_cursor_status(BTreeCursorStatus status);

        BTreeCursor cursor;
        std::optional<Key> exclusive_end;
        std::optional<Key> required_prefix;
        bool exhausted_by_bound = false;
};

struct KeyStoreScanResult {
    KeyStoreStatus status = KeyStoreStatus::Success;
    std::optional<KeyStoreCursor> cursor;
};

class KeyStore : public TransactionUndoExecutor {
    public:
        KeyStore() = default;
        ~KeyStore();

        KeyStore(const KeyStore &) = delete;
        KeyStore &operator=(const KeyStore &) = delete;
        KeyStore(KeyStore &&) = delete;
        KeyStore &operator=(KeyStore &&) = delete;

        // A KeyStore owns one open BTree at a time. Any cursor returned from a
        // scan must be destroyed before its KeyStore.
        KeyStoreStatus open(const std::string &db_file);
        // Closing an already-closed store succeeds. Fails with
        // WriteTransactionActive while any TransactionManager transaction is
        // still active; callers must finish it first.
        KeyStoreStatus close();
        // Writes every dirty page to the database file and syncs once,
        // without closing the store.
        KeyStoreStatus flush();

        KeyStoreGetResult get(
            const TransactionHandle &transaction,
            const Key &key);
        KeyStoreStatus put(
            const TransactionHandle &transaction,
            const Key &key,
            const Value &value,
            Locking locking = Locking::Acquire);
        KeyStoreRemoveResult remove(
            const TransactionHandle &transaction,
            const Key &key,
            Locking locking = Locking::Acquire);

        void attach_transaction_manager(TransactionManager &manager) noexcept;

        void undo(
            Transaction &transaction,
            const UndoDescriptor &undo,
            CompensationAppender append_compensation) override;

        // Successful scans return an already-positioned cursor. An empty result
        // is represented by a cursor whose current status is EndOfScan.
        // Full ordered scan, starting at the leftmost leaf.
        KeyStoreScanResult scan();

        // Lower-bound scan over [start, end of tree).
        KeyStoreScanResult scan_from(const Key &start);

        // Half-open range scan over [start, end).
        KeyStoreScanResult scan_range(const Key &start, const Key &end);

        // Scan encoded string or byte keys beginning with prefix.
        KeyStoreScanResult scan_prefix(const Key &prefix);

    private:
        BTree tree;
        bool is_open = false;
        TransactionManager *transaction_manager = nullptr;
};
