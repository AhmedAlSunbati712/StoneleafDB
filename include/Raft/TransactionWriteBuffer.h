#pragma once

#include <Key.h>
#include <KeyCodec.h>
#include <Raft/RaftEntry.h>
#include <Value.h>

#include <map>
#include <optional>
#include <vector>

// Orders keys by the engine's own encoded-byte comparison. Key deliberately has
// no comparison operators, and giving it a defaulted operator<=> would order by
// (type, size, data) - which is NOT the order the storage engine uses. Two
// disagreeing key orderings in one codebase is the kind of defect that surfaces
// much later as a corrupt index, so this wraps the single canonical one.
struct KeyOrder {
    bool operator()(const Key& lhs, const Key& rhs) const {
        return KeyCodec::compare(lhs, rhs) < 0;
    }
};

// A session's pending writes, and the single authoritative copy of them: it
// serves read-your-own-writes during execution and is serialized into the Raft
// entry at commit, so a write set is never stored twice.
//
// Nothing here touches the KeyStore. A transaction's writes reach the B-tree
// only after its entry is committed and the apply loop runs it - on every node,
// through byte-identical code.
struct TransactionWriteBuffer {
    // nullopt is a tombstone: "this transaction deleted the key", which is a
    // different state from the key simply being absent from the map. A plain
    // erase would lose the delete and let a stale value survive the commit.
    std::map<Key, std::optional<Value>, KeyOrder> writes;

    void put(const Key& key, const Value& value);
    void remove(const Key& key);

    // Three-way answer for read-your-own-writes, because "absent from the
    // buffer" and "deleted by this transaction" must not collapse into one
    // result. Untouched means the caller should read through to the KeyStore.
    enum class Lookup { Untouched, Written, Deleted };
    Lookup lookup(const Key& key, Value& out) const;

    bool empty() const { return writes.empty(); }
    void clear() { writes.clear(); }

    // Collapses repeated writes to one key into last-write-wins, so
    // put(k,1); put(k,2) yields a single MutationOp. Ordering is by key rather
    // than by issue order, which is sound precisely because duplicates are
    // already collapsed: no two operations in the result touch the same key, so
    // no ordering between them is observable.
    std::vector<MutationOp> to_operations() const;
};
