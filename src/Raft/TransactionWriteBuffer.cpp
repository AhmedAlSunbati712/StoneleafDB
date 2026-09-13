#include <Raft/TransactionWriteBuffer.h>

#include <utility>

void TransactionWriteBuffer::put(const Key& key, const Value& value) {
    // insert_or_assign, not insert: a second write to the same key must win,
    // and it must also overwrite a tombstone left by an earlier delete.
    writes.insert_or_assign(key, value);
}

void TransactionWriteBuffer::remove(const Key& key) {
    // Records a tombstone rather than erasing. Erasing would make the key look
    // untouched, and the read path would then fall through to the KeyStore and
    // return a value this transaction has already deleted.
    writes.insert_or_assign(key, std::nullopt);
}

TransactionWriteBuffer::Lookup TransactionWriteBuffer::lookup(
    const Key& key, Value& out) const {
    const auto found = writes.find(key);
    if (found == writes.end()) return Lookup::Untouched;
    if (!found->second) return Lookup::Deleted;
    out = *found->second;
    return Lookup::Written;
}

std::vector<MutationOp> TransactionWriteBuffer::to_operations() const {
    std::vector<MutationOp> operations;
    operations.reserve(writes.size());

    for (const auto& [key, value] : writes) {
        if (value) {
            operations.push_back(MutationOp{
                .type = RaftMutationType::Put,
                .operation = PutMutation{.key = key, .value = *value},
            });
        } else {
            operations.push_back(MutationOp{
                .type = RaftMutationType::Delete,
                .operation = DeleteMutation{.key = key},
            });
        }
    }

    return operations;
}
