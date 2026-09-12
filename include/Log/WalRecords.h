#pragma once

#include <Log/WalPayload.h>

namespace WalRecords {
    PendingWalRecord begin(std::uint64_t transaction_id);
    PendingWalRecord btree_action(std::uint64_t transaction_id, Lsn prev_lsn,
                                  const BTreeActionPayload& payload);
    PendingWalRecord compensation(std::uint64_t transaction_id, Lsn prev_lsn,
                                  const CompensationPayload& payload);
    PendingWalRecord system_action(const SystemActionPayload& payload);
    // raft_index is the Raft entry this transaction applied; 0 for a
    // transaction that applied none, which is every client transaction.
    PendingWalRecord commit(std::uint64_t transaction_id, Lsn prev_lsn,
                            std::uint64_t raft_index = 0);
    PendingWalRecord abort(std::uint64_t transaction_id, Lsn prev_lsn, AbortReason reason);
    PendingWalRecord end(std::uint64_t transaction_id, Lsn prev_lsn);
    WalPayload decode(const WalRecord& record);
}
