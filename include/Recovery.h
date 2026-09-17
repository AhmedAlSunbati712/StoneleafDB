#pragma once

#include <KeyStore.h>
#include <Log/Log.h>
#include <TransactionManager/TransactionManager.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

// ARIES recovery over the WAL. Lives in the library rather than beside main()
// so tests can drive it directly, the way setup_database() does.

// Analysis + redo, against the raw database file: no Pager/BTree/KeyStore is
// open yet. Fills unresolved_transactions with every transaction lacking a
// TxnCommit/TxnEnd, and sets last_applied_raft_index to the highest Raft index
// carried by a committed transaction's commit record - 0 for a database that
// has applied no Raft entries.
void aries_recovery_redo(Log& log,
    const std::string& db_file_name,
    std::unordered_map<TransactionId, Lsn>& unresolved_transactions,
                         std::uint64_t& last_applied_raft_index);

// Undo, for whatever redo left unresolved. Runs through the live KeyStore.
void aries_recovery_undo(Log& log,
    KeyStore& key_store,
    std::unordered_map<TransactionId, Lsn>& unresolved_transactions);

// Returns {base_lsn, is_store} for a WAL segment file name. A name starting
// with "segment-" must have the exact "<20 digits>.store" or
// "<20 digits>.index" shape, or it throws; any other name returns {0, false}.
std::pair<std::uint64_t, bool> parse_wal_segment_name(const std::string& name);

// Deletes every WAL segment except the active one. Only safe once undo has
// flushed every page it dirtied to the database file.
void cleanup_finalized_segments(const std::string& db_file_name);

// The last step of startup recovery: re-records last_applied_raft_index in
// the active segment, durably, then deletes every finalized segment. The
// watermark lives only in commit records, so without the first step a
// restart whose active segment holds none of them would forget it and
// re-apply the Raft log from index 1.
void finish_recovery(TransactionManager& transaction_manager,
                     const std::string& db_file_name,
                     std::uint64_t last_applied_raft_index);
