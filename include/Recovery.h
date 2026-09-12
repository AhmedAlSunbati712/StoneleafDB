#pragma once

#include <KeyStore.h>
#include <Log/Log.h>
#include <TransactionManager/TransactionManager.h>

#include <cstdint>
#include <string>
#include <unordered_map>

// ARIES recovery over the WAL. Lives in the library rather than beside main()
// so tests can drive it directly, the way setup_database() does.

// Analysis + redo, against the raw database file: no Pager/BTree/KeyStore is
// open yet. Fills unresolved_transactions with every transaction lacking a
// TxnCommit/TxnEnd, so the undo pass knows what still needs undoing.
void aries_recovery_redo(Log& log,
    const std::string& db_file_name,
    std::unordered_map<TransactionId, Lsn>& unresolved_transactions);

// Undo, for whatever redo left unresolved. Runs through the live KeyStore.
void aries_recovery_undo(Log& log,
    KeyStore& key_store,
    std::unordered_map<TransactionId, Lsn>& unresolved_transactions);
