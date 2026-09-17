# ARIES Crash-Recovery

Assumptions:
- The WAL log doesn't implement checkpoints.
- On startup, the database server must not process any requests until it has recovered the database.
- A database recovery walks the WAL start-to-finish; there is no dirty-page-table shortcut since there are no checkpoints.
- After recovery is done, the server deletes every WAL segment that is not the active one — this is safe only because a full recovery run resolves every transaction referenced anywhere in the retained log (see Cleanup below).
- After recovery is done, the server can start listening for connections.

## 1. Open the log

List the WAL directory, collect every segment file, and sort by base LSN/offset. Construct the `Log` over those segments with the last one as the active segment (the only one still being appended to).

## 2. Analysis + Redo (single forward pass)

Start at the very first record of the first segment — **whatever type it is**. Do not seek forward looking for a `TxnBegin`; a segment's first record is only guaranteed to be a `TxnBegin` for the very first segment the database ever wrote. Once at least one recovery-and-cleanup cycle has happened, a retained segment's first record can just as easily be a `Compensation`/`TxnEnd` left over from a previous recovery's own undo work (that work gets appended to whatever segment was active *at recovery time*, not back into the segment the transaction originally started in).

Walk every record from there to the end of the log, in order:

- **`TxnBegin`**: add the transaction to the tracking table (unresolved transactions), recording its LSN as the last-seen LSN for that transaction.
- **`BTreeAction`, `Compensation`, `SystemAction`**: these carry physical `PageEffect`s. For each effect, write its after-image to the database file at that page's offset. Then update the transaction's last-seen LSN to this record's LSN. (A conditional check — only write if the page's current on-disk `pageLSN` is less than this record's LSN — is a valid optimization to skip redundant writes, but is not required for correctness: since every record for every page is applied in strict increasing LSN order, the last record touching a page is always the last one applied regardless.)
  - If this record's transaction isn't in the tracking table (its `TxnBegin` isn't visible because that segment was already cleaned up), still redo it — just don't add a new tracking entry for it. This is expected, not an error: a transaction can only have had its home segment deleted if it was already fully resolved (see Cleanup), so it cannot need undo later.
- **`TxnAbort`**: does **not** resolve the transaction — it only records the abort decision. Leave it in the tracking table. (This is exactly how an in-progress, previously-crashed rollback looks: a `TxnAbort` with no following `TxnEnd`.)
- **`TxnCommit`** or **`TxnEnd`**: remove the transaction from the tracking table — it's fully resolved and needs no undo. (A committed transaction is resolved by `TxnCommit` alone, since it never needs undo; only an aborted transaction gets a `TxnEnd`, written once its own undo is complete.)

At the end of this pass, every transaction still in the tracking table is a loser: either it crashed mid-abort (has `TxnAbort`, no `TxnEnd`) or it never reached a decision at all (still "Active" when the crash happened — an in-flight transaction, which recovery treats identically to a crash mid-abort). Both cases start their undo walk from the transaction's last-seen LSN; recovery doesn't need `TxnAbort` to already be present to know a transaction needs undo.

## 3. Undo — globally decreasing LSN order

If the tracking table is non-empty, undo every remaining loser. Order matters at the level of individual records, not whole transactions: maintain one "next LSN to undo" cursor per loser (initialized to its last-seen LSN), in a max-priority structure keyed by that cursor. Repeatedly:

1. Pop the loser with the globally highest next-undo-LSN.
2. Read that record.
   - **`Compensation`**: no page mutation needed (it was already redone in step 2). Set this loser's cursor to the record's `undo_next_lsn` field — **not** `prev_lsn`. `undo_next_lsn` is the pointer the CLR itself recorded to skip past the action it already compensates; following `prev_lsn` instead would walk back into the already-undone action and undo it a second time.
   - **`BTreeAction`**: run the logical undo for `undo` through the transaction's undo executor, producing the compensating `PageEffect`s. Append a new `Compensation` record with `undo_of_lsn` = this record's LSN, `undo_next_lsn` = this record's `prev_lsn`, and the produced effects. Set this loser's cursor to this record's `prev_lsn`.
   - **`TxnBegin`**: this loser has nothing left to undo. Append a `TxnEnd` record for it and drop it from the tracking table — do not re-insert it into the priority structure.
3. If the loser still has work left (didn't just hit `TxnBegin`), re-insert it into the priority structure at its new cursor LSN, and go back to step 1.

Repeat until the tracking table is empty. Because every step operates on whichever loser's *next* record is globally most recent, two losers whose chains interleave on a shared page (a real possibility here, since page latches are held only for the duration of one B-tree call, not a transaction's whole lifetime — two still-uncommitted transactions can structurally touch the same page at different times) are always undone in the same order their forward actions actually happened, just reversed.

Once every loser is fully resolved, call `sync_through` on the highest LSN appended during this pass (or on the log's current tail). This must complete before cleanup — cleanup's safety depends on every record it's about to make unreachable (by deleting the segment holding it) already being durable, and by the transaction's own decision record (`TxnCommit`/`TxnEnd`) also being durable.

## 4. Cleanup

First re-record the Raft applied watermark in the active segment (a synced empty commit carrying `raft_index`), because the commit records that carried it may all live in segments about to be deleted — see *Seeding `commit_index` and `last_applied` on startup* in `Raft Replication.md`. Then delete every segment that is not the currently-active one. This is safe here specifically because step 2 walked the *entire* retained log and step 3 fully resolved every transaction found unresolved in it — by this point every record in every non-active segment has had its effects applied to the database file, and every transaction referenced anywhere in the retained log has a durable `TxnCommit` or `TxnEnd`. Nothing later can need to re-read a deleted segment.

## 5. Start serving

Only after cleanup completes does the server begin accepting connections.
