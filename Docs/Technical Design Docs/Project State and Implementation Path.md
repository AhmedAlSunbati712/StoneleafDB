# Project State and Implementation Path

## Purpose

This document is the short re-entry guide for StoneleafDB. It answers three
questions:

1. What is implemented today?
2. What system are we trying to build?
3. In what order should we build it?

The goal is to keep the next steps understandable and prevent local recovery,
threading, networking, and replication from becoming one large implementation
project.

This document reflects `main` at commit `1f30306`. At that revision, the
repository is clean and all 162 tests pass: 114 unit tests and 48 integration
tests.

## One-Sentence Summary

StoneleafDB is currently an embedded B+ tree with rollback-journal recovery; it
is intended to become a multithreaded server that stores one replicated
key-value database using WAL/ARIES locally and Raft between nodes.

## Current State

### Storage Engine

The current storage stack is:

```text
B+ tree
    -> Pager
        -> process-local page cache
        -> rollback journal
        -> database file
```

The pager currently supports:

- Fixed 4 KiB pages.
- Database creation and opening.
- Page reads and writes.
- Page pinning and unpinning.
- A 64-frame page cache.
- Hash-based page lookup.
- LRU replacement among unpinned pages.
- Dirty-page tracking.
- Dirty-page spill under cache pressure.
- Page allocation by extending the database.
- Page reuse through a freelist.
- Database-header and freelist recovery.

### Recovery

The current recovery mechanism is a rollback journal:

1. Save the original image of each modified page.
2. Make the journal durable.
3. Write dirty pages to the database file.
4. Synchronize the database.
5. Truncate the journal after commit.

The implementation supports:

- Checksummed journal records.
- Multiple journal sections after cache spill.
- Hot-journal detection.
- Restoration of overwritten pages.
- Truncation of pages allocated by an incomplete transaction.
- Recovery of freelist and B+ tree metadata changes.

This provides a functioning embedded recovery model, but it is not WAL and it
does not implement ARIES analysis, redo, or logged undo.

### B+ Tree

The B+ tree currently supports:

- Typed Boolean, unsigned integer, signed integer, string, and byte keys.
- Typed values.
- Point lookup.
- Insert and overwrite.
- Delete.
- Leaf and internal-page splitting.
- Borrowing and merging after deletion.
- Root creation, replacement, and height reduction.
- Ordered cursors and scans.

Active cursors currently prevent mutations and transaction boundaries. That is
safe for the embedded implementation but too coarse for the target server.

### Key-Value API

`include/KeyStore.h` and `src/KeyStore.cpp` implement the local key-value
interface:

```text
open
close
get
put
remove
scan
begin_write_transaction
commit
rollback
```

The boundary supports automatic and explicit write policies, typed point
operations, full and bounded scans, rollback-required transaction state, and
move-only cursors. Integration tests cover persistence, rollback, cross-process
write conflicts, typed ordering, and cursor ownership.

The v1 implementation inherits the B+ tree's count-based split policy. Client
entries are therefore assumed to fit the current 4 KiB page layout in
aggregate; KeyStore validates the existing 16-bit payload representation but
does not add overflow pages or byte-capacity splitting.

### Concurrency

The existing concurrency model is based on multiple embedded processes opening
the same database:

```text
NOLOCK -> SHARED -> RESERVED -> PENDING -> EXCLUSIVE
```

It uses POSIX advisory file locks and the database file-change counter to detect
commits made by another process and invalidate stale private caches.

This model is implemented and tested, but it is not the target server model.
Traditional process-owned file locks cannot represent independent transactions
running as threads inside one server process.

### What Does Not Exist Yet

- A server process or network protocol.
- A shared in-process buffer pool.
- Thread-safe page lookup, pins, or page contents.
- RAII page guards.
- Per-page read/write latches.
- Latch crabbing in the B+ tree.
- Logical key or range locks.
- WAL segments or WAL records.
- Persistent `pageLSN`, `prevLSN`, `recLSN`, or `undoNextLSN` values.
- Compensation log records.
- ARIES analysis, redo, and undo.
- Checkpoint and WAL recycling infrastructure.
- A background page writer.
- Raft, replicated commands, snapshots, or request deduplication.

## Goal State

The first complete distributed target is one unsharded replicated key-value
database. Sharding comes later.

```text
                              Raft group
                     ┌────────────┼────────────┐
                     │            │            │
                   Node A       Node B       Node C
```

Each node is one multithreaded server process:

```text
StoneleafDB node
├── connection worker pool
├── Raft runtime
├── ordered Raft apply loop
├── WAL writer
├── background page writer
└── checkpointer

Shared local engine
├── KeyStore
├── B+ tree
├── transaction manager
├── logical key/range lock manager
├── shared buffer pool
├── page latches and pins
└── WAL/ARIES recovery
```

### Target Behavior

- Many readers operate concurrently.
- One ordered Raft apply loop performs local writes.
- A reader and writer using unrelated keys and leaves do not block each other.
- Readers never observe uncommitted values.
- B+ tree structural changes are protected by short page latches.
- Dirty uncommitted pages may reach disk: STEAL.
- Commit does not force every database page: NO-FORCE.
- WAL supports redo and undo after a crash.
- Every replica applies the same logical commands in the same Raft order.
- Each replica owns its own physical WAL and page layout.
- A successful request survives one node failure in a three-node group.

## Target Concurrency Model

The final server should not use the current database-wide file locks for
transaction coordination.

### Server Ownership

The server acquires one exclusive operating-system lock on
`<database>.lock` at startup and retains it for its lifetime. This prevents two
StoneleafDB server processes or an offline mutation tool from opening the same
database.

This ownership lock is not acquired or promoted by individual transactions.

### Logical Transaction Locks

Logical locks protect isolation:

```text
Get(K)       -> shared lock on K
Put(K)       -> exclusive lock on K
Delete(K)    -> exclusive lock on K
Scan[A, M)   -> shared range lock on [A, M)
```

Transaction locks are owned by transaction IDs, not thread IDs. Strict
two-phase locking holds them through commit or rollback.

A write batch acquires its key locks in canonical sorted-key order. This limits
deadlocks and makes lock acquisition deterministic.

### Page Latches

Page latches protect short physical B+ tree operations:

- Shared latch for reading a page.
- Exclusive latch for changing a page.
- A latch is released as soon as the operation no longer needs the page.
- Latches are never retained for the complete transaction.

Transaction locks and page latches solve different problems:

| Mechanism | Purpose |
| --- | --- |
| Key/range lock | Prevent uncommitted or non-serializable logical reads |
| Page latch | Protect B+ tree bytes and structure in memory |
| Buffer pin | Prevent a frame from being reassigned |
| Filesystem ownership lock | Prevent a second server process |

### Latch Crabbing

Tree traversal uses parent-to-child latch coupling:

```text
latch parent
    -> locate child
    -> pin and latch child
    -> determine whether child is safe
    -> release parent when safe
```

For reads, the parent shared latch is released after the child is pinned and
shared-latched.

For insertion, a child is safe when the encoded insertion fits without a split.
Because keys and values are variable length, safety is based on available
encoded bytes rather than only key count.

For deletion, a child is safe only when deletion cannot cause underflow,
borrowing, merging, or separator propagation. Deleting the first key in a leaf
may require an ancestor separator update even when the leaf remains above
minimum occupancy.

Point writes currently use an optimistic exclusive traversal:

1. Acquire the header, root, and descendants using top-down exclusive latch
   coupling.
2. Release all ancestors when a newly acquired child proves that structural
   propagation cannot reach them.
3. Modify the leaf immediately when the leaf is safe.
4. If the target leaf is unsafe, make no change, release every latch, and
   restart from the header using pessimistic exclusive crabbing.
5. Retain pessimistic logical latches through structural repair and WAL append,
   even when their cache frames are temporarily unreferenced.

This allows a writer modifying a safe rightmost leaf to run concurrently with a
reader accessing the leftmost leaf.

### No Global Reader Drain

There is no normal transaction-level `PENDING -> EXCLUSIVE` reader drain in the
target design. Affected readers wait on logical key or range locks; unrelated
readers continue.

Global coordination remains only for:

- Server startup and recovery.
- Server shutdown.
- The lifetime filesystem ownership lock.
- Short root-pointer replacement.
- Short buffer, WAL, and checkpoint metadata critical sections.

## Raft Apply Loop

Raft stores an ordered logical command log:

```text
index 101: Put(A, 10)
index 102: Delete(B)
index 103: WriteBatch(Put(C), Put(D))
```

After Raft decides that an entry is committed, every node applies committed
entries in index order:

```text
commit index = 103
applied index = 100

apply 101 -> persist applied index 101
apply 102 -> persist applied index 102
apply 103 -> persist applied index 103
```

The apply loop may be a dedicated thread or another serial executor. The
important invariant is ordered, one-at-a-time local application. Connection
workers propose writes to Raft; they do not modify the B+ tree directly.

Raft entries contain logical key-value operations. They do not contain local
page IDs, page images, LSNs, or allocation choices. Every node maintains its
own physical recovery WAL.

## Implementation Path

The ordering below intentionally establishes one correctness boundary at a
time.

## Step 0: Correct the Architecture Documents

Update the detailed replicated-KV roadmap so it no longer prescribes a global
transaction-level reader drain. Make it consistently specify:

- One lifetime filesystem ownership lock.
- Strict two-phase key and range locks.
- Page latches and pins.
- Latch crabbing.
- One ordered Raft apply writer.

Completion criteria:

- The short and detailed roadmaps describe the same concurrency model.
- No implementation begins from contradictory locking requirements.

## Step 1: Finish the Local KeyStore Boundary

Status: implemented over the existing B+ tree.

Why this comes first:

- The interface already exists.
- It is smaller than WAL or threading.
- It creates the logical boundary needed by the server and Raft.
- Network and Raft code should never call pager methods directly.

Required behavior:

- `open` and `close`.
- Typed `get`, `put`, and `remove`.
- Automatic writes.
- Explicit write transactions.
- Commit and rollback.
- Full, lower-bound, range, and prefix scans.
- Explicit rollback-required state after a failed transactional write.

Completion criteria:

- A KeyStore test suite covers every public method.
- Automatic and explicit transactions survive close and reopen.
- Typed keys and values round-trip.
- Cursor close and destruction release all state.

## Step 2: Implement V2 Page and WAL Codecs

Implement and test the new binary formats without changing pager commit behavior
yet.

### Page Format

Reserve persistent metadata in each 4096-byte page:

```text
page magic
format version
page ID
page generation
pageLSN
checksum
page kind
payload
```

This intentionally creates an incompatible StoneleafDB v2 database format.

### WAL Format

Implement codecs for:

- WAL segment headers.
- Common record headers.
- `TXN_BEGIN`.
- `PAGE_UPDATE_FULL`.
- `PAGE_ALLOC` and `PAGE_FREE`.
- `TXN_COMMIT`.
- `TXN_ABORT` and `TXN_END`.
- Compensation log records.
- Checkpoint records.

Begin with complete before- and after-page images. They are expensive, but they
map directly to the existing rollback-journal before-images and minimize the
number of new ideas introduced at once.

Completion criteria:

- Every record round-trips through its codec.
- Checksums detect changed bytes.
- Invalid lengths and flags are rejected.
- A truncated final record is recognized as an incomplete WAL tail.
- Segment rotation never splits a record across files.
- The pager still uses its existing rollback journal.

## Step 3: Replace the Rollback Journal With WAL

Keep the engine single-threaded during this step. Recovery correctness should
not be debugged at the same time as thread interleavings.

### Step 3A: WAL Append and Commit

Write:

```text
TXN_BEGIN
PAGE_UPDATE_FULL
PAGE_UPDATE_FULL
TXN_COMMIT
```

Commit waits until the WAL is durable through `TXN_COMMIT`. It does not force
the database pages.

### Step 3B: PageLSN and Redo

Every database page persists the latest WAL update it contains. Before writing
a frame:

```text
durable_wal_lsn >= frame.pageLSN
```

Recovery applies redo when:

```text
disk_page.pageLSN < update_record.LSN
```

### Step 3C: Undo and CLRs

Add:

- Transaction IDs.
- `prevLSN` transaction chains.
- A transaction table.
- Before-images.
- Loser transaction undo.
- Compensation log records.
- `undoNextLSN` restart pointers.

At this point, uncommitted pages may safely reach the database file: STEAL.

### Step 3D: Checkpointing

Start with a sharp checkpoint:

```text
pause writes
flush WAL
flush dirty pages
sync database
write completed checkpoint
publish control record
recycle safe WAL
resume writes
```

Fuzzy checkpoints come later.

Completion criteria:

- Crash before commit restores the old state.
- Crash after durable commit restores the new state.
- Crash during undo resumes using CLRs.
- Allocation, freelist, root, split, and merge changes recover atomically.
- Repeating recovery is idempotent.
- The rollback journal is removed only after all equivalent tests pass under
  WAL.

## Step 4: Build the Multithreaded Shared Engine

Turn the storage engine into one server-owned instance:

```text
Engine
├── one KeyStore/B+ tree
├── one Pager
├── one BufferPool
├── one TransactionManager
└── one LogManager
```

Implement:

- `DatabaseOwnershipLock` held for the server lifetime.
- A shared page-to-frame directory.
- RAII `BufferHandle`, `ReadPageGuard`, and `WritePageGuard` objects.
- Atomic pin counts.
- Shared/exclusive frame latches.
- Frame generations and structural versions.
- Thread-safe replacement, preferably CLOCK rather than a global LRU list.
- Logical transaction ownership by transaction ID.
- Startup recovery before worker threads are admitted.

Completion criteria:

- Multiple readers share one authoritative cache.
- Two threads cannot load duplicate frames for one page.
- Pinned frames cannot be reassigned.
- A background page copy racing with a new update remains dirty.
- ThreadSanitizer reports no races in cache and pager tests.
- A second server process cannot acquire database ownership.

## Step 5: Add Fine-Grained Locks and Latch Crabbing

Implement strict two-phase key locks for point operations and range locks for
scans. Then convert B+ tree traversal to latch crabbing.

Implementation order:

1. Shared latch coupling for read traversal.
2. Structural version validation and root restart.
3. Optimistic leaf-only insert and overwrite.
4. Pessimistic write crabbing for splits.
5. Safe deletion criteria.
6. Pessimistic delete crabbing for borrowing, merging, and separator changes.
7. Cursor traversal that holds one leaf latch at a time.

Completion criteria:

- A reader and writer on unrelated leaves progress concurrently.
- Readers of a modified key wait through commit or rollback.
- Range scans do not observe uncommitted inserts or deletes.
- Splits and merges never expose an invalid parent/child relationship.
- No transaction-level global reader drain remains.

## Step 6: Add the Single-Node Server

Add:

- A network acceptor or event loop.
- A bounded connection worker pool.
- Versioned request and response framing.
- `Get`, `Scan`, and atomic `WriteBatch` requests.
- Request, key, value, and scan limits.
- Deadlines and cancellation.
- Connection-owned transaction cleanup.

At the end of this step, StoneleafDB is a client/server database but is not yet
replicated.

Completion criteria:

- Multiple clients read concurrently.
- A disconnected writer is rolled back.
- Malformed requests cannot reach storage internals.
- The server recovers completely before accepting connections.

## Step 7: Add One Raft Group

Raft entries contain operations-only logical batches:

```text
WriteBatch {
    Put(...)
    Delete(...)
}
```

Implement:

- Persistent Raft term, vote, and log.
- Leader election.
- Log replication.
- Majority-based commitment.
- Logical command encoding.
- Ordered local application.
- Atomic persistence of data and `last_applied_raft_index`.
- Leader-only ReadIndex reads.

Completion criteria:

- A three-node group preserves acknowledged writes after one node fails.
- Every healthy replica has identical logical key-value contents.
- A node crashing midway through local apply recovers and reapplies safely.

Client request deduplication is deferred. Adding it requires an explicit Raft
entry-format migration beyond the operations-only format.

## Step 8: Add Snapshots and Hardening

Implement:

- A database checkpoint associated with Raft index `N`.
- Snapshot packaging and checksums.
- Snapshot transfer and installation.
- Raft log compaction.
- Recovery from a snapshot followed by remaining Raft entries.
- Fault injection for process crashes, torn WAL writes, torn pages, slow disks,
  network partitions, and repeated recovery crashes.
- Throughput, latency, write-amplification, and recovery-time benchmarks.

Completion criteria:

- A node with no retained Raft history installs a snapshot and catches up.
- Randomized histories converge to identical ordered scans on every replica.
- One-node failure does not lose an acknowledged write.

## Deferred Work

Do not add these while building the first replicated database:

- Sharding.
- Cross-shard transactions.
- Multiple local writers.
- MVCC.
- Follower reads.
- Parallel Raft application.
- WAL delta optimization.
- Nonblocking fuzzy checkpoints.
- Online snapshot creation.

Each item introduces a separate correctness problem. None is necessary to prove
the first complete architecture.

## Immediate Restart Point

The standalone typed WAL and the active V2 page cutover are complete. Store,
Index, Segment, and Log now own
segment discovery, recovery, dense LSN assignment, rollover, lookup,
synchronization, and durability tracking. WAL records use the checksummed
40-byte typed envelope, and the higher layer has typed payload codecs,
factories, and `PendingBTreeAction`. PCache and Pager now own `PageV2` frames,
validate full-page checksums and encoded page numbers on reads, assign page
kinds, and expose pinned frames to B+ tree callers. B+ tree layouts use the
4072-byte page-kind payload while the rollback journal remains authoritative.

The next implementation task is to thread `PendingBTreeAction` through
KeyStore, B+ tree propagation, and every Pager mutation path.
Pager collects exact page effects and keeps affected frames WAL-pending until
`Log::append` assigns the action LSN. Run this shadow logging beside the
rollback journal before enforcing WAL-before-data or cutting recovery over.

Do not begin the network server, thread-safe buffer pool, or Raft while these
tasks are incomplete.

## Relevant Documents and Interfaces

- `README.md`: project direction and high-level status.
- `Docs/Technical Design Docs/Replicated KV Store Roadmap.md`: detailed target
  structures, WAL layouts, recovery, and Raft design.
- `Docs/Technical Design Docs/Pager + Cache.md`: current pager and rollback
  journal behavior.
- `Docs/Technical Design Docs/Tree_Module.md`: B+ tree split and merge rules.
- `include/KeyStore.h`: implemented logical key-value boundary.
- `include/Pager.h`: current embedded pager interface.
- `include/LockMgr.h`: current process-oriented lock manager that will be
  replaced for internal server coordination.
