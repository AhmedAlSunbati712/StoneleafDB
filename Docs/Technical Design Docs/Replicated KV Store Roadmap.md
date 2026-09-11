# Replicated Key-Value Store Roadmap

## Purpose

> **Authority note:** The detailed one-writer WAL payloads, page generations,
> private-page phases, and physical before-image undo later in this roadmap are
> historical staging material. The current local transaction and recovery
> contracts are defined by `Transaction + Lock Manager.md` and
> `V2 Page Format + WAL Mutation Path.md`: compound full-page after-image redo,
> logical key-level undo, CLRs, no page generation, and redo before undo. When
> this roadmap conflicts with either focused design, the focused design wins.

StoneleafDB is moving from an embedded, SQLite-inspired storage library toward a
single-group replicated key-value server. The target is deliberately staged:

1. Make the existing storage engine safe inside one multithreaded server.
2. Replace rollback journaling with a STEAL / NO-FORCE WAL and restartable
   ARIES-style recovery.
3. Put a deterministic key-value command layer above the local engine.
4. Replicate those commands through one Raft group.

Sharding, cross-shard transactions, follower reads, and multi-writer execution
are not part of this roadmap. They should be considered only after one Raft
group has strong local recovery, deterministic application, snapshots, and
repeatable failure tests.

This document is intentionally grounded in commit `859cb3b` on `main`. At that
commit, a clean `make test` runs 114 unit tests and 48 integration tests
successfully.

## Current Implementation

The repository already contains substantially more than a pager prototype.

### Implemented

- A 4 KiB page format and database header containing the file-change counter,
  page count, freelist state, and B+ tree root.
- A pager that opens or creates databases, pins and unpins pages, allocates and
  frees pages, and keeps file and cache state coherent.
- A 64-frame page cache using a hash table plus an unpinned-page LRU list.
- Dirty-page tracking with in-memory before-images.
- A checksummed rollback journal with multiple sections, durable journal
  publication, database-page flushing, hot-journal recovery, and database-file
  truncation after failed page allocation.
- SQLite-style `NOLOCK`, `SHARED`, `RESERVED`, internal `PENDING`, and
  `EXCLUSIVE` transitions implemented with POSIX advisory file locks.
- Cross-process cache invalidation using the database file-change counter.
- A B+ tree with typed keys and values, insertion, overwrite, deletion,
  splitting, borrowing, merging, root replacement, and ordered cursors.
- A typed local `KeyStore` boundary with automatic and explicit write
  transactions, point operations, range and prefix scans, and cursor lifecycle
  enforcement.
- Unit and integration coverage for the codecs, disk I/O, cache, pager,
  multiprocess locking, recovery, B+ tree, KeyStore, and cursor lifecycle.

### Partially Implemented

- The pager already follows the useful high-level progression of allowing a
  reserved writer, blocking new readers during promotion, and requiring
  exclusive access before database-page writes. That behavior is currently
  process-oriented and is not thread-safe.

### Missing

- A server runtime, connection handling, and request protocol.
- Thread-safe transaction locks, page-cache access, pins, frame latches, and
  cursor ownership.
- Persistent page LSNs, WAL records, WAL segments, WAL checksums, and a master
  checkpoint record.
- A transaction table, dirty-page table, compensation log records, and
  analysis/redo/undo recovery.
- A background writer, checkpointer, group WAL flushing, and WAL recycling.
- Raft, replicated command encoding, applied-index persistence, request
  deduplication, and snapshot transfer.

## Target Node Architecture

Each StoneleafDB node is one process. Threads share ordinary heap memory; there
is no interprocess buffer pool.

```text
StoneleafDB node
├── acceptor / network event loop
├── bounded connection worker pool
├── Raft runtime
├── Raft apply thread (the only storage writer)
├── WAL writer thread
├── background page writer
└── checkpointer

Shared engine state
├── key-value service
├── B+ tree
├── transaction manager
├── thread lock manager
├── buffer pool
├── WAL manager
├── transaction table
└── dirty-page table
```

Connection workers validate and encode requests. A worker may execute a read,
but it never directly applies a replicated write to the B+ tree. Writes are
proposed to Raft and reach the storage engine through the ordered apply thread.
This naturally preserves the first-version one-writer rule.

The server takes an exclusive filesystem ownership lock on
`<database>.lock` when it opens the database and holds it for its lifetime.
That lock prevents another server or an offline mutation tool from opening the
same database. It is not acquired, promoted, or released by individual
transactions. All transaction coordination inside the server uses mutexes,
condition variables, and latches.

## Thread-Safe Locking

### File-Locking Boundary

The existing POSIX byte-range lock manager is retired from the pager's normal
read, write, commit, rollback, and recovery paths. Traditional `fcntl` record
locks coordinate processes; threads within one process share process-level
lock ownership and therefore cannot use those locks to represent independent
transactions reliably.

File locking remains only as a server-ownership boundary:

```cpp
class DatabaseOwnershipLock {
  public:
    OwnershipResult acquire(const std::string &database_path);
    void release();

  private:
    int lock_file_fd = -1;
};
```

The ownership lock follows this lifecycle:

```text
open <database>.lock
    -> acquire one non-blocking exclusive OS lock
    -> open database, control file, and WAL
    -> complete recovery
    -> start Raft and connection workers
    -> serve until orderly shutdown
    -> stop workers and close storage
    -> release ownership lock
```

Failure to acquire the ownership lock fails server startup with
`DatabaseAlreadyOpen`. An abnormal process exit releases the OS lock
automatically; the next server still performs WAL recovery before serving.
The lock file contains no authoritative recovery state and must never be used
to decide whether recovery is necessary.

Because one server owns one authoritative buffer pool, v2 also removes the
cross-process cache-purge protocol. The current `file_change_counter` is not
carried into the v2 database header. A commit epoch remains for observability
and future snapshot semantics, but cache validity no longer depends on reading
metadata back from the database file.

### Ownership

Internal locks belong to transaction IDs, not process IDs or thread IDs. A
transaction may eventually resume on another worker, and rollback or
cancellation must identify the transaction independently of its current
thread.

```cpp
using TransactionId = std::uint64_t;

enum class LockLevel : std::uint8_t {
    Unlocked = 0,
    Shared,
    Reserved,
    Pending,
    Exclusive,
};

struct TransactionLockState {
    TransactionId transaction_id;
    LockLevel level = LockLevel::Unlocked;
};

struct GlobalLockState {
    std::mutex mutex;
    std::condition_variable changed;

    std::unordered_map<TransactionId, std::uint32_t> reader_holds;
    std::optional<TransactionId> reserved_writer;
    std::optional<TransactionId> pending_writer;
    std::optional<TransactionId> exclusive_writer;

    bool recovering = false;
    bool shutting_down = false;
};
```

`reader_holds` records ownership, not merely a global count. This permits
cleanup on request cancellation and catches an unmatched unlock. The sum of
its values is the active reader count.

### State Transitions

```text
Read transaction:
    UNLOCKED -> SHARED -> UNLOCKED

Write transaction:
    UNLOCKED -> RESERVED -> PENDING -> EXCLUSIVE
                                      ├── COMMIT -> UNLOCKED
                                      └── ABORT  -> UNLOCKED
```

- `SHARED` is granted only when there is no pending or exclusive writer.
- Only one transaction may own `RESERVED`.
- A reserved writer coexists with existing and new shared readers while all
  modified pages remain transaction-private.
- Promotion first installs `PENDING`. New readers then wait, preventing writer
  starvation.
- Promotion waits until all other reader holds reach zero. It must not wait
  while holding a buffer-directory latch or frame-content latch.
- After the first transition to `EXCLUSIVE`, the transaction cannot downgrade
  or admit readers. It holds exclusive ownership through durable commit or
  complete rollback.
- Condition-variable waits accept a cancellation token and optional deadline.
  Cancellation of a writer triggers rollback rather than silently releasing
  its locks.

Long reads and scans can delay a writer at `PENDING`. That is an accepted
first-version limitation. MVCC is the future mechanism for removing this
reader-drain requirement; it is not part of the first replicated engine.

### Locks, Latches, and Pins

These concepts must remain separate:

- A transaction lock controls logical visibility and isolation.
- A buffer-directory latch protects the page-to-frame mapping and replacement
  metadata.
- A frame-content latch protects the bytes and recovery metadata in one frame.
- A pin prevents frame reassignment while a caller holds a page handle.

The required lock order is transaction lock, buffer-directory latch, then
frame-content latch. No disk or WAL wait may occur while holding the
buffer-directory latch.

The in-memory manager is the sole authority for transaction lock state. The
server must not combine its decisions with the old per-pager file-lock state;
running both mechanisms would create two unsynchronized sources of truth.

## Transaction and Buffer Structures

```cpp
enum class TransactionStatus : std::uint8_t {
    Running = 0,
    Committing,
    Aborting,
    Complete,
};

struct TransactionContext {
    TransactionId transaction_id;
    TransactionStatus status;
    LockLevel lock_level;
    Lsn first_lsn;
    Lsn last_lsn;
    std::optional<RaftPosition> raft_position;
    std::unordered_map<PageId, PrivatePage> private_pages;
};
```

The private-page map preserves the current ability to let readers continue
while a reserved writer works. Each private page records the committed source
generation, a before-image, a mutable after-image, and whether it has already
been installed in the shared pool.

```cpp
enum class FrameState : std::uint8_t {
    Free = 0,
    Loading,
    Valid,
    Evicting,
};

struct BufferFrame {
    alignas(64) std::array<char, 4096> bytes;
    PageId page_id;
    std::uint32_t page_generation;
    std::atomic<std::uint32_t> pin_count;
    std::shared_mutex content_latch;
    FrameState state;
    bool dirty;
    std::optional<TransactionId> dirty_owner;
    Lsn page_lsn;
    Lsn rec_lsn;
    std::uint64_t usage_count;
};
```

The buffer pool uses frame IDs internally and a `PageId -> FrameId` hash table.
Callers receive RAII `BufferHandle`, `ReadPageGuard`, or `WritePageGuard`
objects rather than an unprotected `char *`. A guard pins before releasing the
directory latch, takes the appropriate content latch, and reverses those steps
on destruction. Replacement uses CLOCK rather than a globally mutated LRU
list.

## STEAL / NO-FORCE Write Flow

### Reserved Phase

1. The transaction obtains `RESERVED`, making it the only writer.
2. Reads by the writer check its private-page map before the committed buffer
   pool.
3. Writes copy committed pages into that private map and mutate the copies.
4. Other transactions continue reading committed shared frames.
5. An abort in this phase discards the private pages; no database undo is
   necessary.

### Exclusive Phase

The transaction enters this phase when it is ready to commit or when private
workspace pressure requires STEAL.

1. Install `PENDING`, block new readers, and wait for existing readers.
2. Enter `EXCLUSIVE` without holding page latches.
3. Generate WAL records for private changes and install their after-images into
   shared frames.
4. Mark the frames dirty with the transaction ID, `pageLSN`, and `recLSN`.
5. Allow uncommitted frames to be flushed only while this transaction remains
   exclusive.
6. Continue any remaining transaction work against the shared frames.
7. Commit durably or perform complete logged rollback before releasing
   exclusive ownership.

The background writer normally chooses committed dirty frames. It may flush an
uncommitted frame only if that frame belongs to the current exclusive writer.
An eviction request must never independently promote a transaction; it signals
the owning transaction to enter its exclusive phase.

### WAL-Before-Data Rule

Before issuing any database-page write:

```text
durableWAL >= frame.pageLSN
```

The page writer groups candidates, finds their maximum `pageLSN`, waits for one
WAL flush through that LSN, and then issues all eligible page writes. This is
not one WAL sync per page.

If a frame changes while a copied page is being written, the writer clears
`dirty` only if the current frame `pageLSN` still equals the copied
`pageLSN`. Otherwise the newer version remains dirty with its original
`recLSN` or a newly installed `recLSN`, as appropriate.

### Commit

1. Append `TXN_COMMIT` after every update record.
2. Wait until the WAL is durable through the commit LSN.
3. Mark the transaction committed in memory.
4. Make every installed frame ownerless but still dirty.
5. Release `EXCLUSIVE` and wake readers.
6. Acknowledge the local apply.

Database pages are not forced at commit. This is the NO-FORCE property.

### Abort

1. Set the transaction status to `Aborting` while retaining `EXCLUSIVE`.
2. Walk backward from `lastLSN` using `prevLSN`.
3. Apply each undo and append a compensation log record.
4. Give each restored frame the CLR's LSN and keep it dirty.
5. Append `TXN_END` after rollback completes.
6. Release exclusive ownership only after the shared cache again represents a
   committed database state.

Restored pages need not be forced before readers resume. They may be evicted
only through the ordinary WAL-before-data path, which writes the restored
version before reusing their frames.

## StoneleafDB V2 File Format

The WAL redesign introduces an incompatible format. Existing files are
rejected with `UnsupportedFormat`; automatic migration is deferred.

All persistent integers are big-endian. Checksums use CRC32C. Reserved bytes
must be written as zero and ignored by readers. File and record decoders reject
unknown required flags but tolerate zero-filled extensions covered by a larger
declared header size.

### Common Page Header

Every database page remains exactly 4096 bytes. The first 32 bytes are reserved
for recovery metadata.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Page magic |
| 4 | 2 | Format version (`2`) |
| 6 | 2 | Header bytes (`32`) |
| 8 | 4 | Page ID |
| 12 | 4 | Page generation |
| 16 | 8 | `pageLSN` |
| 24 | 4 | CRC32C |
| 28 | 2 | Page kind |
| 30 | 2 | Flags |

The CRC covers all 4096 bytes with the CRC field treated as zero. Page kinds
are database metadata, freelist, B+ tree internal, and B+ tree leaf. The page
generation increments whenever a freed page ID is reused, preventing an old
WAL record from being applied to a different logical incarnation.

The usable B+ tree payload becomes 4064 bytes. `BTREE_ORDER` must be recomputed
from the encoded cell limits rather than retained as the current hard-coded
value.

### Database Metadata Page

Page zero uses the common page header and starts this payload at byte 32:

| Offset | Size | Field |
| ---: | ---: | --- |
| 32 | 16 | `StoneleafDB v2` magic, NUL-padded |
| 48 | 16 | Database UUID |
| 64 | 4 | Page size (`4096`) |
| 68 | 4 | Database page count |
| 72 | 4 | Freelist head page ID |
| 76 | 4 | Freelist page count |
| 80 | 4 | B+ tree root page ID |
| 84 | 4 | Format flags |
| 88 | 8 | Commit epoch |
| 96 | 8 | Last applied Raft index |
| 104 | 8 | Last applied Raft term |
| 112 | 3984 | Reserved |

The metadata page is updated through ordinary WAL records. Updating the Raft
position in the same local transaction as the key-value changes makes apply
progress atomic with state-machine progress.

### Control File

`<database>.control` contains two 4096-byte slots. Each slot contains:

- Magic and control-format version.
- Database UUID.
- Monotonic control generation.
- WAL timeline.
- Completed checkpoint begin LSN.
- Completed checkpoint record LSN.
- CRC32C and zero-filled reserved bytes.

The checkpointer writes and synchronizes the older slot, then considers the
new generation published. Recovery chooses the valid slot with the greatest
generation. If one slot is torn, the other remains usable. A UUID mismatch
between the database, control file, and WAL is fatal.

A new database initializes both slots with checkpoint LSN zero. Zero means
there is no completed checkpoint, so recovery starts at offset 4096 in WAL
segment zero. The first completed sharp checkpoint replaces that bootstrap
sentinel with a real checkpoint position.

## WAL Storage

### Segments and LSNs

WAL lives in `<database>.wal/`. Segments are 64 MiB and are named by timeline
and zero-padded segment number. The first 4096 bytes of every segment contain:

- Eight-byte WAL magic.
- WAL format version and header size.
- Database UUID.
- Timeline.
- Segment number.
- Segment starting LSN.
- Database page size.
- CRC32C.
- Zero-filled reserved bytes.

An LSN is a 64-bit byte address in the logical WAL space:

```text
segment = LSN / 64 MiB
offset  = LSN % 64 MiB
```

Record offsets are at least 4096. Records are aligned to eight bytes and never
cross segment boundaries. If the next record does not fit, the writer
zero-fills the remaining tail, opens the next segment, writes its header, and
continues at offset 4096.

### WAL Manager State

```cpp
struct LogManagerState {
    std::mutex append_mutex;
    std::condition_variable written;
    std::condition_variable durable;

    Lsn next_lsn;
    Lsn written_lsn;
    Lsn durable_lsn;
    TimelineId timeline;
    std::vector<std::byte> buffer;
};
```

Appending reserves a contiguous LSN range and copies an already encoded record
into the WAL buffer. The WAL writer performs sequential writes and advances
`written_lsn`. A storage sync advances `durable_lsn` and wakes every waiter
whose requested LSN is satisfied. Multiple commits and page flushes share one
sync when their waits overlap.

### Common Record Header

Every record begins with this 64-byte header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Record magic |
| 4 | 2 | WAL format version (`1`) |
| 6 | 2 | Record type |
| 8 | 4 | Flags |
| 12 | 4 | Total aligned record bytes |
| 16 | 4 | Exact payload bytes |
| 20 | 4 | CRC32C |
| 24 | 8 | Record LSN |
| 32 | 8 | Transaction ID, or zero |
| 40 | 8 | `prevLSN`, or zero |
| 48 | 4 | Page ID, or zero |
| 52 | 4 | Page generation, or zero |
| 56 | 8 | Reserved |

CRC32C covers the header with its CRC field treated as zero plus the exact
payload. Alignment padding is zero and is not included in the checksum. The
scanner validates magic, version, lengths, alignment, expected LSN, and CRC
before exposing a record.

The first implementation assigns these record type values:

| Value | Type |
| ---: | --- |
| 1 | `TXN_BEGIN` |
| 2 | `PAGE_UPDATE_FULL` |
| 3 | `PAGE_UPDATE_DELTA` |
| 4 | `PAGE_INIT` |
| 5 | `PAGE_ALLOC` |
| 6 | `PAGE_FREE` |
| 7 | `TXN_COMMIT` |
| 8 | `TXN_ABORT` |
| 9 | `TXN_END` |
| 10 | `CLR` |
| 11 | `CHECKPOINT_BEGIN` |
| 12 | `CHECKPOINT_END` |
| 13 | `CHECKPOINT_COMPLETE` |

## WAL Record Payloads

### `TXN_BEGIN`

```text
u64 start_epoch
u64 raft_index       // zero for a non-Raft maintenance transaction
u64 raft_term        // zero when raft_index is zero
u32 transaction_flags
u32 reserved
```

The record's `prevLSN` is zero. The transaction table sets both `firstLSN` and
`lastLSN` to this record.

### `PAGE_UPDATE_FULL`

```text
byte before_page[4096]
byte after_page[4096]
```

The common header identifies the page and generation. Before encoding the
after-image, the pager sets its `pageLSN` to this record's assigned LSN and
recomputes the page checksum. The before-image is the exact previous page
version, including its previous page LSN and checksum.

This is the first implementation because it maps directly to the pager's
existing before-image bookkeeping and makes physical undo straightforward.
Only one page update for the final private version is needed when entering the
exclusive phase. Additional updates after that phase require additional
records.

### `PAGE_UPDATE_DELTA`

```text
u16 span_count
u16 reserved
u32 encoded_span_bytes

repeated span_count times:
    u16 page_offset
    u16 before_length
    u16 after_length
    u16 span_flags
    byte before[before_length]
    byte after[after_length]
```

Spans are ordered, non-overlapping, and contained within the 4064-byte payload
region. The page header is not encoded as an ordinary span: redo sets
`pageLSN` to the update record, undo sets it to the CLR LSN, and both paths
recompute the page checksum. The full-image and delta record types remain
valid together so large or fragmented changes can continue using full images.

### `PAGE_INIT`

```text
u16 new_page_kind
u16 init_flags
u32 reserved
byte after_page[4096]
```

This initializes a newly allocated generation. Undo is driven by the matching
allocation record rather than by treating uninitialized bytes as a valid old
page.

### `PAGE_ALLOC`

```text
u32 previous_database_page_count
u32 previous_freelist_head
u32 previous_freelist_count
u32 allocation_source       // 1 = file extension, 2 = freelist
u32 allocated_page_id
u32 allocated_generation
u32 previous_page_generation
u32 reserved
```

Redo makes the generation allocated and ensures the database is long enough.
Undo either restores the freelist linkage or makes a tail allocation
reclaimable. Physical truncation is permitted only when every page above the
target belongs to the loser transaction; otherwise undo returns pages to the
freelist.

### `PAGE_FREE`

```text
u32 previous_freelist_head
u32 previous_freelist_count
u32 freed_page_id
u32 freed_generation
byte before_page[4096]
```

Redo links the page into the freelist using the page updates generated by the
operation. Undo restores the live page image and prior freelist metadata. A
page generation is incremented on the next allocation, not merely on free.

### `TXN_COMMIT`

```text
u64 commit_epoch
u64 raft_index
u64 raft_term
u32 commit_flags
u32 reserved
```

Commit becomes durable when the WAL is synchronized through the end of this
record. The record carries the Raft position as a recovery cross-check; the
authoritative applied position is also stored in database page zero within the
same transaction.

### `TXN_ABORT` and `TXN_END`

`TXN_ABORT` contains a 32-bit reason code and reserved bytes. It marks the
decision to roll back but is not proof that rollback finished. `TXN_END` has no
payload and removes the transaction from the transaction table. A committed
transaction may write `TXN_END` asynchronously after its commit record is
durable.

### `CLR`

```text
u64 undo_next_lsn
u64 compensated_lsn
u16 compensation_kind      // full update, delta, alloc, free, or init
u16 compensation_flags
u32 redo_bytes
byte redo_of_undo[redo_bytes]
```

A CLR's normal `prevLSN` links it to the transaction's prior log record. Its
`undo_next_lsn` points to the next record that still needs undo. The embedded
redo payload uses the corresponding page-operation encoding. CLRs are redone
but never undone.

### Checkpoint Records

`CHECKPOINT_BEGIN` has no payload. Its LSN is the checkpoint ID.

One or more `CHECKPOINT_END` records contain:

```text
u64 checkpoint_id
u32 chunk_number
u32 chunk_flags            // LAST_CHUNK when this is the final chunk
u32 transaction_count
u32 dirty_page_count

transaction entries:
    u64 transaction_id
    u8 transaction_status
    byte reserved[7]
    u64 first_lsn
    u64 last_lsn

dirty-page entries:
    u32 page_id
    u32 page_generation
    u64 rec_lsn
```

`CHECKPOINT_COMPLETE` contains the checkpoint ID, final checkpoint-end LSN,
and chunk count. Only after this record is durable does the checkpointer publish
the checkpoint in the alternate control-file slot.

## Physical and Logical Logging Boundary

The local WAL is physical or physiological. The initial implementation logs
complete page before- and after-images. The optimized form names a physical
page and records byte spans within that page.

This is safe for the first engine because there is exactly one writer, the
writer holds exclusive access before shared pages can contain uncommitted
state, and recovery undoes that writer's changes in reverse LSN order. B+ tree
splits, merges, root changes, allocations, and freelist edits are all restored
physically rather than being left behind as nested top actions.

If StoneleafDB later adds concurrent writers, structural B+ tree operations
should become redo-only system transactions and user-level undo should become
logical by key. That is explicitly not required for the one-writer roadmap.

The Raft log is logical. It carries key-value operations, not page IDs, page
images, local LSNs, or local allocation choices. Physical WAL files are never
sent to other nodes.

## Dirty-Page and Transaction Tables

### Transaction Table

```cpp
struct TransactionTableEntry {
    TransactionStatus status;
    Lsn first_lsn;
    Lsn last_lsn;
    std::optional<RaftPosition> raft_position;
};
```

The table contains transactions that have begun but have not written
`TXN_END`. The one-writer rule limits ordinary running writers to one, but the
format and recovery algorithm do not depend on that shortcut.

### Dirty-Page Table

```cpp
struct DirtyPageTableEntry {
    PageId page_id;
    std::uint32_t page_generation;
    Lsn rec_lsn;
};
```

`recLSN` is the first update that made the page dirty after its latest
successful flush. Later updates advance `pageLSN` but do not change `recLSN`.
After a page image is written, it leaves the table only if the current frame
still has the written generation and `pageLSN`; otherwise it remains dirty.

## Recovery

Recovery completes before the server accepts network requests or starts Raft
application.

### WAL Scan

1. Read the newest valid control slot and verify its database UUID.
2. Open the referenced timeline and checkpoint segment.
3. Validate segment headers and records in LSN order.
4. Stop at an invalid or incomplete record only when it is in the unsynchronized
   tail. An invalid record before a known durable boundary is corruption.
5. Remove or ignore bytes after the last valid record before reopening WAL for
   append.

### Analysis

1. Start at the checkpoint begin LSN referenced by the control file.
2. Load transaction-table and dirty-page-table entries from the completed
   checkpoint chunks.
3. Scan forward, updating transaction status and `lastLSN`.
4. Insert a dirty page with the first update LSN observed after it was absent;
   preserve its existing `recLSN` on later updates.
5. Identify committed winners, completed transactions, and running or aborting
   losers.

### Redo

1. Start at the minimum `recLSN` in the dirty-page table.
2. Repeat history, including updates from loser transactions and CLRs.
3. For a page record, skip when the page is absent from the dirty-page table,
   the record precedes its `recLSN`, the generation differs, or the persistent
   `pageLSN` is greater than or equal to the record LSN.
4. Otherwise apply redo, set `pageLSN`, recompute the page checksum, and mark
   the recovered frame dirty.

Repeating loser history before undo reconstructs the exact state that existed
at the crash.

### Undo

1. Put each loser transaction's `lastLSN` in a max-LSN priority queue.
2. Pop the greatest LSN so undo proceeds globally backward.
3. For an ordinary undoable record, apply its before-state and append a CLR
   whose `undoNextLSN` is that record's `prevLSN`.
4. For a CLR, continue at its `undoNextLSN`.
5. At `TXN_BEGIN`, append `TXN_END` and remove the transaction.
6. Flush recovery-generated WAL before permitting any recovered page carrying
   its LSN to reach the database file.

If the node crashes during undo, the next recovery redoes the CLRs and resumes
from their `undoNextLSN` values.

## Checkpointing and WAL Recycling

### First Checkpoint

The first implementation uses a sharp checkpoint to establish correctness:

1. Block new applies and obtain exclusive engine access.
2. Flush WAL through the chosen checkpoint target.
3. Flush every committed dirty page and synchronize the database file.
4. Write and synchronize the checkpoint record sequence.
5. Publish the alternate control slot.
6. Resume apply.

With no active writer and an empty dirty-page table, WAL before the checkpoint
can be recycled.

### Fuzzy Checkpoint

The later checkpointer does not stop readers or the writer while collecting its
snapshot:

1. Append `CHECKPOINT_BEGIN`.
2. Copy the transaction and dirty-page tables under their short metadata
   latches.
3. Append chunked `CHECKPOINT_END` records and `CHECKPOINT_COMPLETE`.
4. Synchronize WAL and publish the control slot.
5. Let the background writer advance the recoverable horizon.

Local WAL segments can be removed only below all applicable horizons:

- The minimum dirty-page `recLSN`.
- The earliest log record still required to undo an active transaction.
- The completed checkpoint referenced by the control file.
- Any local backup or diagnostic retention requirement.

Raft follower progress does not retain the local physical WAL because Raft
replicates a separate logical log.

## Replicated Command Format

StoneleafDB initially exposes leader-only `Get`, `Scan`, and atomic
`WriteBatch`. Interactive distributed transactions are deferred.

The Raft entry stores its term and dense index followed directly by an
operations-only logical batch. Each operation begins with:

```text
u8 operation_type       // 0 = Put, 1 = Delete
u8 key_type
u32 key_size
byte key[key_size]
// Put only:
u8 value_type
u32 value_size
byte value[value_size]
```

Keys and values use the existing canonical StoneleafDB type-tagged codecs.
Decoding validates operation counts, lengths, logical types, key/value formats,
and trailing bytes before application. A committed command must never be
silently skipped. Client request deduplication is deferred and will require an
explicitly designed entry-format migration.

## Raft Write and Apply Path

```text
client WriteBatch
    -> leader validates the logical mutations
    -> leader appends logical command to Raft
    -> command reaches durable majority
    -> Raft marks entry committed
    -> each node's apply thread receives entries in index order
    -> local ARIES transaction applies the batch
    -> applied index commits atomically with the mutations
    -> leader returns the command result
```

The leader replies only after both quorum commitment and local application.
This simplifies result delivery and guarantees that a successful response can
be served by the leader's current state. Without request deduplication, a lost
response remains an unknown result and is not automatically retried as a new
write.

Initially, every node synchronizes its local commit WAL even though the logical
command is already durable in the Raft log. This double logging is accepted for
the correctness milestone. A later benchmarked optimization may rebuild a
node's local state from a snapshot plus Raft rather than forcing every local
commit, but it must not weaken WAL-before-data for pages that can be stolen.

### Apply Recovery

If a node crashes while locally applying Raft entry `N`:

- A durable local commit includes page zero with `last_applied = N`; the entry
  is not applied again.
- A missing local commit causes ARIES to undo the partial update. Page zero
  remains at `N - 1`, so Raft reapplies `N`.
- A crash during that undo is handled by CLRs before Raft application resumes.

The apply thread rejects gaps and requires:

```text
incoming_index == last_applied_index + 1
```

### Linearizable Reads

The first version serves linearizable reads only through the leader:

1. Obtain a Raft ReadIndex confirmed by a quorum in the current term.
2. Wait until local `last_applied_index` reaches that index.
3. Acquire `SHARED` and execute the B+ tree read or scan.

A long scan may delay the next writer's promotion to exclusive. Follower reads
and lease reads are future optimizations.

## Raft Snapshots

The first snapshot is deliberately sharp:

1. Pause Raft application at committed index `N`.
2. Complete a sharp local checkpoint and synchronize the database.
3. Package the database file with a manifest containing database UUID, v2
   format version, page size, file length, whole-file checksum,
   `lastIncludedIndex`, and `lastIncludedTerm`.
4. Resume application while snapshot transfer proceeds from the immutable
   package.

A follower installs into a temporary path, verifies the manifest and page
checksums, synchronizes the file and containing directory, atomically replaces
its inactive database, initializes a fresh local WAL timeline, and starts
applying at `lastIncludedIndex + 1`.

Raft log compaction may pass index `N` only after the snapshot is durable and
available for followers that can no longer receive earlier entries.

## Implementation Milestones

### Milestone 1: V2 Storage Format

- Add page, metadata-page, control-file, WAL segment, record, and command
  codecs with strict bounds and checksum validation.
- Recalculate B+ tree layout for the common page header.
- Reject old databases explicitly.
- Preserve current B+ tree behavior on newly created v2 databases.

Acceptance: all codec corruption tests pass; B+ tree insert/delete/reopen tests
pass on v2 pages; no WAL or server concurrency is enabled yet.

### Milestone 2: Multithreaded Local Engine

- Replace process-owned pager state with one server-owned engine and shared
  buffer pool.
- Add `DatabaseOwnershipLock`, acquire it once during server startup, and hold
  it until storage shutdown.
- Remove POSIX byte-range locking from pager operations and replace the current
  `LockMgr` implementation with the transaction-owned in-memory manager.
- Remove file-change-counter cache refresh and cache purge; all workers use the
  same authoritative buffer pool.
- Add transaction-owned lock state, condition-variable promotion, frame IDs,
  pins, latches, guards, and CLOCK replacement.
- Complete recovery before starting connection workers or Raft application.
- Keep the one-writer and reader-drain behavior.
- Adapt the completed `KeyStore` boundary to the shared, thread-safe local
  engine.

Acceptance: thread sanitizer runs clean; concurrent readers return stable
values; a reserved writer coexists with readers; pending blocks new readers;
exclusive promotion cannot starve; a second process cannot acquire server
ownership of the same database.

### Milestone 3: Full-Image WAL and Recovery

- Implement WAL segments, the WAL writer, full-page update records,
  transaction chaining, allocation/free records, and WAL-before-data.
- Replace rollback-journal commit and recovery with STEAL / NO-FORCE recovery.
- Implement transaction and dirty-page tables, CLRs, and analysis/redo/undo.
- Keep a sharp checkpoint for initial WAL reclamation.

Acceptance: deterministic crash injection at every WAL, page-write, commit,
abort, and recovery boundary always produces either the old committed state or
the new committed state.

### Milestone 4: Background Persistence

- Add the background writer, fuzzy checkpoints, double-buffered control file,
  WAL recycling, delta records, and group WAL flushes.
- Measure WAL bytes per transaction, syncs per second, dirty-page age,
  checkpoint duration, recovery time, and write throughput.

Acceptance: checkpoints do not block readers; recovery begins from the latest
completed checkpoint; recycling never removes required redo or undo; delta
logging improves bytes written without changing results.

### Milestone 5: Single-Node Server API

- Add bounded connection handling and versioned request/response framing.
- Expose `Get`, `Scan`, and atomic `WriteBatch` through the completed key-value
  layer.
- Add request size, key/value size, scan lifetime, timeout, and connection
  limits.

Acceptance: concurrent clients cannot bypass transaction ownership; malformed
requests cannot reach the pager; cancellation safely releases reads or rolls
back writes.

### Milestone 6: One Raft Group

- Replicate logical write batches, persist Raft state, and apply through one
  ordered thread.
- Persist the applied index in each local transaction.
- Add leader-only ReadIndex reads and retry-safe client responses.

Acceptance: three nodes retain acknowledged writes through any single-node
failure; leader replacement does not duplicate a request; recovering followers
converge to identical logical key-value contents.

### Milestone 7: Snapshots and Hardening

- Implement sharp Raft snapshot creation, transfer, verification,
  installation, and log compaction.
- Add network partitions, slow disks, torn WAL writes, torn page writes,
  repeated recovery crashes, and process termination to the fault suite.
- Establish latency, throughput, recovery-time, and disk-amplification
  baselines before considering more concurrency.

Acceptance: a node with no retained Raft history installs a snapshot and catches
up; randomized histories produce identical logical state on every healthy
replica.

## Required Test Matrix

### Locking and Cache

- A second server process cannot acquire the database ownership lock.
- Normal reader, writer, commit, and rollback operations issue no file-lock
  transitions after server startup.
- An abnormal server exit releases ownership, and the next server recovers
  before admitting requests.
- Many readers acquire and release shared ownership concurrently.
- One reserved writer coexists with readers and excludes a second writer.
- Pending blocks new readers and eventually reaches exclusive.
- Cancellation cleans reader ownership and rolls back the writer.
- Pins prevent eviction; generation checks prevent stale-handle reuse.
- Dirty-frame writeback retains dirty state when the frame changes during I/O.

### WAL and Recovery

- Every record type round-trips and rejects malformed lengths, flags, and CRC.
- Torn segment headers and torn final records stop at the safe WAL tail.
- A page write is never issued before its `pageLSN` is durable.
- Crash before commit undoes every stolen page.
- Crash after durable commit redoes every missing page without undoing it.
- PageLSN and generation checks make repeated redo idempotent.
- Crash during rollback redoes CLRs and resumes at `undoNextLSN`.
- Allocation/free/root/freelist changes recover atomically.
- Sharp and fuzzy checkpoint interruption leaves the previous control slot
  usable.
- WAL recycling respects dirty-page and active-transaction horizons.

### Raft

- Leader failure before majority commitment does not expose the write.
- Leader failure after commitment but before response returns the stored result
  when the client retries.
- Local crash midway through apply undoes and reapplies the committed entry.
- Duplicate requests mutate state once.
- ReadIndex waits for local application.
- Followers reject gaps and corrupted commands.
- Snapshot installation verifies UUID, version, checksum, term, and index.
- A three-node randomized test compares complete ordered scans after failures.

## Observability

Expose at least these node-local counters and gauges before performance tuning:

- Active readers and current writer lock level.
- Buffer hits, misses, pins, evictions, dirty frames, and forced STEAL entries.
- WAL bytes appended, written LSN, durable LSN, sync count, and sync latency.
- Dirty-page-table size and oldest `recLSN`.
- Checkpoint target, duration, pages written, and recyclable WAL bytes.
- ARIES analysis, redo, and undo durations plus records processed.
- Raft term, role, commit index, applied index, proposal latency, and snapshot
  index.

Assertions remain enabled in tests for lock ownership, page generation,
`pageLSN <= durableLSN` before page writes, contiguous apply indexes, and
transaction-table transitions.

## Sources

- C. Mohan et al., [ARIES: A Transaction Recovery Method Supporting
  Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead
  Logging](https://www.cs.cmu.edu/~15849g/readings/mohan92.pdf).
- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus
  Algorithm](https://raft.github.io/raft.pdf).
- PostgreSQL documentation, [Write-Ahead Logging
  Introduction](https://www.postgresql.org/docs/current/wal-intro.html).
- PostgreSQL documentation, [WAL
  Internals](https://www.postgresql.org/docs/current/wal-internals.html).
- PostgreSQL documentation, [Resource Consumption and Shared
  Buffers](https://www.postgresql.org/docs/current/runtime-config-resource.html).
