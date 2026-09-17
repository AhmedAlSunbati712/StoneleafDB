# V2 Page Format and WAL Mutation Path

## Status

This document records the chosen incremental path from the current rollback
journal to a physical write-ahead log. It is authoritative for the V2 page
header, the pager-to-B+ tree boundary, page mutation capture, the first logger,
the rollback-journal transition, and startup recovery sequencing.

The implementation remains on the rollback journal today. The V2 page
representation is now active in PCache, Pager, and B+ tree page codecs, while
the typed WAL and Log coordinator remain standalone. The mutation boundary,
WAL cutover, and WAL recovery executor remain future work.

`V2` is the name of this migration path and its new source files. It is not a
version number stored in the page bytes.

## Goals

- Keep every persistent database page exactly 4096 bytes.
- Add `pageLSN` and a page checksum before integrating WAL.
- Treat the cached 4096 bytes as the authoritative persistent representation;
  the runtime `page_num` mirror is only a cache lookup key.
- Let the B+ tree borrow pinned `PageV2` objects so operation-scoped latch
  ownership remains explicit, while `BTreePage` interprets their raw bytes.
- Replace `begin_write` with an operation-scoped mutation boundary that logs
  the complete physical after-images propagated by one B+ tree action.
- Build and validate the logger independently, then run it beside the rollback
  journal while pager paths are converted one at a time.
- Remove the rollback journal only after WAL-before-data, commit durability,
  startup recovery, and crash tests are complete.
- Do not add a periodic checkpointer during the first WAL cutover.
- Do not make a permanent single-writer assumption in the page format, logger,
  or mutation API. Concurrency control is a separate design task.

## Non-Goals for This Stage

- The concrete concurrent B+ tree latch-coupling algorithm.
- Periodic or fuzzy checkpoints.
- WAL reclamation or bounded startup time.
- Raft, networking, snapshots, or cross-process reader consistency during the
  transitional embedded-engine stage.
- Delta or physiological redo records.
- Automatic migration of existing database files.

## V2 Page Format

One page is exactly 4 KiB:

```text
4096-byte V2 page
├── bytes 0..23: common persistent header
└── bytes 24..4095: 4072-byte page-kind payload
```

This personal project has one current on-disk format. The page does not carry a
format version, header-size field, page generation, flags, or compatibility
padding. A format change updates the implementation and intentionally makes
older database files unsupported.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Page magic (`SLPG`) |
| 4 | 4 | Page number |
| 8 | 8 | `pageLSN` |
| 16 | 4 | CRC32C |
| 20 | 4 | Page kind |

Persistent integers are big-endian. Page number zero is valid and identifies
the database metadata page. LSN zero means that no WAL update has yet been
assigned. CRC32C covers the complete 4096-byte page with bytes 16 through 19,
the CRC field itself, treated as zero. Covering the header is required because
recovery must not trust a `pageLSN` from a torn page.

V2 page kinds are:

| Value | Kind |
| ---: | --- |
| 1 | `DatabaseMetadata` |
| 2 | `Freelist` |
| 3 | `BTreeInternal` |
| 4 | `BTreeLeaf` |

Zero and unknown kinds are invalid. A live leaf is not converted into an
internal page during ordinary root splitting: the B+ tree allocates a new
internal root. Kind changes normally occur during allocation, free, and reuse.

### Why There Is No Page Generation

V2 relies on monotonic LSN order plus logged allocation and initialization:

- A page number is never reused without `PAGE_ALLOC` and `PAGE_INIT` records.
- `PAGE_INIT` gives the reused page image its record LSN.
- Recovery processes retained physical records in increasing LSN order.
- Redo applies a record only when the persisted `pageLSN` is smaller than the
  record LSN.

An old-incarnation record is skipped when the new incarnation is already on
disk. If the old incarnation is still on disk, ordered redo eventually reaches
the later `PAGE_INIT` record and replaces it. A valid future checkpoint starts
after initialization records it no longer needs to retain.

### Payload Ownership

The common page codec treats bytes 24 through 4095 as opaque. Their meaning is
owned by the page-kind-specific subsystem:

- Page zero uses the database metadata payload codec.
- Freelist pages use the pager's freelist payload codec.
- Internal and leaf pages use the B+ tree page codec.

The exact V2 B+ tree and freelist payload layouts are separate decisions. The
common codec can be implemented and tested before those layouts are finalized.

## Raw Bytes Are the Persistent Source of Truth

The cached page duplicates only the page number as runtime cache identity. Its
serialized page number, kind, `pageLSN`, checksum, and payload remain encoded
in the 4096-byte image and are read and written through `V2PageCodec`.

The pager sets the runtime `page_num` after initializing a new page or
validating a disk-loaded page. It must equal the page number encoded in `data`.
The encoded value remains the persistent authority; the runtime mirror exists
to preserve the current cache's direct page-number lookup behavior.

```cpp
struct PageV2 {
    std::array<char, 4096> data{};

    // Runtime-only cache state. Never serialized.
    std::uint32_t page_num = 0;
    std::uint32_t refs_num = 0;
    bool is_dirty = false;
    bool need_flushing = false;
    std::shared_mutex latch;
};
```

The latch protects the cached page image for one short B+ tree operation. A
thread keeps the page referenced while waiting for or holding the latch,
unlocks before unreferencing, and never stores the latch in a transaction.
The first implementation uses `std::shared_mutex`; a fairer custom latch is
deferred until profiling demonstrates starvation.

`V2PageCodec` provides checked readers, writers, and whole-page validation:

```cpp
namespace V2PageCodec {
    std::uint32_t page_num(std::span<const char, 4096> page);
    std::uint64_t page_lsn(std::span<const char, 4096> page);
    V2PageKind page_kind(std::span<const char, 4096> page);

    void set_page_lsn(std::span<char, 4096> page, std::uint64_t lsn);
    void set_page_kind(std::span<char, 4096> page, V2PageKind kind);
    void update_checksum(std::span<char, 4096> page);
    V2PageCodecResult validate(std::span<const char> page);
}
```

The codec validates exact size, magic, page kind, and whole-page CRC32C before
the pager exposes a disk-loaded page. The pager separately compares the
decoded page number with the page number it requested from disk. Only after
whole-page validation succeeds may recovery use the encoded `pageLSN` for its
redo comparison.

## Pager-to-B+ Tree Boundary

The pager and cache retain ownership of every `PageV2`. The B+ tree may borrow
a pinned `PageV2*` so `BTreeOperation` can hold its shared or exclusive latch
and later unreference it through the pager. The pointer is invalid after that
reference is released and must never be stored in a transaction.

`BTreePage` receives the complete `PageV2::data` span. It uses `V2PageCodec` to
read and validate the common page kind from those bytes, then interprets bytes
24 through 4095 as its B+ tree payload. It can change the page kind through the
codec when a deliberate reinitialization requires that. The pager does not
pass page number, page kind, `pageLSN`, CRC, and payload as separate values.

This does not remove checking responsibilities from `BTreePage`.
Common-header validation by the pager is additive; `BTreePage` still rejects
an invalid B+ tree kind, malformed cell layout, impossible offsets, and other
B+ tree page corruption.

The full-page span is non-owning. Its writes modify the cached bytes directly,
and it becomes invalid when the owning pin or mutation ends.

The existing `BTreePage` assumes its B+ tree type byte is at offset zero, so it
cannot consume the V2 page unchanged. Its V2 adaptation must preserve the
4096-byte interface while shifting page-specific decoding to byte 24 and
recalculating capacity. That remains a later, explicit migration step; the
standalone page codec does not modify the legacy B+ tree.

## Page Mutation Boundary

`begin_mutation` is the intended replacement for `begin_write`. During the
incremental migration, both entry points coexist so unconverted code keeps the
rollback-journal behavior unchanged. Each converted call site uses a complete
`begin_mutation`/`finish_mutation` pair; after the last conversion,
`begin_write` is removed.

### Operation Mutation Batch

The WAL unit is one complete user-visible B+ tree action, not an independently
undoable page replacement. One insert, update, or delete may propagate through
a leaf, one or more internal pages, newly allocated pages, and database
metadata. The operation therefore owns a mutation batch containing every page
it changes.

```cpp
class OperationMutationBatch {
  public:
    PageMutationV2 &begin_mutation(PageNumber page_num);
    WalAppendResult finish(UndoDescriptor undo);
    void cancel();

  private:
    Transaction &transaction;
    std::vector<PageMutationV2> mutations;
};
```

Each `PageMutationV2` keeps the page pinned, exposes its cached 4096 bytes, and
owns one transient before-image plus prior runtime dirty state. The before-image
is only for cancelling an operation that has not successfully appended its WAL
record. It is not persisted as transaction undo information.

The operation batch prevents its pages from being evicted or modified by a
second physical operation until the compound WAL append completes. Page
latches are held only across this in-memory operation and WAL append, never
across WAL synchronization, transaction commit, or client think-time.

### Beginning a Page Mutation

`begin_mutation(page_num)` within a batch:

1. Finds, pins, and exclusively latches the cached page.
2. Copies its current 4096 bytes into transient scratch memory.
3. Records the prior dirty and flushability state.
4. Returns the mutable full-page span used by the B+ tree.

Touching the same page again in the same batch reuses its existing mutation;
the compound record needs only that page's final after-image. The batch keeps
the first scratch image so complete cancellation returns the cache to the
state before the logical action began.

### Finishing an Operation

After the B+ tree completes all propagated changes, `finish(undo)`:

1. Counts the affected pages and determines the compound record size.
2. Copies every final page content image into the pending action payload. The
   encoded image's `pageLSN` and checksum are recovery-owned fields rather than
   values trusted during replay.
3. Encodes the logical undo descriptor and all physical effects.
4. Calls `Log::append`, which assigns and returns the record LSN.
5. Sets the transaction's `lastLSN` to the returned LSN.
6. Writes that LSN into every affected cached page and recomputes its checksum.
7. Marks the affected frames dirty and flushable.
8. Releases the operation's page latches and pins.

All affected pages use the compound action LSN as `pageLSN`. A page cannot be
written while the action is incomplete. Once it is flushable, the ordinary
spill path first makes WAL durable through that shared action LSN.

Appending copies the complete record before `finish` returns. Redo applies the
recorded page content, assigns the enclosing record's LSN as `pageLSN`, and
recomputes the whole-page checksum. Append does not synchronize WAL; commit
and cache-spill paths perform durability waits. The append path must reserve
required memory before modifying pages so a full WAL buffer cannot force disk
I/O while B+ tree latches are held.

If preparation fails before physical append begins, the batch restores every
transient before-image and its prior runtime state before releasing pins and
latches. An unfinished batch destructor performs the same cancellation. An
ambiguous physical append failure instead places the engine in
recovery-required state. After append succeeds, later transaction abort uses
logical undo rather than the discarded scratch images.

### Propagated Structural Change

Inserting one key may split a leaf, propagate a separator, split an internal
page, create a root, and update page-allocation metadata. One action record can
therefore contain effects such as:

```text
BTREE_ACTION Insert(K)
undo: Delete(K)
physical effects:
    Write old leaf after-image
    Allocate right leaf after-image
    Write old parent after-image
    Allocate right internal after-image
    Allocate new root after-image
    Write database metadata after-image
```

Recovery installs these images in their encoded order. Temporary inconsistency
while recovery applies part of the record is acceptable because recovery runs
before clients start. If recovery itself crashes, the intact WAL record is
replayed again; valid pages already carrying the action LSN are skipped and
the remaining pages are installed.

### Allocation, Free, and Kind Changes

An effect distinguishes writing an existing page, allocating a page, and
turning a page into a freelist page. Allocation by file extension first ensures
that the file can address the page and then installs its complete initialized
after-image. Reuse is represented by the logged free-page and initialization
images in LSN order. The first implementation does not shrink the database
file during ordinary free.

V2 deliberately has no page generation. Ordered WAL replay, logged allocation
and initialization, and monotonic `pageLSN` values distinguish reuse. When a
valid reused page already has a later LSN, an old record is skipped. When the
page is torn, recovery replays retained complete after-images in order so the
newest initialization and subsequent images win.

## Logger Module

The logger is implemented as an independent module before it becomes the
authoritative recovery mechanism.

WAL lives in a log directory. Each segment has:

- A store containing length-prefixed encoded records.
- An index mapping each relative LSN to its store offset.
- A base LSN persisted in segment metadata and reflected in its file identity.
- Configured store and index size limits.

The store is an append-only sequence of independently framed records. Each
frame begins with a four-byte unsigned big-endian payload length followed by
exactly that many opaque payload bytes. The store owns its file descriptor,
uses positional I/O, and returns the length-prefix offset from append for the
index to persist.

On open, the store scans framing through the final complete record. A partial
length prefix or partial payload at the physical end of the file is reported
as an incomplete tail. The caller must explicitly repair that tail by
truncating to the reported last-valid boundary before another append. This
framing scan cannot distinguish a corrupted length value from a genuinely
truncated payload.

The first record codec deliberately has a minimal payload:

```text
+---------------------------+---------------------------+
| absolute LSN (8 bytes)    | opaque data (0..N bytes)  |
| unsigned, big-endian      | binary-safe `char` bytes  |
+---------------------------+---------------------------+
```

Absolute LSN zero is rejected because zero means "none" in page metadata. The
opaque data has no transaction or page-mutation interpretation yet. Record
magic, type, version, length duplication, and checksum are deferred until the
physical WAL record families are finalized.

The index is a dense array of fixed-width entries beginning at relative LSN
zero. Each 12-byte entry contains a four-byte unsigned big-endian relative LSN
followed by an eight-byte unsigned big-endian Store offset. Lookup reads entry
`relative_lsn * 12` and verifies that the encoded relative LSN matches the
requested position.

On open, an index file whose size is not a multiple of 12 reports an incomplete
tail. A complete entry whose relative LSN does not match its ordinal marks the
remainder as corrupt. `Index` is the mechanical fixed-width file layer and
encodes the relative LSN supplied by its caller; `Segment` owns the requirement
that new entries are dense.

Absolute LSN is:

```text
absolute LSN = segment base LSN + relative LSN
```

Each authoritative Store record encodes its absolute LSN. Segment derives the
four-byte relative Index key by subtracting its base LSN and rejects an append
unless the result is the next dense ordinal. Absolute LSN zero remains the
"none" value used by an untouched page's `pageLSN`, so the first segment base
and initial `next_lsn` are one. Relative LSN zero is the first record in any
segment. When a segment is empty, its next absolute LSN is its base LSN.
Otherwise:

```text
next LSN = segment base LSN + last valid relative LSN + 1
```

On rollover, the new segment's base LSN is the logger's current `next_lsn` and
its first record again has relative LSN zero.

The logger completes both the Store-record append and its corresponding Index
entry before testing the active segment's limits. `Segment::is_maxed()` uses a
strict greater-than comparison. If either resulting file size exceeds its
configured limit, that complete record remains in the active segment and the
next record begins a new segment. Records are never divided between segments.
`max_index_bytes` must be an integer multiple of the fixed 12-byte Index-entry
width, and both configured limits must be nonzero.

The Store is the correctness authority and the Index is derived. The crash
model assumes bytes in a structurally complete frame or entry retain the values
written to them. Recovery handles missing writes, incomplete append tails, and
Store/Index persistence gaps; arbitrary damage inside complete bytes is outside
scope and fails open rather than being repaired.

Only the Store is synced at a durability boundary. `Log::sync_through` and
`RaftLog::sync_through` call `sync_store()`; the Index is synced only by
`close()` and by recovery. So after a crash the Index can be arbitrarily stale:
short, with blocks that were never written reading back as zeros, or with
entries pointing at the wrong frame. Segment open therefore trusts none of it
until it agrees with the Store:

1. Truncate an incomplete Store frame to the last complete boundary.
2. Walk the whole Store, recording each frame's offset and checking the dense
   absolute-LSN (or Raft index) sequence.
3. Retain the longest Index prefix whose entries have the right ordinal and
   point at the right frame. A zero-filled block fails the ordinal check.
4. Truncate everything after that prefix and append mappings for every
   remaining Store record.

Recovery synchronizes a changed authoritative Store before the repaired Index.
Walking the whole Store costs a read of every record in the segment on open,
which recovery's redo pass reads anyway; in exchange no Index state a crash can
produce stops the log from opening.

The minimal record codec catches short records, reserved LSN zero, and a
non-dense absolute-LSN sequence. It does not yet protect opaque data with a
checksum. In particular, same-length interior payload corruption can remain
undetected until the checksummed physical record header is introduced.

The log manager is the owner of LSN allocation. At runtime it holds
`next_lsn`, `written_lsn`, and `durable_lsn`. It reconstructs `next_lsn` at
open by scanning the authoritative store through the final valid record; it
does not read that value from a database page and does not maintain a second
persisted "current LSN" counter that could disagree with the WAL.

Every database page, including page zero, still has its own `pageLSN`. Page
zero's value means only "the newest WAL action reflected in this metadata page
image." It is not the global last or next LSN. The WAL segment base plus the
last valid relative LSN is the persistent source from which the log manager
reconstructs the allocator.

The logger abstraction also owns existing segments, the active segment,
record append, sequential scan, record lookup needed by `prevLSN`, and
durability tracking.

### Finishing the Existing `Log` Layer

The repository already has the lower mechanical layers:

- `Store` owns length-prefixed record bytes.
- `Index` maps a segment-relative LSN to a Store offset.
- `Segment` coordinates Store and Index append, read, scan, synchronization,
  tail recovery, dense LSN validation, and size-limit detection.
- `WalRecord` currently contains an absolute LSN and an opaque byte payload.
- `WalRecordCodec` currently encodes that minimal envelope.

The next logger task is to finish `Log` as the owner and coordinator above
`Segment`, rather than introducing transaction or B+ tree behavior into the
lower layers. Its first public responsibilities are:

```cpp
class Log {
  public:
    void open(const std::string &directory);
    void close();

    Lsn append(PendingWalRecord record);
    WalRecord read(Lsn lsn) const;
    std::vector<WalRecord> scan() const;

    void sync_through(Lsn target_lsn);
    Lsn durable_lsn() const noexcept;

  private:
    std::string directory_;
    std::unique_ptr<Segment> active_segment_;
    std::vector<std::unique_ptr<Segment>> segments_;
    Lsn next_lsn_ = 1;
    Lsn durable_lsn_ = 0;
};
```

`Log::open` discovers and orders existing segments, opens each segment so its
Store/Index pair can recover, selects or creates the active segment, and
reconstructs the next absolute LSN. `append` assigns that LSN, appends through
the active segment, and creates the next segment only after the complete
crossing record leaves the current segment maxed. `sync_through` synchronizes
the required segments in LSN order, always Store before its derived Index, and
advances `durable_lsn_` through at least the requested record.

`sync_through` is a group commit. One thread syncs at a time, with the `Log`
mutex released, and it takes everything appended when it started rather than
only its own target; a caller arriving meanwhile waits and usually finds its
record already covered. `Store::sync` and `Index::sync` fsync without holding
their locks either, since a sync only has to cover appends that returned before
it began. `close()` waits for an in-flight sync, which holds raw segment
pointers. On macOS each fsync is an `F_FULLFSYNC`, which also stalls `write()`
calls to other files on the same volume while it runs, so the fewer threads that
append to the WAL at all, the better appends behave under load.

`Log`, `Segment`, `Store`, and `Index` remain unaware of `Key`, `Value`, page
layout, splits, or logical undo. The common WAL envelope gains record type,
transaction ID, `prevLSN`, framing validation, and checksum, but its payload
remains opaque bytes produced by a higher-level typed payload codec.

```cpp
struct PendingWalRecord {
    WalRecordType type;
    TransactionId txn_id;
    Lsn prev_lsn;
    std::vector<char> data;
};
```

Callers do not choose an LSN. `Log::append` converts the pending record into a
stored `WalRecord` using its current `next_lsn_` and returns the assigned LSN.
`Segment` retains its existing responsibility to reject any record whose LSN
does not equal that segment's next dense value.

An append failure after physical Store I/O begins is potentially ambiguous:
the authoritative Store record may exist even when its Index append failed.
The existing `Segment::recovery_required_` boundary is therefore preserved.
Such a result poisons the open storage engine and requires close, reopen, and
WAL recovery; it is not treated as a definitely unappended action that can be
restored in memory and followed by more writes.

## Typed WAL Record Family

Every typed record has a framed, checksummed common header conceptually
containing:

```cpp
enum class WalRecordType : std::uint16_t {
    TxnBegin,
    BTreeAction,
    Compensation,
    SystemAction,
    TxnCommit,
    TxnAbort,
    TxnEnd,
};

struct WalRecordHeader {
    std::uint32_t total_size;
    std::uint16_t format_version;
    WalRecordType type;
    Lsn lsn;
    TransactionId txn_id;
    Lsn prev_lsn;
    std::uint32_t checksum;
};
```

Exact serialized offsets are finalized with the typed codec. `prev_lsn` is the
previous record for the same transaction and `Transaction::last_lsn` is
advanced after each successful append.

### Transaction Boundary Records

`TXN_BEGIN` starts the chain and has `prevLSN = 0`. It is written lazily:
`TransactionManager::begin()` logs nothing, and `prepare_to_log()` writes the
begin just before the transaction's first action is built. A transaction that
never logs an action - a read, or a replicated session's lock-only local
transaction - writes no WAL records at all, not even a decision; one whose
commit carries a Raft index always writes its begin and commit. `TXN_COMMIT` records the
commit decision and becomes the transaction's durability point when WAL is
synchronized through its end. `TXN_ABORT` records the reason rollback began;
it does not mean rollback finished. `TXN_END` records completed cleanup,
especially after every required compensation record has been appended during
abort. The first implementation synchronizes WAL through an aborting
transaction's `TXN_END` before releasing its logical locks or acknowledging
rollback.

```cpp
struct TxnBeginPayload {};
struct TxnCommitPayload {};

enum class AbortReason : std::uint8_t {
    ClientRequest,
    DeadlockVictim,
    StatementFailure,
    InternalError,
};

struct TxnAbortPayload {
    AbortReason reason;
};

struct TxnEndPayload {};
```

Empty payloads serialize as zero payload bytes rather than relying on the C++
size of an empty struct. Commit timestamps, isolation metadata, or replicated
command identity can be added only through an explicit WAL format revision.

### B+ Tree Action Record

One `BTREE_ACTION` record combines one logical inverse with every physical
after-image produced by that operation:

```cpp
enum class BTreeActionKind : std::uint8_t {
    Insert,
    Update,
    Delete,
};

struct InsertUndo { Key key; };
struct UpdateUndo { Key key; Value old_value; };
struct DeleteUndo { Key key; Value old_value; };

using UndoDescriptor = std::variant<
    InsertUndo,
    UpdateUndo,
    DeleteUndo
>;

enum class PageEffectKind : std::uint8_t {
    Write,
    Allocate,
    Free,
};

struct PageEffect {
    PageEffectKind kind;
    PageNumber page_num;
    PageImage after_image;
};

struct BTreeActionPayload {
    BTreeActionKind action;
    UndoDescriptor undo;
    std::vector<PageEffect> physical_effects;
};
```

The undo mapping is:

```text
Insert(key, value)       -> Delete(key)
Delete(key, old_value)   -> Insert(key, old_value)
Update(key, old, new)    -> Update(key, new, old)
```

The logical inverse appears once even when the operation changes many pages.
Redo applies the physical effects; undo searches the current tree by key and
executes the inverse once. Existing strict logical locking retains the key's
lock until commit or complete abort, so the inverse cannot race a conflicting
operation on that key.

### Compensation Record

Every completed logical inverse appends a redo-only CLR:

```cpp
struct CompensationPayload {
    Lsn undo_of_lsn;
    Lsn undo_next_lsn;
    std::vector<PageEffect> physical_effects;
};
```

`undo_of_lsn` identifies the action reversed. `undo_next_lsn` is that action's
`prevLSN` and tells recovery where the transaction's remaining undo resumes.
The effects are the complete physical after-images produced by executing the
inverse against the current B+ tree. A CLR is redone but never undone.

Undo restores logical contents, not the former physical tree shape. An insert
that caused a split is undone by deleting the inserted key; the split may
remain as a valid underfilled structure. Merge-on-delete is postponed in the
first implementation.

### System Action Record

A redo-only `SYSTEM_ACTION` contains a system action kind plus a vector of
physical page effects. It is available for compaction, independent structural
maintenance, and allocator maintenance that is not logically owned by one
user action. Initially, splits and root propagation caused directly by an
insert can remain inside that insert's compound `BTREE_ACTION` record.

```cpp
enum class SystemActionKind : std::uint8_t {
    PageCompaction,
    AllocatorMaintenance,
    IndependentStructuralMaintenance,
};

struct SystemActionPayload {
    SystemActionKind action;
    std::vector<PageEffect> physical_effects;
};
```

An independent system action uses transaction ID and `prevLSN` zero and is
always redo-only. A structural effect caused directly by a user action stays
inside that transaction's `BTREE_ACTION` rather than being duplicated here.

### Why There Are No Persistent Before-Images

Persistent transaction undo is logical, so an action record does not store a
full-page before-image. Restoring one would erase later updates made by other
transactions to different keys on the same physical page. Before-images exist
only as operation-local scratch used to cancel a mutation whose compound WAL
append has not succeeded.

## KeyStore-to-Pager Integration

The cross-layer object is a pending action payload, not an encoded `WalRecord`.
KeyStore creates it where the logical meaning is known, B+ tree helpers pass it
through physical propagation, Pager adds exact page effects, and the completed
payload is encoded and appended only after the operation succeeds.

```cpp
struct PendingBTreeAction {
    TransactionId txn_id;
    Lsn prev_lsn;

    std::optional<BTreeActionKind> action;
    std::optional<UndoDescriptor> undo;

    std::vector<PageEffect> page_effects;

    void add_or_replace_page_effect(PageEffect effect);
};
```

This is a higher-level builder. The final action kind and inverse may remain
unset until B+ tree returns whether a `put` inserted or replaced and returns
any old value. Both must be set before encoding. The builder does not have an
LSN because only `Log` assigns LSNs, and it does not contain already encoded
WAL bytes while lower layers are still discovering physical effects.

### KeyStore Responsibility

KeyStore obtains the required logical key lock, associates the action with its
transaction, and provides the logical inverse. A `put` must distinguish a new
insert from replacement of an existing value; a remove must retain the removed
value:

```cpp
struct BTreePutResult {
    BTreeStatus status;
    std::optional<Value> old_value;
};
```

After B+ tree success, KeyStore selects:

```text
no old value      -> InsertUndo { key }
old value exists  -> UpdateUndo { key, old_value }
successful remove -> DeleteUndo { key, removed_value }
```

This avoids a second lookup solely for WAL construction. If no logical change
occurred, KeyStore cancels the pending action and appends no action record.

### B+ Tree Responsibility

Every mutating B+ tree entry point and propagation helper receives the same
pending action:

```cpp
BTreePutResult put(
    const Key &key,
    const Value &value,
    PendingBTreeAction &wal_action
);

BTreeStatus propagate_splitting(
    PageNumber page_num,
    std::vector<TraversalPathEntry> &path,
    PendingBTreeAction &wal_action
);
```

The B+ tree does not encode records or copy page bytes. It passes the action to
every Pager call that can mutate an existing page, allocate or free a page,
change a root, or update metadata. This includes every current `begin_write`,
`allocate_page`, `free_page`, and `set_btree_root` path.

Because the current `BTree` privately owns its `Pager`, it also exposes narrow
completion forwarding during this transition:

```cpp
void complete_wal_action(
    PendingBTreeAction &wal_action,
    Lsn assigned_lsn
);

void cancel_wal_action(PendingBTreeAction &wal_action);
```

These methods delegate frame finalization or scratch restoration to Pager.
They do not encode or append WAL themselves. A future shared `StorageEngine`
may own this orchestration directly, but that larger refactor is not required
to integrate the current ownership layout.

### Pager Responsibility

Pager owns exact physical capture and frame lifecycle. Its mutating interface
requires the pending action:

```cpp
PageMutation begin_mutation(
    PageNumber page_num,
    PendingBTreeAction &wal_action
);

PagerAllocateResult allocate_page(
    V2PageKind kind,
    PendingBTreeAction &wal_action
);

PagerResult free_page(
    PageNumber page_num,
    PendingBTreeAction &wal_action
);

PagerResult set_btree_root(
    PageNumber root_page_num,
    PendingBTreeAction &wal_action
);
```

On first touch, Pager captures transient cancellation state, takes an extra
operation pin, and marks the frame WAL-pending and therefore not flushable. At
each completed physical change it adds or replaces that page's effect. If the
same logical operation touches one page repeatedly, only its final after-image
remains in the compound record.

Pager, not B+ tree, understands the exact persistent 4096-byte representation,
allocation/free effect kind, page checksum, and runtime frame state. B+ tree
therefore never constructs a `PageEffect` from raw bytes itself.

### Successful Operation

The end-to-end write path is:

```text
KeyStore creates PendingBTreeAction with txn ID and prevLSN
    -> BTree performs the operation and propagates the same action
    -> Pager collects every final physical effect and keeps pages WAL-pending
    -> KeyStore supplies the final logical undo descriptor
    -> BTreeActionCodec encodes the completed opaque payload
    -> Log assigns an LSN and appends the record
    -> transaction.lastLSN becomes the returned LSN
    -> KeyStore calls BTree::complete_wal_action with the returned LSN
    -> Pager installs that LSN/checksum in affected frames
    -> Pager marks frames dirty and flushable and releases operation pins
```

The after-image payload need not trust recovery fields copied before LSN
assignment. Redo installs the recorded page content, writes the enclosing
record's LSN into `pageLSN`, and recomputes the whole-page checksum. After the
runtime append succeeds, Pager performs the same `pageLSN` and checksum update
on the cached frames before making them flushable.

Appending does not synchronize WAL. Commit and page-spill paths retain their
separate durability responsibilities.

### Failure Boundaries

Before physical WAL append begins, B+ tree failure, validation failure, codec
failure, or buffer-allocation failure cancels the operation. Pager restores
the transient page images and prior dirty state, releases pins, and discards
the pending action through `BTree::cancel_wal_action`.

After physical append begins, failure may leave a valid authoritative Store
record without its derived Index entry. The engine enters a recovery-required
state and stops accepting operations. It does not restore pages and continue
because reopening may discover and redo that record.

The practical conversion order is:

1. Finish `Log` over the existing Segment/Store/Index implementation. Complete.
2. Add the typed, checksummed common WAL envelope. Complete.
3. Add the typed payload codec and `PendingBTreeAction`. Complete.
4. Thread the pending action through KeyStore, B+ tree, and every Pager mutation
   path while the rollback journal remains authoritative.
5. Add WAL-pending frame state and complete page-effect collection tests.
6. Enforce WAL-before-data on spill and eviction.
7. Add transaction-manager commit, logical abort, CLRs, and startup recovery.
8. Remove rollback journaling only after crash-injection equivalence tests pass.

## Incremental Rollback-Journal-to-WAL Transition

### Stage 1: V2 Page Format and Pager Cutover — Complete

V2-named page types, the common page codec, whole-page CRC32C, corruption
tests, and raw full-page views are implemented. PCache and Pager now own
`PageV2` frames, Pager results expose pinned `PageV2*` values, page zero uses
the database-metadata kind, allocation assigns the requested B+ tree kind,
freeing assigns the freelist kind, and disk reads validate checksum, kind, and
encoded page number. B+ tree codecs interpret bytes 24 through 4095 while the
rollback journal continues to capture complete 4096-byte images.

### Stage 2: Standalone Logger — Complete

Implement store/index segments, LSN allocation, append, rollover, scan,
lookup, durability tracking, and torn-tail tests without pager integration.

### Stage 3: Shadow Mutation Logging

Add `begin_mutation` and `finish_mutation` beside `begin_write`. Convert pager
and B+ tree modification paths one at a time. Converted operations append WAL
records. The operation coordinator groups every propagated page effect into
one compound action record, while the rollback journal remains the
authoritative commit and crash-recovery mechanism.

The transitional engine is intentionally awkward: it may force database pages
according to the rollback-journal policy even though it also emits WAL. That
temporary behavior is removed only after all physical mutations are logged.

### Stage 4: WAL Cutover

After allocation, free, metadata, root, leaf, internal, split, and file-length
changes all emit complete compound WAL records, and logical undo plus CLRs are
implemented:

- Remove `begin_write` and make `begin_mutation` the only page-write boundary.
- Append and synchronize `TXN_COMMIT` before acknowledging commit.
- Stop forcing dirty database pages at commit.
- Before any dirty page write, make WAL durable through that page's `pageLSN`.
- Permit uncommitted dirty pages to reach disk under STEAL.
- Remove rollback-journal commit, abort, and hot-journal recovery only after
  equivalent WAL crash tests pass.

## Startup Recovery Without Regular Checkpoints

The first WAL cutover has startup recovery but no periodic checkpointer. The
component is a recovery manager, even if it shares low-level page-application
helpers with a future checkpointer.

On database open, before accepting embedded callers, recovery:

1. Scans retained WAL from the first segment.
2. Reconstructs transaction state and per-transaction `lastLSN` values.
3. Redoes every required physical after-image in LSN order, including history
   from transactions that did not commit. A valid page is skipped when its
   `pageLSN` is already at least the record LSN.
4. Uses checksums to detect torn pages. A torn page's `pageLSN` is not trusted;
   retained complete after-images reconstruct that page in WAL order.
5. After physical redo has restored a structurally valid B+ tree, identifies
   transactions without durable commit records as losers.
6. Undoes each loser logically by following `lastLSN`, `prevLSN`, and any
   CLR `undoNextLSN` values. Every newly completed inverse emits a CLR with its
   propagated physical after-images.
7. Synchronizes recovery-generated WAL before allowing any page carrying those
   CLR effects to reach disk, then opens the database.

Redo-before-undo is essential for structural operations. If only some pages of
an uncommitted split reached disk, physical redo first installs all pages from
the intact compound record. Logical undo then removes the losing key without
trying to reverse the split itself.

### Torn Pages and Recovery Re-entry

WAL-before-data guarantees that the complete compound record is durable before
any page carrying its LSN is written. A crash can nevertheless interrupt the
database-page write itself and leave a mixture of old and new sectors. Because
the whole-page checksum includes `pageLSN`, recovery treats any checksum
failure as an invalid page and does not perform an LSN comparison against it.
It reconstructs that page from retained full after-images in increasing LSN
order, ending at the newest action applicable to that page.

WAL records themselves use length framing and checksums. A partial final WAL
record is excluded from the valid WAL prefix. No database page may depend on
that excluded record because the page could not have been written until WAL
was synchronized through the complete record.

Applying a compound action to several database pages is restartable rather
than an atomic multi-page disk write. Recovery may crash after installing only
some effects. On the next start, pages with a valid checksum and
`pageLSN >= actionLSN` are skipped; torn or older pages receive their recorded
after-images. Repetition eventually installs the complete structurally valid
state before logical undo begins.

Without regular checkpoints:

- WAL is not reclaimed.
- Recovery scans the complete retained history.
- Startup time and disk usage grow over time.

Those costs are accepted for the initial cutover. Periodic checkpointing,
checkpoint metadata, and safe segment reclamation are a later milestone.

## Durability Rules

- `finish_mutation` appends but does not synchronize the WAL.
- A cached after-image is never written to the database before WAL is durable
  through its `pageLSN`.
- Commit is not acknowledged before its commit record is durable, unless the
  transaction logged no action and carries no Raft index: nothing depends on
  that record surviving a crash.
- Database pages are not forced at commit.
- WAL append buffers must own record bytes; they cannot retain spans into
  mutable cached pages.
- A page participating in an unfinished compound action is pinned and not
  flushable.
- A failed or abandoned action restores all transient scratch before-images
  before releasing its pins.
- A page checksum failure makes its encoded `pageLSN` untrusted. Recovery uses
  retained complete WAL after-images to reconstruct it.
- WAL records have length framing and checksums; recovery ignores only a
  defined incomplete final record and rejects corruption inside retained
  history.

## Immediate Implementation Order

V2 pages, the typed Log stack, logical page latches, transaction-aware KeyStore
operations, complete B+ tree page effects, WAL finalization, WAL-before-data,
and latch-scoped CLR append are implemented. `wal_pending` prevents an action's
frames from being selected for eviction before append assigns an LSN.

The remaining cutover boundary is startup recovery. The rollback journal is
still retained for the legacy embedded API and hot-journal recovery until WAL
analysis/redo/undo and equivalent crash-injection tests exist. It must not be
deleted merely because runtime commit and abort now emit WAL.
