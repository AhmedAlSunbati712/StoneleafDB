# Transaction + Lock Manager

## Status and Scope

This is the evolving target design for local interactive transactions,
logical key locks, lock promotion, wait-for graph maintenance, deadlock
victim handling, rollback, and client retry behavior.

The first implementation favors a simple invariant over an optimal victim
policy: the global wait-for graph is kept acyclic. When registering a new
wait would create a cycle, the transaction making that request becomes the
deadlock victim and its new dependencies are not left installed.

This document does not yet choose the graph container, client retry limit, or
wire representation of a deadlock error. Runtime rollback uses logical
key-level inverse operations, while the local WAL records complete physical
after-images for redo and compensation records for restartable undo.

## Core Invariants

The implementation must preserve these rules:

1. Logical locks are owned by transaction IDs, not worker threads.
2. Granted `S` and `X` locks remain held through durable commit or complete
   abort.
3. An `S -> X` promotion retains `S` while waiting. It never releases `S` and
   rejoins the queue as an unrelated `X` request.
4. Promotion changes ownership atomically from `S` to `X`.
5. A deadlock victim retains all granted locks until its changes are undone.
6. Page latches and pins are short-lived B+ tree resources. They are not held
   in the transaction across statements or client think-time.
7. Each transaction has at most one outstanding statement and therefore one
   outstanding logical-lock request in the first version.
8. Every wait-for edge represents a dependency that currently prevents a
   lock request from being granted.
9. Registering new wait edges and checking whether they create a cycle is one
   synchronized graph operation.
10. The requester is rejected when its new edges create a cycle, so a cycle
    is never left in the persistent graph.

## Component Ownership

The server owns one shared storage engine, transaction manager, lock manager,
and wait-for graph:

```text
Server
├── StorageEngine
│   ├── KeyStore
│   ├── BTree
│   ├── BufferPool
│   └── WAL manager
├── TransactionManager
│   ├── active transaction table
│   └── global WaitForGraph
├── LockManager
│   └── sharded logical-key lock table
└── client sessions
    └── optional active transaction handle per connection
```

The responsibilities are divided as follows:

- A client session associates one connection with at most one explicit
  transaction.
- `TransactionManager` creates, looks up, commits, aborts, and removes
  transactions. It owns the global wait-for graph.
- `LockManager` owns key-lock compatibility, granted owners, FIFO queues, and
  the calculation of blockers for each request.
- `KeyStore` requests logical locks and performs encoded key-value operations.
  It does not inspect lock queues or construct graph edges.
- `BTree` and the buffer pool acquire page pins and latches for short physical
  operations. They do not own transaction-lifetime logical locks.
- The statement executor coordinates autocommit and explicit-transaction
  boundaries. It converts a lock-manager deadlock result into transaction
  abort and a client-visible error.

### Operation-Scoped Page Latches

Transactions own logical key locks, but they do not own page latches. Each B+
tree call creates a `BTreeOperation` containing logical page latches acquired
by page number from a sharded `PageLatchManager`.
`PageV2` stores no latch. The operation owns no pins or frame pointers, so the
B+ tree explicitly unreferences frames during traversal while retaining any
ancestor latch needed for later propagation.

The B+ tree prevents latch cycles with one global order: header before root,
parent before child, and a consistent sibling order. Code never reacquires an
ancestor while retaining a descendant and never waits for a logical key lock
while retaining a page latch. No latch wait-for graph is maintained.

Point operations know their key and acquire the transaction-level key lock
before entering the tree. A range scan must first discover a candidate under a
shared page latch. It copies that key, releases its page guards, acquires
`S(candidate)`, and reseeks from the root to revalidate the candidate before
returning its value. The complete cursor and latch-crabbing protocol is
specified in [Tree_Module.md](Tree_Module.md).

### Write Operation Handoff

One mutating KeyStore call creates a `PendingBTreeAction` after it owns the
transaction's logical key lock. The object begins with transaction identity
and `prevLSN`; KeyStore fills the final action kind and logical inverse once B+
tree reports whether the operation inserted, replaced, removed, or made no
change. The same object travels through B+ tree propagation into every Pager
mutation path:

```text
KeyStore: transaction and logical inverse
    -> BTree: leaf/internal/root propagation
    -> Pager: exact physical after-images and frame state
    -> BTreeActionCodec: opaque action payload
    -> Log: LSN assignment and append
```

Low-level `Log`, `Segment`, `Store`, and `Index` objects do not understand keys,
values, or B+ tree pages. KeyStore does not encode WAL bytes, and B+ tree does
not copy persistent page images. Pager is the only layer that captures exact
page effects.

After `Log::append` succeeds, its returned LSN becomes the transaction's new
`lastLSN`; Pager installs the same LSN and a recomputed checksum in every
affected cached page before making those frames flushable. Before append, all
affected frames remain operation-pinned and WAL-pending. A preparation failure
can cancel from transient scratch images. An ambiguous physical append failure
requires reopening and recovery rather than continuing the transaction.

## Session and Server Context

`SessionContext` means the state of one client connection, not the state of
the complete server:

```cpp
struct SessionContext {
    std::optional<TransactionHandle> active_transaction;
};
```

If the existing server connection object already owns this field, a separate
type is unnecessary. The important boundary is that the session stores only
the ID or handle for its own active transaction.

The server-wide active-transaction collection belongs to
`TransactionManager`:

```cpp
class TransactionManager {
  public:
    TransactionManager(
        Log &log,
        LockManager &lock_manager,
        TransactionUndoExecutor &undo_executor
    );

    TransactionHandle begin();
    TransactionHandle find(TransactionId txn_id) const;
    Lsn append_action(
        const TransactionHandle &txn,
        PendingWalRecord action
    );
    CommitStatus commit(const TransactionHandle &txn);
    AbortStatus abort(
        const TransactionHandle &txn,
        AbortReason reason
    );

    WaitRegistrationStatus register_wait(
        TransactionId waiting_txn,
        std::span<const TransactionId> blockers
    );

    void remove_wait(TransactionId waiting_txn);

  private:
    Log &log;
    LockManager &lock_manager;
    TransactionUndoExecutor &undo_executor;

    mutable std::shared_mutex transactions_mutex;
    std::unordered_map<
        TransactionId,
        TransactionHandle
    > active_transactions;

    WaitForGraph wait_for_graph;
};
```

`TransactionHandle` is `std::shared_ptr<Transaction>`. The active table owns
one handle and the session or current command owns another while using the
transaction. Removing a terminal transaction from the table therefore cannot
destroy it underneath an executing worker. B+ tree and key-store calls receive
a non-owning `Transaction &` obtained from that handle.

The manager receives the WAL, logical-lock manager, and a logical-undo
executor as non-owning references. The undo executor applies one stored
logical inverse through the B+ tree and receives a one-shot compensation
callback from TransactionManager. It invokes that callback with the physical
effects while its `BTreeOperation` still owns all relevant page latches, then
installs the returned CLR LSN before releasing them. This keeps transaction
lifecycle and WAL chaining in the manager without making it implement B+ tree
operations itself.

## Transaction Context

A transaction records its lifecycle, strongest granted logical locks, and
the information needed to undo its writes:

```cpp
enum class TransactionState : std::uint8_t {
    Active,
    AbortRequested,
    Committing,
    Aborting,
    Committed,
    Aborted,
};

struct HeldKeyLock {
    Key key;
    LockMode mode;
};

class Transaction {
  public:
    TransactionId id() const noexcept;
    TransactionState state() const noexcept;
    Lsn last_lsn() const noexcept;

  private:
    friend class TransactionManager;

    const TransactionId id_;
    TransactionState state_ = TransactionState::Active;

    std::vector<HeldKeyLock> held_key_locks_;
    std::optional<Key> waiting_for_key_;

    Lsn last_lsn_ = 0;
};
```

The immutable ID prevents identity changes after registration. One client
thread owns and executes each transaction, so lifecycle state and `lastLSN` do
not need atomic storage. A shared mutex protects the transaction-manager map:
lookups and wait validation take shared ownership, while ID allocation, table
mutation, and lifecycle transitions take exclusive ownership. A
transaction remains `Active` while its current statement waits for a lock;
pending/granted/cancelled status belongs to the `Waiter`, because `LockManager`
receives only a transaction ID. `TransactionManager` owns valid state
transitions and WAL-chain updates.

The transaction does not contain page latches. A normal operation and a
logical inverse acquire the required page latches, modify the current tree,
append their compound physical effects, and release the latches before moving
on.

### Lifecycle Interface Ownership

Public `commit` and `abort` operations live on `TransactionManager`, not on
`Transaction`. A transaction is state and transaction-local metadata; it does
not own the WAL manager, lock manager, active table, wait-for graph, or B+ tree
undo executor required to finish either boundary.

`TransactionManager::commit` coordinates:

1. The atomic transition from `Active` to `Committing`.
2. Verification that the transaction has no unfinished B+ tree action.
3. Append of `TXN_COMMIT` through the transaction's `prevLSN` chain.
4. WAL synchronization through the commit record — skipped when the
   transaction logged no B+ tree action and the record carries no Raft index.
   Such a transaction changed nothing: if its commit record is lost in a crash,
   recovery ends it with nothing to undo, and the client could not tell. This
   keeps read-only transactions off the disk.
5. The transition to `Committed`.
6. Logical-lock and graph cleanup.
7. Removal from the active table.

`TransactionManager::abort` coordinates:

1. A single transition from `Active` or `AbortRequested` to `Aborting`, so
   concurrent client rollback and deadlock cancellation cannot run undo twice.
2. Cancellation of any outstanding lock waiter while retaining granted locks.
3. Append of `TXN_ABORT`.
4. Reverse traversal from `last_lsn_` and execution of each logical inverse.
5. Append of a compensation log record for every completed inverse.
6. Append of `TXN_END`, WAL synchronization through that record, and the
   transition to `Aborted`.
7. Release of logical locks, graph cleanup, and active-table removal.

The transaction destructor never commits or aborts. Losing a client handle
does not silently choose a durability outcome; connection cleanup explicitly
asks `TransactionManager` to abort an active transaction.

### Current Transaction-Manager Implementation

The implemented coordinator now owns begin-record creation, active-table and
wait-for-graph node ownership, transaction lookup, B-tree action appends,
commit durability, logical abort traversal, CLR chaining, durable `TXN_END`,
and terminal cleanup. `append_action()` accepts only a matching B-tree action
whose transaction ID and `prevLSN` agree with the current transaction, then
updates `last_lsn_` only after the append succeeds.

Begin records are deferred. `begin()` registers the transaction but appends
nothing; `prepare_to_log()` appends `TXN_BEGIN` the first time a caller is about
to build an action, and returns the LSN that action must chain to. Commit
writes a decision only for a transaction that logged something or carries a
Raft index, and abort of a transaction that logged nothing writes nothing. Most
transactions - reads, and every replicated session's lock-only transaction -
therefore never touch the WAL. That matters beyond record count: on macOS a
`write()` stalls while any `F_FULLFSYNC` on the volume is in flight, so every
append a session skips is one fewer stall for the apply loop.

The first `begin()` after opening a retained WAL scans its records and chooses
one greater than the largest prior transaction ID. This prevents transaction
identity reuse across reopen until recovery or a checkpoint introduces a
persisted transaction-ID allocator.

Logical-lock acquisition is now connected to the coordinator. Construction of
`TransactionManager` attaches it to `LockManager`; immediate and delayed grants
are recorded in `held_key_locks_`, and commit or abort releases those locks in
reverse acquisition order. Contended requests enter the FIFO queue before
their complete blocker set is registered. Failed registration removes the
queue entry, while a successful grant removes its outgoing graph edges before
notification.

Explicit cancellation of a waiter by transaction ID and key remains to be
implemented. That path will populate `waiting_for_key_`, remove the queued
request under its shard mutex, remove the graph edges, mark the waiter
cancelled, and notify it before deadlock-victim abort can be initiated from a
different thread.

## Lock Table

The logical lock table is sharded by `KeyHash`. Each shard mutex protects its
map and every `LockState` stored in that map:

```cpp
struct LockShard {
    std::mutex mutex;
    std::unordered_map<Key, LockState, KeyHash, KeyEqual> states;
};
```

A waiter must distinguish a normal exclusive request from a conversion,
because a conversion already owns `S`:

```cpp
enum class WaiterKind : std::uint8_t {
    Shared,
    Exclusive,
    Upgrade,
};

enum class WaiterState : std::uint8_t {
    Pending,
    Granted,
    DeadlockVictim,
    Cancelled,
};

struct Waiter {
    TransactionId txn_id;
    WaiterKind kind;
    WaiterState state = WaiterState::Pending;
};

struct LockState {
    std::unordered_set<TransactionId> shared_owners;
    std::optional<TransactionId> exclusive_owner;
    std::deque<std::shared_ptr<Waiter>> waiters;
    std::condition_variable changed;
};
```

The queue should be a `deque`, not a `std::queue`, because deadlock
cancellation must locate and remove a waiter that may not be at the front.

## Wait-For Graph

The wait-for graph is global across every logical key:

```text
T1 -> T2
```

means that T1 currently has a lock request that cannot be granted until T2
stops blocking it.

A possible initial representation is:

```cpp
class WaitForGraph {
  public:
    bool add_edges(
        TransactionId from,
        std::span<const TransactionId> destinations
    );

    void remove_outgoing(TransactionId txn_id);
    void remove_transaction(TransactionId txn_id);

  private:
    std::mutex mutex;
    std::unordered_map<
        TransactionId,
        std::unordered_set<TransactionId>
    > edges;
};
```

`TransactionManager` owns this object and exposes the registration and removal
operations needed by `LockManager`. `KeyStore` never updates the graph.

The incremental `WaitForGraph` implementation maintains both outgoing and
incoming adjacency sets under one shared mutex. `add_edges()` validates and
inserts a complete set under one exclusive lock. It returns `false` when a node
is missing or any edge would create a cycle, and retains none of the new edges
when the batch is rejected. Duplicate edges are successful no-ops.

### Acyclic-Graph Invariant

Before a new request registers its blockers, the graph is assumed to be
acyclic. All new edges originate at the requesting transaction:

```text
requester -> blocker 1
requester -> blocker 2
requester -> blocker 3
```

Adding these edges creates a cycle exactly when at least one blocker can
already reach the requester in the existing graph.

For example, before adding `T3 -> T1`:

```text
T1 -> T2 -> T3
```

T1 can already reach T3, so adding `T3 -> T1` would create:

```text
T1 -> T2 -> T3 -> T1
```

The initial detector therefore performs this operation under the graph mutex:

```text
for every proposed blocker B:
    if B can already reach requester:
        reject registration as DeadlockVictim

insert requester -> every blocker
return Waiting
```

The reachability search may initially be DFS or BFS. The graph is expected to
contain only active and waiting transactions, so a straightforward traversal
is acceptable for the educational first version.

The add-and-check operation must be serialized. Two threads must not both
check an old graph, both see no cycle, and then concurrently insert edges that
together create a cycle.

## Calculating Blockers

`LockManager` calculates blockers while holding the target key's shard mutex.
The current transaction is never included as its own blocker.

### Shared Request

An `S` request is blocked by:

- Another transaction holding `X`.
- Every earlier `X` or `Upgrade` waiter that must remain ahead to preserve
  writer fairness.

Earlier `S` waiters are compatible and may be granted in the same reader
batch.

### New Exclusive Request

An `X` request from a transaction that owns no lock on the key is blocked by:

- The current `X` owner.
- Every current `S` owner.
- Every earlier waiter in the FIFO queue.

All earlier waiters matter because the requested target mode is exclusive and
cannot coexist with either `S` or `X`.

### Shared-to-Exclusive Promotion

An `Upgrade` request is blocked by:

- Every other current `S` owner.
- An `X` owner, if one exists and is another transaction.
- Every incompatible earlier waiter required by FIFO ordering.

The promoter remains in `shared_owners`, but it is excluded from its own
blocker set.

An earlier `X` waiter creates an immediate dependency cycle when that waiter
already depends on the promoter's `S` lock:

```text
T1 owns S(A)
T2 is ahead waiting for X(A): T2 -> T1
T1 requests Upgrade(A):      T1 -> T2
```

The requester-victim policy rejects T1's promotion, allowing the earlier
writer to proceed after T1's complete abort releases `S(A)`.

## Shared Acquisition

To acquire `S(key)`:

1. Hash the validated key and lock its shard mutex.
2. Find or create `LockState`.
3. Return `AlreadyShared` if the transaction already owns `S`.
4. Return `AlreadyExclusive` if it already owns `X`.
5. Grant immediately when there is no `X` owner and no earlier waiter that
   must remain ahead.
6. Otherwise create and enqueue a `Shared` waiter.
7. Calculate blockers from the owner and earlier-waiter state.
8. Atomically register the dependencies in the global wait-for graph.
9. If registration would create a cycle, remove the new waiter and return
   `DeadlockVictim` without installing the rejected graph edges.
10. Otherwise wait on the key's condition variable. Waiting releases the
    shard mutex and reacquires it before checking waiter state.
11. Return only after the grant path has installed the transaction in
    `shared_owners`, removed its outgoing wait edges, and marked it granted.

## New Exclusive Acquisition

To acquire `X(key)` when the transaction does not own `S(key)`:

1. Hash the key, lock its shard, and find or create `LockState`.
2. Return `AlreadyExclusive` if the transaction already owns `X`.
3. Grant immediately only when there are no owners and no earlier waiters.
4. Otherwise enqueue an `Exclusive` waiter.
5. Calculate blockers from every owner and earlier waiter.
6. Atomically register those dependencies and check whether they create a
   cycle.
7. If the requester is a deadlock victim, remove its waiter and return the
   victim result.
8. Otherwise sleep until the grant or cancellation path changes waiter state.
9. The grant path installs `exclusive_owner`, removes outgoing wait edges,
   marks the waiter granted, and notifies the sleeper before acquisition
   returns successfully.

## Shared-to-Exclusive Promotion

Promotion begins when `lock_exclusive(txn, key)` finds the transaction in
`shared_owners`.

### Immediate Promotion

Promotion may happen immediately when:

- The transaction is the only shared owner.
- There is no exclusive owner.
- There is no earlier waiter that must remain ahead.

The ownership transition is atomic under the shard mutex:

```cpp
state.shared_owners.erase(txn_id);
state.exclusive_owner = txn_id;
```

The transaction's held-lock record is then changed from `Shared` to
`Exclusive`.

### Waiting Promotion

When immediate promotion is impossible:

1. Keep the transaction in `shared_owners`.
2. Enqueue an `Upgrade` waiter at the FIFO position of the request.
3. Calculate blockers, excluding the transaction itself.
4. Register its global wait-for edges and check for a cycle atomically.
5. If the new edges create a cycle, choose the requester as the victim and
   remove the new upgrade waiter.
6. If no cycle is created, wait while continuing to own `S`.
7. Grant only when the upgrade waiter is eligible, no `X` owner exists, and
   the promoter is the only remaining `S` owner.
8. Atomically remove the promoter from `shared_owners`, install it as
   `exclusive_owner`, remove its outgoing wait edges, and mark it granted.

There is never a successful waiting path that removes the promoter's `S` lock
before `X` is granted.

## Multiple Upgraders on One Key

Assume:

```text
shared_owners = {T1, T2, T3}
waiters = []
```

T1 requests promotion:

```text
shared_owners = {T1, T2, T3}
waiters = [Upgrade(T1)]

graph:
T1 -> T2
T1 -> T3
```

No cycle exists, so T1 waits while retaining `S`.

T2 then requests promotion:

```text
shared_owners = {T1, T2, T3}
waiters = [Upgrade(T1), Upgrade(T2)]

proposed edges:
T2 -> T1
T2 -> T3
```

The graph already contains `T1 -> T2`. Adding `T2 -> T1` would create a
cycle, so T2 becomes the victim. Its upgrade waiter is cancelled, its
transaction is completely aborted, and only then is its granted `S` released.

After T2 aborts:

```text
shared_owners = {T1, T3}
waiters = [Upgrade(T1)]
```

T1 remains blocked by T3. When T3 commits, aborts, or requests a promotion
that is itself rejected as cyclic, T1 eventually becomes the only shared
owner and is promoted atomically.

## Cross-Key Deadlock

The graph is global because a transaction may hold locks on several keys:

```text
T1 owns X(A)
T2 owns X(B)
```

T1 requests `X(B)`:

```text
LockState(B): T2 owns, T1 waits
Graph:        T1 -> T2
```

T2 then requests `X(A)`:

```text
LockState(A): T1 owns, T2 requests a wait
Proposed:     T2 -> T1
```

Because T1 already reaches T2, the proposed edge closes a cycle. T2 is
rejected as the requester-victim. This uses the same mechanism as same-key
promotion; there is no local per-key wait-for graph.

A longer cycle works the same way:

```text
T1 owns A and waits for T2 on B
T2 owns B and waits for T3 on C
T3 owns C and requests A held by T1

T1 -> T2 -> T3
proposed T3 -> T1 creates the cycle
```

## Waiting and Notification

A waiting thread uses the shard mutex with a predicate:

```cpp
state.changed.wait(shard_lock, [&] {
    return waiter->state != WaiterState::Pending;
});
```

The condition-variable wait atomically releases the shard mutex while the
thread sleeps and reacquires it before checking waiter state.

The grant path installs ownership before waking the waiter:

```text
remove eligible waiter from queue
    -> install S or X ownership
    -> remove its outgoing graph edges
    -> mark waiter Granted
    -> notify condition variable
```

The awakened thread does not race to acquire ownership again. It wakes already
recorded as the owner.

If one condition variable is shared by every waiter on the key, the grant path
uses `notify_all()`. Waiters that were not granted recheck their predicates and
sleep again.

## Fair Grant Procedure

After an owner releases a lock or a waiter is cancelled, the releasing thread
runs the grant procedure while holding the shard mutex:

- If an `X` owner exists, grant nothing.
- If the front waiter is `Shared`, grant the consecutive front batch of
  `Shared` waiters while no `X` owner exists.
- If the front waiter is `Exclusive`, grant it only when there are no `S` or
  `X` owners.
- If the front waiter is `Upgrade(T)`, grant it only when there is no `X`
  owner and `shared_owners` contains exactly T.

Each granted waiter's outgoing graph edges are removed as part of the grant.
Cancellation reruns the same procedure because removing one waiter may make a
later request eligible.

## Deadlock-Victim Handling

When `register_wait()` reports that the requester's new edges would create a
cycle, `LockManager` performs only local request cancellation while holding
the shard mutex:

```text
remove the new waiter
    -> mark it DeadlockVictim
    -> ensure its rejected outgoing edges are absent
    -> rerun the key's grant procedure if necessary
    -> release the shard mutex
    -> return DeadlockVictim
```

`LockManager` does not remove the transaction from `shared_owners` and does
not perform database rollback.

The statement executor passes the result to `TransactionManager`, which
performs complete abort:

```text
mark transaction AbortRequested
    -> cancel any remaining pending request
    -> mark transaction Aborting
    -> undo its changes while retaining every granted logical lock
    -> release every granted logical lock
    -> remove all graph edges involving the transaction
    -> mark transaction Aborted
    -> remove it from the active-transaction table
    -> clear the session's active transaction ID
    -> return DeadlockVictim to the client
```

The client-visible deadlock result should be returned only after server-side
abort is complete. This guarantees that the retry begins after the victim's
old locks and effects have been removed.

## Rollback and Page Latches

Rollback does not restore an old complete page image. That would erase a later
change made by another transaction to a different key on the same page.
Instead, every user-visible B+ tree action stores one logical inverse:

```text
INSERT(key, value)       -> DELETE(key)
DELETE(key, old_value)   -> INSERT(key, old_value)
UPDATE(key, old, new)    -> UPDATE(key, new, old)
```

The already-granted logical lock on that key remains held while the inverse
runs, so it cannot race another transaction changing the same key. The inverse
searches the current B+ tree for the key; it does not assume the record remains
on the page changed by the original action.

Each inverse briefly pins and exclusively latches the pages it changes. Its
complete propagated physical effects are appended as one compensation log
record before those pages become flushable. A CLR is redo-only and carries
`undoNextLSN`, so recovery that crashes during rollback redoes completed
inverse actions and continues at the next transaction record still requiring
undo.

Structural propagation is not reversed to its former physical shape. If an
insert split a leaf and its transaction later aborts, logical undo deletes the
inserted key but may leave the split pages underfilled. They remain a valid B+
tree. The first implementation should avoid merge-on-delete so rollback does
not need to recreate an earlier tree shape.

Only after every inverse has a CLR and `TXN_END` is durable may
`TransactionManager` release the transaction's logical locks and acknowledge
abort. A transaction that logged no action has no inverse, so its `TXN_END` is
appended but not synchronized, for the same reason its commit is not. Page latches remain operation-scoped and are never retained through
client think-time.

## Autocommit Statements

A statement outside explicit `BEGIN` runs in a server-created transaction:

```text
begin transaction
    -> execute statement
    -> commit
    -> return result
```

On failure:

```text
begin transaction
    -> execute statement
    -> abort completely
    -> return error
```

The server may retry an autocommit statement after a deadlock when it knows
the attempt was completely aborted, still owns the entire operation, and has
not returned a result. The retry policy must eventually be bounded.

## Explicit Interactive Transactions

`BEGIN` creates a transaction and stores its ID in the client session. Each
later statement looks up that transaction in `TransactionManager` and passes
it to `KeyStore`:

```text
BEGIN        -> create T1; session.active_transaction = T1
GET A        -> KeyStore::get(T1, A)
PUT B, value -> KeyStore::put(T1, B, value)
COMMIT       -> commit T1; clear session transaction
```

Only one command for a transaction executes at a time. This keeps one
outstanding wait registration per transaction and prevents two connection
workers from concurrently mutating the same transaction context.

## Client-Side Retry

The server cannot transparently replay an arbitrary interactive transaction
after returning intermediate reads. If a client read `10`, computed `20`, and
later lost a deadlock, replaying only `PUT 20` would use stale input. The
complete callback must execute again and re-read the current value.

The client library should expose a callback wrapper:

```cpp
template<typename Function>
void run_transaction(Function operation) {
    for (;;) {
        begin();

        try {
            operation(*this);
            commit();
            return;
        } catch (const DeadlockVictim &) {
            rollback();
            // Run the complete callback again from BEGIN.
        }
    }
}
```

In this sketch, `rollback()` must be idempotent client/session cleanup because
the server has already completed the victim abort. The final client API may
instead expose a dedicated `clear_aborted_transaction()` operation.

The production wrapper must add:

- A bounded retry count.
- Backoff or jitter under repeated contention.
- A way to return the final deadlock error after retry exhaustion.
- Documentation that the callback must not repeat non-transactional external
  side effects such as sending email or charging a card.

An unknown commit result is different from a deadlock victim. If the client
loses its connection after sending `COMMIT`, it cannot blindly rerun the
transaction because the commit may have succeeded. That case requires request
identity and deduplication.

## Lock and Graph Synchronization

The initial inline detector may use the following lock order:

```text
one lock-shard mutex
    -> wait-for graph mutex
```

No graph operation may call back into `LockManager` or acquire a shard mutex
while retaining the graph mutex. Full abort must run after both mutexes have
been released.

Holding the shard mutex while calculating blockers and registering them keeps
the observed owner and queue state stable. Holding the graph mutex across
reachability checking and edge insertion keeps the acyclic-graph invariant
stable.

This ordering is a current design choice and must be covered by concurrency
tests before implementation is considered complete.

## Graph Cleanup Rules

Outgoing wait edges are removed when:

- The requested lock is granted.
- The request is cancelled.
- Registering the request would create a cycle.
- The transaction begins abort.

All remaining incoming and outgoing edges involving a transaction are removed
after it no longer owns locks and leaves the active-transaction table.

Because rigorous two-phase locking retains granted locks until transaction
end, an edge to an owner normally remains meaningful until that owner commits
or completes abort, unless the waiting request itself is granted or cancelled.

## Initial Result Types

The lock manager needs results that distinguish ownership from failure:

```cpp
enum class LockResult : std::uint8_t {
    Granted,
    AlreadyShared,
    AlreadyExclusive,
    DeadlockVictim,
    Cancelled,
};
```

For `lock_exclusive()`, finding the requester in `shared_owners` starts the
promotion procedure. The requester retains its shared ownership while queued,
is excluded from its own blocker set, and atomically replaces that ownership
with exclusive ownership after every other reader and earlier waiter clears.

## Deferred Decisions

- DFS versus BFS and graph test strategy.
- A later victim policy that may choose a transaction other than the
  requester.
- Deadlock timeout as a diagnostic fallback.
- Client retry count, backoff, and wire error format.
- Cancellation tokens and connection-shutdown behavior while waiting.
- Whether a future API permits parallel statements inside one transaction.
- Range-lock dependencies and phantom prevention.
