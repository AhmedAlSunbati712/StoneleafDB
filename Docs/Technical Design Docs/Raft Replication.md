# Raft Paper Notes
## Random ideas as I'm reading
- **Client contact with the leader node**: Instead of handling the logic of retries and contacting servers on the client side, we can build a uniform gateway in front of the cluster that the client talks to. This gateway will handle talking to the leader and retries by contacting followers to learn about the leader. It also provides nice separation between the storage engine and the replication layer. This can also introduce some complication i guess? I keep forgetting the deadlock handling model that we have. Does the server abort the transaction then redo it or does it ask the client explicitly to retry the transaction again? If it asks the client, then the replication layer has to catch that and retry instead of returning to the client and to avoid duplicate entries in the raft log. We are roughly going to follow a model similar to the one we already have the current server implementation. Thread per connection.
- **Network Resource Consumption with the gateway model**: This is actually not really that efficient since it will take up at least
- Read this comment in CommandServer.cpp:         // TODO this is an issue i believe if we end up implementing replication
        // We should retry inside the engine to avoid replicating the whole transaction
        // entry again which can cause redundancy.
        send_get_response(socket_fd, result);
        return;
        now a couple of things: If the deadlock came because of an earlier transaction that we are waiting on, then we can just wait on it to finish and redo our transaction and the order of the raft log is going to be the correct order of transactions.
        But now if it's deadlocked with a transaction. that came after it in the raft log, and this one was chosen to be aborted and retried
        then the order it will be applied will actually be after but the entry in the raft log is before it. So when other machines apply this entry in the raft log, it could apply it in a different order.
## Guaranteed Properties
- **Election Safety**: At most one leader can be elected in a given term.
- **Leader Append-Only**: A leader never overwrites or deletes entries int its log. It only appends entries
- **Log Matching**: If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index (Doesn't matter if the entry is not committed to a majority yet or not, whether it will be obsolete in a future term or not.)
- **Leader Completeness**: If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered-terms.
- **State Machine Safety**: If a server has applied a log entry at a given index to its stae machine, no other server will ever apply a different log entry for the same index. (I believe the reason is that only committed entries are applied to the state machine. Any other entry at the same index would have to be on a minority of servers, which wouldn't be able to get a quroum and therefore can't apply the index.)
## Raft Basics
A Raft cluster typically consists of five servers to tolerate up to two failures. A server is in one of three states:
- **Leader**: Accepts client requests, proposes new entries to the cluster, commits entries, and sends log entries to followers.
- **Follower**: Passive, doesn't accept client requests. If a client tries to connect to a follower server, it's redirected to server.
- **Candidate**: When the election timeout hits, a follower transitions to a candidate to elect a new leader. The timeout is reset everytime a:
    - Valid AppendEntries RPC from the current leader is received (term >= current term)
    - Granting its vote to a candidate.
Time in raft is divided into terms of arbitrary lengths called terms. Each term starts with an election. If an election results in a split vote, the term will end with no leader, one of the nodes will timeout and transition to becoming a candidate for the new term.
Terms serve as a logicla clock in Raft. Each server stores the current term number which increases monotonically over time:
- If one server's current term is smaller than the other's, it updates its current term to the larger value.
- If a candidate or a leader discovers its term is out of date, it immediately reverts to follower state.
- If a server receives a request with a stale term number, it rejects the request.

### RPCs
- `RequestVote` RPCs are initiated by candidates during elections.
- `AppendEntries` RPCs are initiated by leaders to replicate log entries and to also provide a heartbeat.
### Leader Election
When servers start up, they begin as followers. A server remains in follower state as long as it receives valid RPCs from a leader or a candidate. What counts as a valid RPC?
- A `RequestVote` from a candidate with a term >= current term and we haven't voted to someone else yet. Also, the candidate's log must be at least as up-to-date as the voter's own log
- An `AppendEntries` RPC from a valid leader (term >= current term).
To begin an election, a follower increments its current term and transitions to candidate state. It then votes for itself and issues `RequestVote` RPCs in parallel to each of the other servers in the cluster. A candidate remains in this state until one of three things happen:
- **It wins the election**: It receives votes from a majority of the servers in the full cluster for the same term. Each server will vote for at most one candidate in each term on a first-come-first-serve basis. Once a candidate wins an election, it becomes the leader. It then sends heartbeat messages to all of the other servers to establish authority.
- **Another server establishes itself as leader**: While waiting for votes, a candidate may receive `AppendEntries` RPC from a leader that claims to be from a termat least as large as the candidate's current term. The candidate then returns to follower state.
- **A period of time goes by with no winner**: The candidate neither wins nor loses the election. The votes could be split between multiple candidates. When this happens, candidates will timeout at different times and start the new election.
Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly. Usually selected from a fixed interval (150 - 300ms)
### Log Replication
Once a leader is selected, they start servicing client requests. Each client request contains a command. Whe a new command is sent:
1. The leader appends the command to its log as a new entry, then issues `AppendEntries` RPC to all of the followers in parallel to replicte the entry.
2. When the entry has been replicated, the leader applies the entry to its state machine and returns the result of that execution to the client.
3. If the followers crash or run slowly, of if network packets are lost, the leader retires `AppendEntries` RPCs indefinitely.
An entry that is replicated on a majority of servers is a committed entry. Raft guarantees that committed entries are durable and will eventually be executed by all of the available state machines.
Comitting an entry also commits all preceeding entries in the leader's log, including entries created by previous leaders.
The leader keeps track of the highest index it knows to be committed and it includes that index in the future `AppendEntries` RPCs.
If there's a backlog of unapplied entries, the server needs to apply them sequentially before responding to the client.
- The leader keeps track of the highest index committed and it sends it along with all of the subsequent `AppendEntries` RPCs so that the other servers eventually find out and apply the entries through that index to the state machine.
- If two entries in different logs have the same index and term, then they store the same command.
- If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.

During normal operaition, the logs of the leader and followers stay consistent. However, crashes could lead to any of the following scenarios when a leader wakes up:
- A follower could containt all the exact entries
- A follower could be missing entries
- A follower could contain extraneous entries
- A follower could be both missing and containing extranous entries
In Raft, the leader handles inconsistencies by forcing the followers' logs to duplicate its own. To bring a follower's log into consistency with its own, the leader must find the latest log entry where the two logs agree, delete any entries in the follower's log after that point, and send the follower all of the leader's entries after that point.
#### Follower's consistency check in `AppendEntries` RPC
1. Reply false if term < currentTerm (An outdated leader trying to communicate)
2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm. (Inconsistency detection, the leader has to go one index back and keep trying)
3. if an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it (if the prevLogTerm matches but the new entry doesn't match with the entry that exists in the followers log at that index, truncate and append)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

If the consistency check fails because of 2, the leader decrements nextIndex and retries the `AppendEntries` RPC. Eventually, nextIndex will reach a point wher ethe leader and the follower logs match.
On a succeesful `AppendEntries` RPC, the leader sets `matchIndex[i] = prevLogIndex + len(entries_sent)` and `nextIndex[i] = matchIndex[i] + 1`. After an election, the leader initializes `nextIndex` to `leader's last log index + 1` and `matchIndex` to 0. It's optimistic with respect to nextIndex and pessimistic with respect to matchIndex.
### Safety
There's an extra criteria that we need to impose on the eligiblity for leader election to ensure that the leader for any given term contains all of the entries committed in previous terms
#### Election Restriction
Raft uses the voting process to prevent a candidate from winning an election unless its log contains all committed entries. If the candidate's log is at least as up-to-date as any other log in that majority, then it will hold all the committed entries.
Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs have matching terms, then whichever one is longer is more up-to-date.
#### Safety argument
Check the proof on page 9.
At the time a server applies a log entry to its state machine, its log must be identical to the leader’s log up through that entry and the entry must be committed
# Raft Design
There are two approaches we could take:
- Decouple the replication layer completely the client and the storage server.
- Couple them and complicate things.
Let me describe the first one first
## Decoupled Raft Design
We can imagine that our CLI will have the follinwg usage:
```
stoneleafdb addr1 [...]
```
Where each argument is an ip:port address of the database replicas. If the addresses are local, the CLI will spin up a database engine process for each of the local addresses. Otherwise, it just saves these addresses if they are remote.
Now, the process that is started here will act as a gateway between connecting clients and the database replicas. For now, we will assume reads and writes go through the leader only. Later on we can try to handle reads from any server.
The reason for this is that the client doesn't have to know about the different addresses of the database replicas, track who is the leader, do retries when the leader gets network-partitioned from the majority of the cluster. The gateway can handle all of that.
The gateway holds the following state:
- `leader_addr`: The address of the leader server. Initialized to a random choice from the CLI args
- `node_addr[]`: A list of addresses of all the nodes in the cluster.
Ok my first critique of this approach was that it's wasting network resources. But let's actually think about it:
- Assume there are `n` nodes.
- Let there be `m` sessions open. Therefore, there are `m` connections between the clients and the gateway and `m` connections between the gateway and the leader node. That's `2m` connections
- Now, each node is connected to each other node which is `n(n+1)/2` nodes.
- Total number of connections is `2m + n(n+1)/2`

Do we think this abstraction is worth it or not? I believe it is honestly. It just strips away a lot of details from the client but I actually think it's redundant and not necessary.
Let's implement the retries and the reconnections on the client module. When we initialize the client, we just need to pass it the `node_addr[]` array and thats it. Ok, let's forget about this then.

## Coupling Raft Replication with the storage process
Which is the only way either way. The client will have a list of the nodes' client addresses (the `client_addr` column of the cluster config, never the Raft addresses) when connecting to the database. It will have the states `leader_addr` and `node_addr[]`. On the first request, it's going to mak a random connection to any of the nodes, set that address as the leader. If they reply with a different address, it will set that as the leader address and retry. On Every call, the client will set a timouet. When the timeout expires, it connects to a random node until it finds the new leader. If none of those work after a configurable number of tries, the cluster is down.
In terms of network connections, we will reuse the same model from before where a single session kicks off it's own thread.
### Client Messages
The client will send commands to the servers. Each message will be the command type and then the command payload which is just an array of bytes.
```
message ClientCommand {
    uint8 command_type
    bytes[] command_payload
}
```
Actually, let's not use protobufs for client server communication. Let's use a usual layout. We would still have the command type which is one byte, and the command payload which could be however many bytes. The message sent over the wire will be actually:
- Message Size: 8 bytes (this is the 1 byte + the number of bytes in command_payload)
- Command type: 1 byte
- Command Payload: However many bytes
The server will execute the following procedure on recieving messages from the clients
```
Parse the payload and extract the command
If there's no active current transaction:
    Create a Raft log entry with the command.
    Append it to the Raft Log
    Check if it's replicated on a majority
        apply to state machine and return success to the client
```
Now, there's an issue with this approach. The issue is that inside the if statement, we keep con checking if the entry is replicated on a majority of servers and there's no timeout control. How should the client timeout?
So let's think about this gradually. On the client side, users create sessions which establish connections.
Now, on every command we send, the interaction is as the following:
- Send the command on the session socket fd, and then listen for incoming bytes and read
We can set a timeout on the read loop such that if we don't receive a byte by the end of that timeout, we close the file descriptor and connect to a new one and retry.
```
Pseudocode of Client Sessions commands

received <- False
while (!received){
    send command
    received <- read_result
    if (!received) {
        reconncet_to_cluster
    }
}
```

```
Pseudocode of reconnect_to_cluster

close the current file descriptor
pick a random server address
connect to the file descriptor
leader_addr <- send a ping to check if its a leader or not
if leader_addr != address_chosen:
    close the file descriptor and connect to the new leader
```

Now, send command itself needs to reconnect to the leader if when it tries to send a message gets redirected. So in general, we need a helper called connect_to_address that will be used both inside reconnect to cluster and send command.
Ok so i think the approach should be the following:
- Ensure an entry is replicated on a majority of servers.
- Then send a message to reset the timeout on the session
- Then apply to the state machine and then return the result to the client session.
My initial thought was that after getting the majority to replicate the entry, we gotta wait until last_applied for this server be the index before the entry we are processing right now. But this kills multi-threading throughput in the engine since we are doing operations sequentially. However if we do the following, combined with the fact that an entry can't be committed unless all of its previous ones are also committed:
- Once you replicate start applying the command to the state machine
Since committing a record commits all records before it, we know the last applied will be caught up since all of them will kick off the applying of the record once they replicate. but what if it isn't? what if there's an entry that is replicated that doesn't have a connected client? then the last applied will lag a little bit behind and we need ti naje syre they are applied before this one is applied.
Instead of applying it ourselves, should we just have a thread that spins up other threads that apply log entries and we just check?
A background thread that wakes up every time last_applied lags behind commit_index and applies all of the backlog and spins up a thread for each entry, possibly with a bounded number of threads? we dont wanna overload the cpu. hmmm we already have a thread per connection/session. The thing is we can't rely on other connections to apply their entries. what if: Before a leader fully replicates an entry, it dies. then gets reelected and continues on resuming replicating that log now the original connection died and the apply step didnt get applied so last-applied is not updated and stuck. Therefore, we actually need a background routine that keeps on msking sure that last applied is caught up with commit index

Server Routines / Threads:
- Connection Acceptor
- Thread per connection. Accepts commands, replicates, and executes a command
- Apply Thread that makes sure last applied is caught up with commit index. Spins up a maximum of n threads to apply n entries. There are a maximum of n threads at any point.
### Transactions in Raft Replicated Database
Here I'm talking about explicit transactions and not implicit ones created for one-off commands not wrapped in a transaction. I was thinking about two different approaches to this:
#### One entry per transaction
Reads are served normally. Writes are not yet applied nor replicated to the database. As writes come in, we batch them together and group them into a struct or object:
```
TransactionWrite[]
TransactionWrite itself is a variant of TransactionPut or TransactionDelete
struct TransactionPut {
    key
    value
}

struct TransactionDelete {
    key
}
```
# Final Raft Replication Design
To replicate the database, we will be replicating the literal logical entries/commands in a transaction to other nodes. Let's discuss the lifecycle of a single command and the lifecycle of multiple commands wrapped in a transaction.
## Lifecycle of a single command
Consider the following command/transaction
```
BEGIN
put(k1,v1)
COMMIT
```
The client could either explicitly start a transaction or could just send this command and the engine will create a transaction automatically. Either way, it doesn't matter. What will be replicated will be a Raft entry that consists of this put operation. A raft entry will just be a vector of mutation operations:
```c++
enum class RaftMutationType : std::uint8_t {
    Put = 0,
    Delete
};

struct PutMutation {
    Key key;
    Value value;
};

struct DeleteMutation {
    Key key;
};

struct MutationOp {
    RaftMutationType type;
    std::variant<PutMutation, DeleteMutation> operation;
};

// Session-local write buffer. This is the single authoritative copy of a
// transaction's pending writes: it serves read-your-own-writes during
// execution and is serialized into the Raft entry at commit time, so the
// write set is never stored twice.
//
// nullopt is a tombstone (this transaction deleted the key), which is a
// different state from the key simply being absent from the map.
struct TransactionWriteBuffer {
    std::map<Key, std::optional<Value>> writes;

    // Collapses repeated writes to the same key into last-write-wins, so
    // put(k,1); put(k,2) produces a single MutationOp.
    std::vector<MutationOp> to_operations() const;
};

struct RaftMutationEntry {
    std::uint64_t term;
    std::uint64_t idx;
    std::vector<MutationOp> operations;
};
```
Each raft mutation entry is assumed to be an ACID transaction. The apply loop creates one local transaction per entry, purely so the mutations land in the WAL under a single commit record. That transaction takes no logical locks (see *Applying a committed Raft entry*).

So, the command will arrive on the socket associated with the session the client opened. The flow will depend on whether the client opened an explicit transaction or not. In this subsection, we will discuss implicit transactions since explicit transactions are identical to the next subsection. Once the entry arrives, the server will try to acquire exclusive lock on the logical key. Once it succeeds, it records the write in the session's `TransactionWriteBuffer` without touching the KeyStore. At commit time the buffer is serialized into a `std::vector<MutationOp>`, appended to the leader's own Raft log (which assigns the term and index), and the per-follower replication threads are signalled to propose it to the cluster.

The session thread does **not** advance `commit_index` itself. Advancing it is the replication loop's job (see *Replicating Raft entries*), for two reasons:

1. **Ownerless entries.** If each session only ever advanced `commit_index` for its own entry, an entry with no waiting session — one inherited from a previous leader, or one whose client disconnected — would never get committed, and if such entries sit at the tail of the log then `commit_index` stalls permanently.
2. **The current-term rule.** A leader may only advance `commit_index` by counting replicas for an entry from *its own term* (Raft §5.4.2, Figure 8). Committing an earlier term's entry by replica count can lose acknowledged data. That check needs the whole `replicated_index[]` array and the current term, which is state the replication loop owns.

So the session thread's only interaction with Raft state after proposing is to **wait until `last_applied >= entry.idx`**. It does not separately wait for a majority: an entry cannot be applied unless it was committed, and it cannot be committed without a majority, so the single wait on `last_applied` subsumes both. Once it wakes, the locks are released and the result is returned to the client.
Concretely, here's the full pseudocode:
```
parse the client command
acquire the logical locks necessary for the command
if lock acquisition failed (Deadlock victim):
    if there exists an explicit transaction:
        clean up its resources and release the locks
        clear the session's write buffer
    return failure status to the client

if the command is a read:
    if the key is present in the session's write buffer:
        serve it from the buffer (nullopt -> KeyNotFound)   // read-your-own-writes
    else:
        read through to the KeyStore

if the command is a write:
    record it in the session's write buffer (last-write-wins on the key)
    // nothing touches the KeyStore or the B-tree yet

if the command is an abort:
    clean up resources and release locks
    clear the session's write buffer

if the command is a commit or an implicit transaction command:
    lock(state_mutex)
    if state != Leader:                              // deposed mid-transaction
        unlock; release locks; clear buffer
        return {Failed, leader_client_address()}     // client address, not the Raft one
    operations  <- write_buffer.to_operations()
    my_term     <- current_term
    idx         <- RaftLog.append(my_term, operations)
    replication_cv.notify_all()
    result      <- await_commit(idx, my_term)        // see below; releases the lock while waiting
    unlock

    release locks
    clear the session's write buffer
    return result to the client

return to the user
```

### Checking leadership
A session that arrives at a follower is rejected at accept time, before a handler thread is spawned: the acceptor reads `state` and `leader_raft_address` under `state_mutex` and replies with `{Failed, leader_client_address()}` immediately. That keeps a follower from paying for a thread and a parse just to say "not me."

The redirect must carry the leader's **client** address. `leader_raft_address` is learned from `AppendEntries`, so it is the leader's Raft (gRPC) address; a client sent there would reach the gRPC port and fail the protocol. `leader_client_address()` translates it through `cluster_nodes` (see *Raft State*).

That check is a fast path, not the correctness-critical one. **Leadership can change mid-session** — a transaction may open while we are leader, buffer writes for seconds, and reach `COMMIT` after we have been deposed. So the propose path re-checks, and the re-check must be atomic with the append: verifying leadership, reading `current_term`, and appending happen under a single hold of `state_mutex`. Split them and a deposed server stamps an entry with a stale term, which a new leader then truncates while the session waits on an index that will never apply.

### Returning the commit result
The apply loop returns nothing to the session, because there is nothing for it to return. **Every client-visible result is already determined during execution, before the entry is proposed.** A read was answered from the write buffer or the KeyStore at the time it was issued; a write was recorded in the buffer and answered then; a deadlock victim failed before any entry existed. By the time an entry is committed, applying it is *mandatory* — every node must apply it or diverge — so there is no per-transaction failure the apply loop could report. It only signals **completion**, and `last_applied` already does that.

So there is no result map keyed by Raft index, and nothing to garbage-collect when a client disconnects. The session waits on `applied_cv` and derives its own answer:

```
await_commit(idx, my_term):              // caller holds state_mutex; releases it while waiting
    deadline <- now() + COMMIT_TIMEOUT
    loop:
        if last_applied >= idx:                    return Success
        if RaftLog.term_at(idx) != my_term:        return Failed     // truncated
        if now() >= deadline:                      return Unknown
        applied_cv.wait_until(lock, deadline)
```

The order matters. `last_applied >= idx` is checked **first**: once an entry has applied, it has applied, even if we were deposed a microsecond later.

The truncation case is worth the extra check because it upgrades an `Unknown` into a definite `Failed`. If the entry at our index no longer carries our term, a later leader overwrote it — and by Leader Completeness a *committed* entry is never overwritten, so observing the truncation proves the entry never committed. That is a strictly better answer for the client, since `Failed` is safe to retry and `Unknown` is not.

`RaftLog.term_at(idx)` must treat an index past the end of the log as "not our term" — truncation can shorten the log below `idx`.

**Losing leadership is deliberately not an exit condition.** It tells us nothing about the entry's fate: a new leader either already holds it, in which case it commits and replicates back to us and `last_applied` eventually passes `idx`, or it does not, in which case it is truncated. Either outcome is definitive and usually arrives within a heartbeat or two, and the deadline still bounds the wait. Exiting early on `state != Leader` would convert those into `Unknown` — the one answer a client without deduplication cannot act on — so the session stays parked and lets the cluster resolve it.

This costs nothing on the now-follower: the apply path takes no logical locks, so a stranded session holding key locks blocks neither the apply loop nor replication.

The truncation path in the `AppendEntries` receiver must therefore call `applied_cv.notify_all()`, or a session whose entry was just discarded sits until `COMMIT_TIMEOUT` and reports `Unknown` for something provably `Failed`.

**If the apply loop itself fails** — an I/O error, a full disk — that is not a transaction result. A committed entry must be applied on every node, so a node that cannot apply it cannot stay in the cluster. The apply loop halts the process rather than skipping the entry or reporting an error upward; recovery then replays from the Raft log on restart.
## Lifecycle of an interactive multi-command mutation Transaction
Basically the same as of the single command except that we don't propose the cluster unless a commit statement is reached. We allow **read-your-own-writes** (we will discuss reads in another subsection) from the session's `TransactionWriteBuffer`: a read first consults the buffer and falls through to the KeyStore on a miss. Note this is *not* a dirty read — a dirty read is seeing another transaction's uncommitted writes, which the locks held for the duration of the transaction specifically prevent. A transaction only ever sees its own uncommitted writes. New write operations are recorded in the `TransactionWriteBuffer` associated with this session. Locks are held for the whole duration until the transaction either fully commits and is applied to the state machine or is aborted. Furthermore, no changes are applied to the local state machine (the database) until the entry is replicated on a majority of servers
## Applying a committed Raft entry
We will have exactly one background thread on every server — leader and follower alike, running byte-identical code — that waits until `commit_index` is greater than `last_applied`. Each time it wakes it applies at most `MAX_APPLY_BATCH_SIZE` entries in a single run, creating a fresh local transaction per entry, iterating over that entry's operations, and committing.

Two properties this loop must have:

**The apply path must not acquire logical locks.** It does not reuse the leader's session transaction and there is no map from Raft index to transaction id. Such a map would be leader-local state, so the loop simply would not work on a follower, and it would make the leader's and followers' apply paths permanently different — the exact divergence this design exists to prevent.

The reason it must not acquire locks is *not* that no locks are involved. On the leader they very much are:

- **Write-write safety** comes from the apply thread being single-threaded and the only writer to the B-tree — sessions only ever touch their write buffer. This holds on every server.
- **Exclusion against readers is already established before the loop runs**, by different means depending on the role. On the **leader**, the proposing session still holds its locks and does not release them until `last_applied` passes its index. For the apply loop to write key `k`, that session must have held X(`k`), so no other session can hold any lock on `k` while the entry applies. On a **follower** there are no sessions and reads are leader-only, so there is nothing to exclude.

So the apply loop is not running lock-free — on the leader it runs *inside* an exclusion someone else already holds. If it tried to acquire X(`k`) itself it would block forever on the session's own X(`k`), which is exactly the self-deadlock this design has to avoid. Page latches still apply for physical consistency; only the `LockManager` is bypassed.

(The reader-exclusion argument holds while reads are leader-only. Serving reads from followers later would need MVCC or latch-level snapshots.)

**Interface.** Rather than a separate apply-only method on `KeyStore`, locking becomes a caller-supplied parameter on the existing methods. An enum rather than a bare `bool`, since `put(txn, k, v, false)` tells a reader nothing at the call site and this flag switches off a safety property:

```c++
enum class Locking : std::uint8_t { Acquire, Skip };

// Whether commit() makes its commit record durable before returning. Defer is
// for the apply loop only: commit() releases the transaction's locks, so a
// client transaction using it would release them before the commit is durable.
enum class Durability : std::uint8_t { Sync, Defer };

KeyStoreStatus put(
    const TransactionHandle &transaction,
    const Key &key,
    const Value &value,
    Locking locking = Locking::Acquire);

KeyStoreRemoveResult remove(
    const TransactionHandle &transaction,
    const Key &key,
    Locking locking = Locking::Acquire);
```

Defaulting to `Acquire` means every existing call site and every future one keeps the safe behavior without change. Dispatching on the mutation type is the apply loop's job, not the KeyStore's — it stays an inline switch in the loop:

```
for operation in raft_entry.operations:
    switch operation.type:
        case Put:    KeyStore.put(txn, operation.key, operation.value, Locking::Skip)
        case Delete: KeyStore.remove(txn, operation.key, Locking::Skip)
```

**The apply transaction is still a real transaction.** Skipping locks does not mean the handle is unnecessary — the handle is the WAL identity, not a lock scope. `WalRecord` is `{lsn, type, transaction_id, prev_lsn, data}`, and every field but `data` comes from the `Transaction`: `id_` tags the records so ARIES analysis can group them, and `last_lsn_` forms the backward chain undo walks, so `append_action` has nothing to chain without it. It is also what makes an entry **atomic**: if the server crashes after applying three of an entry's five operations, there is no `TxnCommit` record, so ARIES undoes the partial entry rather than leaving the database in a state no Raft index describes. It also carries the Raft index of the entry being applied, which is what lets recovery reconstruct `last_applied`:

```c++
struct CommitPayload { std::uint64_t raft_index; };   // was empty
```

The commit record is the right home for it rather than the record header: it is exactly the point at which an applied entry becomes durable, so "highest `raft_index` in a committed transaction" is precisely the watermark recovery needs, and it costs eight bytes per applied entry rather than eight per record. See *Seeding `commit_index` and `last_applied` on startup*.

One thing to verify when implementing: `TransactionManager::commit` calls `release_locks`, which for an apply transaction runs against an empty lock set. That should be a no-op, but confirm it does not assert.

**`last_applied` advances only after the WAL is synced.** Sessions wake on `last_applied >= idx` and immediately reply success to the client, so advancing it before `flush wal log` would report a commit that is not yet durable. The batch is applied, then flushed, then the watermark moves.

```
apply loop:
    wait until commit_index > last_applied
    batch_end <- last_applied
    num_applied <- 0
    while batch_end < commit_index && num_applied < MAX_APPLY_BATCH_SIZE:
        raft_entry <- RaftLog.read(batch_end + 1)
        txn <- TxnMgr.begin()          // fresh txn: WAL identity, no logical locks
        for operation in raft_entry.operations:
            switch operation.type:
                case Put:    KeyStore.put(txn, operation.key, operation.value, Locking::Skip)
                case Delete: KeyStore.remove(txn, operation.key, Locking::Skip)
        TxnMgr.commit(txn, Durability::Defer)   // no fsync yet; the batch syncs once below
        batch_end++
        num_applied++
    flush wal log
    // Durable only now, so only now may a waiting session reply success.
    last_applied <- batch_end
    notify waiting sessions
```
## The Raft Log
`RaftLog` is its own module with its own directory (`<db>.raft/` alongside `<db>.wal/`). It is not the WAL and cannot be folded into it: the WAL is append-only and authoritative for what *happened*, while the Raft log holds entries that are replicated but not yet committed and gets **truncated from the tail** as a matter of normal operation. One log cannot have both properties.

It does reuse the WAL logger's architecture, and Raft fits that architecture more naturally than the WAL does — Raft indices are dense contiguous integers starting at 1, which is exactly what `Index` already assumes.

### What is shared and what is not
The line is **whether the class knows what a record is**. The file layer does not and is shared outright; the record layer does and gets a parallel Raft implementation.

| Layer | Record-aware? | Disposition |
|---|---|---|
| `Store` | no — opaque length-prefixed payloads | **shared verbatim** |
| `Index` | no — dense ordinals mapped to byte offsets | **shared verbatim** |
| `Segment` | yes — `append(const WalRecord&)`, returns `WalRecord` | parallel Raft implementation |
| `Log` | yes — same, plus `truncate_suffix()` | parallel Raft implementation |
| `Config` | n/a | shared. `initial_lsn = 1`; index 0 means "no entry", matching the `prev_idx = 0` vacuous match |

`Index` is shared for exactly the reason `Store` is: nothing in its interface is WAL-specific. It takes a `std::uint32_t` ordinal and a `std::uint64_t` offset and does arithmetic on a file descriptor — its own header already calls the parameter an ordinal ("Segment must supply the current entry ordinal as `relative_lsn`"). `truncate_to()` likewise already provides the index-side half of `truncate_suffix()`; the WAL uses it to discard a derived suffix and Raft uses it to discard authoritative entries, but the operation is identical.

Sharing it costs a cosmetic rename — `relative_lsn` to `ordinal` — and correcting three doc comments that say "WAL index file". Five files reference it (`Segment.h`, `Config.h`, `Index.cpp`, and two tests), so this is worth doing now, as the second consumer appears, rather than later. It should also move out from under `include/Log/`, since it will no longer belong to the WAL.

The accepted tradeoff is coupling: a future WAL-motivated change to `Index` would reach Raft too. For a component this small and this stable that is preferable to maintaining two byte-identical files.

`Segment` and `Log` are deliberately parallel implementations rather than one templated over a record type. The record types differ and truncation exists on only one side; a single abstraction parameterized both ways is harder to read and change than two that merely look alike.

Segments pay off again later: log compaction is deleting whole segments from the front, so the mechanism is already in place when snapshots arrive.

### Entry format
The in-memory entry is the `RaftMutationEntry` already defined above, `{term, idx, operations}`. An **empty `operations` vector is a no-op entry**: it replicates and commits normally but applies nothing, which is the mechanism read handling will use to confirm `last_applied` is current. The Store payload encodes `{term, index, operations}`, with `term` and `index` first at fixed offsets and the variable-length operations after. The index is redundant with the entry's position and is stored anyway so the decoder can validate it, matching how the WAL validates the LSN carried in each record payload.

All integers are unsigned big-endian. The payload is operations-only and has
no command envelope, checksum, client ID, or request ID:

```text
u64 term
u64 index
u32 operation_count

repeated operation_count times:
    u8 mutation_type       // 0 = Put, 1 = Delete
    u8 key_type
    u32 key_size
    byte key[key_size]
    if mutation_type == Put:
        u8 value_type
        u32 value_size
        byte value[value_size]
```

Keys and values use their existing canonical encoded representations. The
decoder rejects zero terms and indexes, unknown mutation or logical types,
invalid key/value representations, impossible counts, truncated fields, and
trailing bytes. Request deduplication is deferred; adding it would require an
explicitly designed entry-format migration rather than silently changing this
layout.

### Interface
```c++
class RaftLog {
    public:
        void open(const std::string& directory);
        void close();

        // Appends one entry in `term` and returns the index assigned to it.
        // Used by the propose path, under state_mutex.
        std::uint64_t append(std::uint64_t term, std::vector<MutationOp> operations);

        // Appends entries received from a leader beginning at first_index.
        // The caller has already resolved conflicts via truncate_suffix().
        void append_from_leader(std::uint64_t first_index,
                                std::span<const RaftMutationEntry> entries);

        RaftMutationEntry              read(std::uint64_t index) const;
        std::vector<RaftMutationEntry> scan_from(std::uint64_t index) const;

        std::uint64_t last_index() const noexcept;
        std::uint64_t last_term()  const noexcept;

        // Returns 0 when index is 0 or beyond the end of the log. Terms start
        // at 1, so 0 is unambiguously "no entry here" and callers comparing
        // term_at(idx) against their own term get the right answer for a
        // truncated-away index without a separate bounds check.
        std::uint64_t term_at(std::uint64_t index) const;

        // Deletes the entry at from_index and every entry after it.
        void truncate_suffix(std::uint64_t from_index);

        void          sync_through(std::uint64_t index);
        std::uint64_t durable_index() const noexcept;
};
```

### Locating terms
`term_at(index)` reads the entry and returns its `term`, with one wrinkle: **it returns 0 when `index` is 0 or past the end of the log**, rather than throwing the way `read()` does. Terms start at 1, so 0 is unambiguously "no entry here". That is what lets the commit-wait predicate compare `term_at(idx)` against its own term and get the right answer for an index that has been truncated away, with no separate bounds check, and what makes `prev_idx = 0` a vacuous match in the replication thread.

No caching or index-side term column is warranted. The frequent callers are the replication thread's `prev_term` and `advance_commit_index()`, both driven by the heartbeat rather than by client load: with a 50ms heartbeat and four peers that is roughly 80 lookups per second, or about a tenth of a percent of one core. The other two callers, the commit-wait predicate and `last_term()` during elections, are rarer still.

Since the entry codec is being written anyway, putting `term` at a fixed offset at the front of the payload lets `term_at()` read eight bytes at a known position instead of decoding the whole `operations` vector. That costs nothing to design in and is the only concession worth making here.

### Tail truncation
`AppendEntries` rule 3 requires deleting a conflicting entry and everything after it. `truncate_suffix(from_index)` drops whole segments whose base index is at or above `from_index`, then truncates the segment containing it.

**Within a segment, truncate the Store first and the Index second.** This ordering is not arbitrary — it is the only crash-safe one, and it falls out of the invariant `Segment::recover()` already enforces, that the Store is authoritative and the Index is derived:

- **Store first, then Index.** A crash in between leaves an Index longer than its Store. That is exactly the state recovery already repairs: it locates the last matching Store/Index boundary and rebuilds only the inconsistent Index suffix, converging on the truncated state.
- **Index first, then Store.** A crash in between leaves a Store longer than its Index. Recovery rebuilds the Index from the authoritative Store and **the truncated entries come back**. That is a Raft safety violation, not merely a bug: entries a leader ordered us to discard reappear in our log.

Truncation must be durable before the `AppendEntries` reply that depends on it is sent, for the same reason the append is.

### Durability
One rule: **an entry must be durable before anything is told it exists.** Concretely:

- A **follower** calls `sync_through(last_new_index)` before replying `success = true`. Reply first and the leader may count us toward a majority for an entry we then lose in a crash.
- A **leader** calls `sync_through(idx)` before its own entry counts toward the majority in `advance_commit_index()`. The leader counts itself, so its own copy has to be as durable as any follower's.

Both sync **once per batch**, not per entry — an `AppendEntries` carrying twenty entries is one `fsync`, which is what makes throughput scale with load rather than collapse under it.

### Persisting `current_term` and `voted_for`
These are Raft's other durable state and they do not belong in either log — the Raft log gets truncated, the WAL is for the state machine, and these two fields are neither. They live in a small fixed-layout file in the same `<db>.raft/` directory:

```text
term (8, BE) | has_vote (1) | vote_len (4, BE) | vote bytes ("host:port")
```

`RaftHardStateStore` owns this file as `state`; its replacement file is
`state.tmp`. A missing `state` file means `{term = 0, voted_for = nullopt}`.
The persistence surface rejects decreasing terms and replacing or clearing an
existing vote within the same term. Repeating the same state is idempotent,
and a higher term may clear the prior term's vote.

It is rewritten in full on every change, which happens at most a few times per election — once when the term advances, once when a vote is granted. Write to a temporary file, `fsync`, `rename` over the original, then `fsync` the directory. That gives atomic replacement, so a crash mid-write leaves either the old contents or the new ones and never a torn record.

The write must complete **before** the RPC that depends on it is sent: before a `RequestVote` goes out with a new term, and before a `vote_granted = true` reply leaves the server. Advertise a term you then forget across a crash, and you can vote twice in it.

`RaftState` does these writes itself: `advance_term()`, `grant_vote()` and `become_candidate()` call `RaftHardStateStore::persist()` before returning, while the caller holds `state_mutex`. See *Locking discipline* for why the lock is held.

### Concurrency
`RaftLog` carries its own `shared_mutex`, so reads by the replication threads and the apply loop run concurrently with each other.

**Lock ordering is `state_mutex` → `raft_log_mutex`, always.** The propose path establishes it: it holds `state_mutex`, checks leadership, and calls `append()` inside that hold. Nothing may acquire the two in the opposite order. The place this would realistically go wrong is the `AppendEntries` receiver, where it is natural to consult the log for the consistency check before touching Raft state — take `state_mutex` first there, even for the read.

## Replicating Raft entries
This will be a background thread that loops just like the apply loop. This thread only runs on leaderd. It shouldn't run on followers. It's job is both sending heartbeats to servers and also sending them any raft log entries they might be missing. A couple of state variables that live on the leader server that we will need:
- `replicated_index[i]`: The highest index known to be replicated on server `i`. Initialized to all 0's
- `send_next[i]`: The first next Raft log index to send to the `ith` server. Notice that we might be sending multiple entries starting at this index

This must be **one long-lived thread per follower**, not a loop that spawns a fresh RPC thread per tick. If the tick period is shorter than RPC latency — which it will be during catch-up — spawning per tick puts multiple `AppendEntries` in flight to the same follower. Their replies can land out of order and both mutate `send_next[i]` and `replicated_index[i]`, so a late failure reply decrements `send_next` for a follower that has already caught up. That corrupts replication progress silently. One thread per follower keeps at most one RPC in flight per peer and makes the updates trivially serialized.

```
// One instance per follower i. Never more than one AppendEntries in flight
// to the same server.
replication thread for follower i:
    loop:
        wait until (there are entries to send to i) or (heartbeat interval elapsed)
        sent_term <- current_term

        if send_next[i] > 1:
            prev_idx  <- send_next[i] - 1
            prev_term <- RaftLog.term_at(prev_idx)   // in-memory; see Locating terms
        else:
            prev_idx  <- 0      // vacuous match: nothing precedes the first entry
            prev_term <- 0

        entries <- RaftLog.ScanRead(send_next[i])
        result  <- grpc_clients[i].append_entries(
                       sent_term, self_raft_address, prev_idx, prev_term, entries)

        if result.term > current_term:
            current_term <- result.term
            transition to follower
            continue

        // Drop stale replies. We may have changed term or lost leadership
        // while this RPC was in flight, in which case the reply says nothing
        // about the current term's replication progress and must not touch
        // replicated_index.
        if sent_term != current_term or state != Leader:
            continue

        if !result.success:
            send_next[i]--          // TODO: the paper's conflicting-term hint
            continue                //       makes this O(terms) not O(entries)

        replicated_index[i] <- prev_idx + len(entries)
        send_next[i]        <- replicated_index[i] + 1
        advance_commit_index()
```

`advance_commit_index()` is the single place `commit_index` moves. Sessions never write Raft state; they only wait on `last_applied`. Centralizing it here is what lets entries with no waiting session still get committed, and it is where the current-term rule is enforced:

```
advance_commit_index():
    for N from RaftLog.last_index() down to commit_index + 1:
        // Raft 5.4.2 / Figure 8: a leader may only commit by counting
        // replicas for an entry from its OWN term. Committing an earlier
        // term's entry this way can lose acknowledged data. Entries before
        // N commit transitively once N commits.
        if RaftLog.term_at(N) != current_term:    // in-memory; see Locating terms
            continue
        if a majority of replicated_index values >= N (counting the leader itself,
           which is not in the map):
            commit_index <- N
            notify the apply loop
            break
```
### Messages
```c++
struct AppendEntriesRequest {
    std::uint64_t term;            // leader's term
    NodeAddress   leader;          // leader's own configured address, verbatim
    std::uint64_t prev_index;      // index immediately preceding `entries`; 0 if none
    std::uint64_t prev_term;       // term at prev_index; 0 when prev_index is 0
    std::vector<RaftMutationEntry> entries;   // empty for a heartbeat
    std::uint64_t leader_commit;   // leader's commit_index
};

struct AppendEntriesResponse {
    std::uint64_t term;            // responder's current_term, so a stale leader steps down
    bool          success;         // false means the consistency check failed
};
```

### Receiving AppendEntries
```
on AppendEntries(term, leader, prev_index, prev_term, entries, leader_commit):
    lock(state_mutex)

    // 1. Stale leader. No timer reset - a deposed leader must not be able to
    //    suppress elections.
    if term < current_term:
        return {current_term, success = false}

    // The term is valid, so this is the current leader. Record it and reset the
    // timer BEFORE the consistency check: a log mismatch does not mean the
    // leader is dead, it means the leader is alive and repairing us.
    if term > current_term or state != Follower:
        become_follower(term, leader)          // resets timer, sets leader_raft_address
    else:
        leader_raft_address <- leader
        reset_election_timer()

    // 2. Consistency check. prev_index 0 is a vacuous match: nothing precedes
    //    the first entry. term_at() returns 0 past the end of the log, so a
    //    follower whose log is too short fails here without a bounds check.
    if prev_index > 0 and RaftLog.term_at(prev_index) != prev_term:
        return {current_term, success = false}     // leader decrements send_next and retries

    // 3. Truncate ONLY at a genuine conflict.
    //    Tracked as an offset into `entries`, not a log index, so no
    //    conversion arithmetic is needed below. entries[k] belongs at log
    //    index prev_index + 1 + k.
    new_from <- len(entries)                   // default: everything already present
    for k, entry in entries:
        idx      <- prev_index + 1 + k
        existing <- RaftLog.term_at(idx)
        if existing == 0:                      // past the end of our log
            new_from <- k
            break
        if existing != entry.term:             // conflict: discard this index and all after
            RaftLog.truncate_suffix(idx)
            applied_cv.notify_all()            // a just-deposed leader's sessions can now observe Failed
            new_from <- k
            break
        // already present with a matching term: leave it alone, keep scanning

    // 4. Append what is actually new. new_from == len(entries) means the
    //    leader sent nothing we do not already have - the duplicate-RPC and
    //    heartbeat case - so there is nothing to write.
    if new_from < len(entries):
        RaftLog.append_from_leader(prev_index + 1 + new_from, entries[new_from:])

    RaftLog.sync_through(prev_index + len(entries))    // durable BEFORE replying success

    // 5. Advance commit_index, bounded by what we actually hold.
    if leader_commit > commit_index:
        commit_index <- min(leader_commit, prev_index + len(entries))
        apply_cv.notify_one()

    return {current_term, success = true}
```

**Truncate only on a real conflict.** Rule 3 is *"if an existing entry conflicts with a new one — same index, different terms — delete it and all that follow."* Blindly calling `truncate_suffix(prev_index + 1)` on every request is the classic implementation bug: a duplicated or reordered `AppendEntries` would then delete entries the leader has already counted toward a majority, including committed ones. Entries that are already present with a matching term are left untouched, and only the suffix from the first genuine mismatch is replaced.

**A heartbeat is not a special case.** An empty `entries` vector still carries `prev_index`, `prev_term`, and `leader_commit`, so it still resets the timer, still runs the consistency check — and can still fail it, which is how a lagging follower gets discovered during idle periods — and still advances `commit_index`. There is no separate heartbeat path.

**The `min()` in step 5 matters.** `leader_commit` can be ahead of what this follower holds, because the leader commits as soon as a *majority* has an entry and this follower may not be in that majority. Taking the minimum keeps `commit_index` from running past the end of our log and handing the apply loop an index it cannot read.

**Durability precedes the reply**, per *Durability* above: reply `success = true` before `sync_through()` returns and the leader may count us toward a majority for an entry we lose in a crash. Truncation is covered by the same sync.

**A deliberate exception to the locking rule.** This handler holds `state_mutex` across `sync_through()`, which is I/O — one of the two places the rule under *Locking discipline* is broken on purpose (the other is persisting term and vote). Releasing the lock between the state decision and the log write would let a concurrent `AppendEntries` from a different term interleave its truncation with ours, and the check would no longer mean anything by the time we acted on it. The cost is acceptable because a follower has nothing else to do: its sessions are idle, other `AppendEntries` must serialize anyway, and the election timer was just reset so it cannot fire during the write. Lock ordering is still `state_mutex` → `raft_log_mutex`, which is exactly the inversion this handler would otherwise introduce by consulting the log before taking Raft state.

## Raft State
An object that will live on every server and used extensively through the controller layer (the client handler, replication and follower threads).

Three invariants are enforced through the method surface rather than left to callers, because all three are silent when violated:

1. **A vote and known leader belong to exactly one term.** `advance_term()` is the only way to change the term; it atomically replaces `voted_for` and clears `leader_raft_address`. Splitting these into independent fields makes it possible to carry stale election state into a new term — either granting a second vote or redirecting clients to a leader from an older term.
2. **Leader-only state is per-leadership, not per-process.** `send_next` and `replicated_index` are reinitialized by `become_leader()` on every election win. Initializing them once in the constructor leaves a re-elected leader reusing stale progress from its previous term.
3. **The cluster config is the only source of address spellings.** Address-as-identity means every comparison and every hash is a string comparison, so a node that spells itself two ways is two nodes. Rather than normalizing or resolving at runtime, the spellings are fixed once and never derived:

   - One cluster config, **byte-identical on all nodes**, listing every node as a pair: its Raft (gRPC) address and its client address. They are different ports, so they are different strings:

     ```text
     # raft_addr       client_addr
     10.0.0.1:5001     10.0.0.1:6001
     10.0.0.2:5001     10.0.0.2:6001
     10.0.0.3:5001     10.0.0.3:6001
     ```
   - Each node is launched with `--self` set to its own **client** address, which must match exactly one row's `client_addr`. That row's `raft_addr` becomes `self_raft_address`. `RaftState`'s constructor takes the full list plus that self address, asserts membership, partitions the Raft addresses into `self_raft_address` and `peers`, and builds `cluster_nodes`. A mismatch is a startup failure, not a runtime surprise.
   - **The Raft address is the node's identity**: `voted_for`, the `send_next` / `replicated_index` keys, the RPC sender field, and the vote file all use it. The client address is never compared or hashed; its only use is being handed to a client in a redirect, via `cluster_nodes`.
   - Every RPC carries the sender's own configured address, copied verbatim from that file.
   - **Addresses are never derived from a socket.** `getpeername()` is not used for identity anywhere. It returns `127.0.0.1:5001` on one connection and `localhost:5001` or `::ffff:127.0.0.1:5001` on another; those hash differently, so `voted_for == candidate` fails and the server grants a second vote in a term it has already voted in — two leaders, arriving through string comparison.

   With identical configs, exact string equality is always correct and no normalization or DNS resolution is needed. The membership check on inbound RPCs (`sender not in peers` -> reject) then earns its keep as a **config-drift detector**: with correct configs it can never fire, so if it does, two nodes disagree about the cluster and you want to know immediately rather than during an election.

**A note on the leader-only maps and threading.** `send_next` and `replicated_index` are read and written by one replication thread per peer, each touching only its own key. That is safe *only* because `become_leader()` inserts every key up front: modifying distinct elements of a container concurrently is not a data race, but an insertion can rehash and invalidate everything. So the replication threads must never insert. Use `.at()` rather than `operator[]` in those threads — `operator[]` default-constructs a missing key, which is exactly the insertion that breaks this, while `.at()` throws and surfaces the bug immediately.


```c++
// Nodes are identified by address rather than by a separate id, so this is
// also the type persisted in voted_for and sent on the wire.
struct NodeAddress {
    std::string host;     // exactly as written in the cluster config
    std::uint16_t port;

    bool operator==(const NodeAddress&) const = default;
    auto operator<=>(const NodeAddress&) const = default;

    // "host:port". Also the gRPC target, and the form RaftHardStateStore
    // persists voted_for in.
    std::string to_string() const;

    // Inverse of to_string(); splits on the last ':'. Used to read the cluster
    // config and the persisted vote. Must round-trip exactly -
    // from_string(s).to_string() == s - or config spellings drift.
    static NodeAddress from_string(std::string_view text);
};

// Required to key an unordered_map on NodeAddress.
template <>
struct std::hash<NodeAddress> {
    std::size_t operator()(const NodeAddress& a) const noexcept {
        std::size_t h = std::hash<std::string>{}(a.host);
        h ^= std::hash<std::uint16_t>{}(a.port) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// One row of the cluster config.
struct ClusterMember {
    NodeAddress raft;             // gRPC address; the node's identity
    NodeAddress database_server;  // where database clients connect; only ever handed to clients
};

enum class State : std::uint8_t {
    Leader = 0,
    Follower,
    Candidate,
};

class RaftState {
    public:
        // cluster is every row of the cluster config, including this node.
        // self_client_address is the --self flag: this node's database server
        // address. It must match exactly one row's database_server, or
        // construction throws; that row's raft address becomes
        // self_raft_address and every other row's raft address goes in peers.
        // current_term and voted_for are loaded from hard_state_store, which
        // must already be open and must outlive this object. last_applied
        // comes from ARIES recovery, and commit_index starts equal to it. The
        // node always starts as a Follower, and the constructor calls
        // reset_election_timer() so an existing leader gets one full timeout
        // to reach us before we campaign.
        RaftState(std::vector<ClusterMember> cluster,
                  const NodeAddress& self_client_address,
                  RaftHardStateStore& hard_state_store,
                  std::uint64_t last_applied);

        // Every method below requires the caller to hold state_mutex for its
        // whole duration. None of them perform I/O, except advance_term(),
        // grant_vote() and become_candidate(): they persist term and vote
        // through hard_state_store before returning, deliberately while the
        // caller holds state_mutex. See Locking discipline.

        // --- Term and vote --------------------------------------------------
        // The ONLY way to change the term. A vote is scoped to exactly one
        // term, so raising the term replaces it in the same operation;
        // exposing a bare term setter makes it possible to carry a stale vote
        // into a new term and grant a second vote in it, which allows two
        // leaders in one term. A new term also invalidates the prior leader.
        // Persists (new_term, new_vote) before returning.
        void advance_term(std::uint64_t new_term,
                          std::optional<NodeAddress> new_vote = std::nullopt);

        // Grants the vote if we have not voted for a different candidate this
        // term and the candidate's log is at least as up to date as ours:
        // a later last term wins; on equal terms, the longer or equal log
        // wins. Our own last index and term come from RaftLog, which the
        // caller reads while holding state_mutex (lock order: state_mutex ->
        // raft log mutex). On a grant, persists (current_term, candidate)
        // before returning, so the reply never leaves before the vote is
        // durable.
        bool grant_vote(const NodeAddress& candidate,
                        std::uint64_t candidate_last_log_index,
                        std::uint64_t candidate_last_log_term,
                        std::uint64_t own_last_log_index,
                        std::uint64_t own_last_log_term);

        // --- State transitions ----------------------------------------------
        // Atomically: advance the term by one, vote for ourselves, clear
        // votes, state = Candidate, reset_election_timer(). Persists the new
        // term and self-vote before returning, so no RequestVote can go out
        // carrying a term that is not durable.
        void become_candidate();

        // Clears and fully repopulates send_next (last_log_index + 1) and
        // replicated_index (0), one entry per peer. This must run on EVERY
        // election win, not once at construction: a server that leads in term
        // 5, steps down, and leads again in term 9 would otherwise reuse stale
        // progress from its first leadership, believe followers are further
        // along than they are, and skip entries they never received. Records
        // this node as the leader for the new leadership.
        void become_leader(std::uint64_t last_log_index);

        // If new_term > current_term, calls advance_term(), which persists.
        // Records the leader (nullopt when the higher term arrived on a
        // RequestVote or an RPC response), sets state = Follower, resets the
        // election timer, and notifies election_cv so a parked ex-leader
        // starts timing again.
        void become_follower(std::uint64_t new_term, std::optional<NodeAddress> leader);

        // --- Election timing ------------------------------------------------
        // election_deadline = now + random(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX).
        // become_candidate(), become_follower() and the constructor call it
        // internally, so the only explicit callers are the two handlers that
        // hear from a legitimate peer without changing state: AppendEntries
        // from the current leader while already a follower, and RequestVote
        // at the moment a vote is granted.
        void reset_election_timer();

        // --- Client redirects -----------------------------------------------
        // The leader's database server address:
        // cluster_nodes_.at(*leader_raft_address_), or nullopt when no leader
        // is known. Never hand leader_raft_address() itself to a client - it
        // is a gRPC address.
        std::optional<NodeAddress> leader_client_address() const;

        // --- Accessors ------------------------------------------------------
        // For the election thread, replication threads, apply loop, sessions
        // and RPC handlers. There are deliberately no setters for state,
        // current_term, voted_for, votes, or which peers the progress maps
        // hold: those change only through the methods above, which is how the
        // invariants under Raft State stay enforced.
        State state() const noexcept { return state_; }
        std::uint64_t current_term() const noexcept { return current_term_; }
        const std::optional<NodeAddress>& voted_for() const noexcept { return voted_for_; }
        const std::optional<NodeAddress>& leader_raft_address() const noexcept { return leader_raft_address_; }
        const NodeAddress& self_raft_address() const noexcept { return self_raft_address_; }
        const std::vector<NodeAddress>& peers() const noexcept { return peers_; }
        std::size_t cluster_size() const noexcept { return cluster_size_; }
        std::uint64_t commit_index() const noexcept { return commit_index_; }
        std::uint64_t last_applied() const noexcept { return last_applied_; }
        std::chrono::steady_clock::time_point election_deadline() const noexcept { return election_deadline_; }

        // Membership check for inbound RPCs. With byte-identical configs it can
        // never fail; if it does, two nodes disagree about the cluster.
        bool is_peer(const NodeAddress& address) const {
            return address != self_raft_address_ && cluster_nodes_.contains(address);
        }

        // Leader-only progress for one peer. .at() on purpose: become_leader()
        // inserts every key up front, and an unknown peer is a bug that must
        // throw rather than insert - an insertion can rehash the map while
        // another replication thread is using it.
        std::uint64_t send_next(const NodeAddress& peer) const { return send_next_.at(peer); }
        std::uint64_t replicated_index(const NodeAddress& peer) const { return replicated_index_.at(peer); }

        // Every peer's replicated index, for advance_commit_index()'s majority count.
        const std::unordered_map<NodeAddress, std::uint64_t>& replicated_indexes() const noexcept {
            return replicated_index_;
        }

        // --- Mutators -------------------------------------------------------
        // AppendEntries from the current leader while already a Follower in the
        // same term. A higher term goes through become_follower() instead.
        void set_leader_raft_address(const NodeAddress& leader) { leader_raft_address_ = leader; }

        void set_send_next(const NodeAddress& peer, std::uint64_t index) { send_next_.at(peer) = index; }
        void set_replicated_index(const NodeAddress& peer, std::uint64_t index) { replicated_index_.at(peer) = index; }

        // Both only move forward: committed entries stay committed, and entries
        // are applied in order and only once committed.
        void set_commit_index(std::uint64_t index) {
            assert(index >= commit_index_);
            commit_index_ = index;
        }
        void set_last_applied(std::uint64_t index) {
            assert(index >= last_applied_ && index <= commit_index_);
            last_applied_ = index;
        }

        // Records a granted vote from peer in the current campaign. Returns true
        // once we hold a majority, counting our own vote. votes_ is a set, so a
        // duplicate reply from the same peer cannot count twice.
        bool record_vote(const NodeAddress& peer) {
            votes_.insert(peer);
            return votes_.size() + 1 >= cluster_size_ / 2 + 1;
        }

        // --- Synchronization ------------------------------------------------
        // Public because callers lock state_mutex around multi-step sequences
        // (check leadership, read the term, append) and other threads wait on
        // the condition variables.
        //
        // One coarse mutex guards every field in this class. Critical sections
        // are a handful of integer reads and writes, so contention is not a
        // concern at this cluster size, and a single lock removes any question
        // of lock ordering between the session threads, the apply loop, the
        // replication threads and the RPC handlers. std::mutex rather than
        // std::shared_mutex because std::condition_variable only composes with
        // std::mutex.
        mutable std::mutex state_mutex;

        // Four condition variables rather than one, so a notification wakes
        // only the threads that can actually make progress. All share
        // state_mutex.
        std::condition_variable election_cv;    // election thread: deadline reached, or we stopped being Leader
        std::condition_variable apply_cv;       // apply loop: commit_index > last_applied (notify_one, single waiter)
        std::condition_variable applied_cv;     // sessions: last_applied >= their own idx (notify_all, many waiters)
        std::condition_variable replication_cv; // replication threads: entries appended, or we became leader (notify_all)

        // Set under state_mutex at shutdown, then every condition variable is
        // notified. Tested by EVERY wait predicate in the server: a thread
        // parked on a condition variable cannot be stopped any other way.
        bool shutting_down = false;

        // --- Timing constants -----------------------------------------------
        static constexpr auto ELECTION_TIMEOUT_MIN = std::chrono::milliseconds(150);
        static constexpr auto ELECTION_TIMEOUT_MAX = std::chrono::milliseconds(300);
        static constexpr auto HEARTBEAT_INTERVAL   = std::chrono::milliseconds(50);

    private:
        RaftHardStateStore& hard_state_store_;

        State state_ = State::Follower;

        // Persistent: written through hard_state_store_ before any RPC or reply
        // that depends on them leaves this server. current_term is NOT
        // derivable from the log - it is raised on becoming a candidate and on
        // seeing a higher term in any RPC, both of which happen without
        // appending an entry, so it can exceed the term of every entry we hold.
        std::uint64_t current_term_ = 0;
        std::optional<NodeAddress> voted_for_; // gRPC address of the node we voted for this term

        std::optional<NodeAddress> leader_raft_address_; // gRPC address of the leader; nullopt until we hear from one this term or become leader
        NodeAddress self_raft_address_;                   // gRPC address of this node; its identity
        std::vector<NodeAddress> peers_;                  // every other node's gRPC address, self excluded

        // gRPC address -> database server address, for every node including
        // ourselves. Built once from the cluster config; never modified.
        std::unordered_map<NodeAddress, NodeAddress> cluster_nodes_;
        std::size_t cluster_size_ = 0; // cluster_nodes_.size(): peers + ourselves. Used for majority

        // Leader-only. Cleared and fully repopulated by become_leader() on
        // every election win, never trusted across a leadership change. One
        // entry per peer, self excluded.
        std::unordered_map<NodeAddress, std::uint64_t> send_next_;        // next raft log index to send each peer
        std::unordered_map<NodeAddress, std::uint64_t> replicated_index_; // highest index known replicated on each peer

        // Peers that granted us a vote in the current campaign. Cleared by
        // become_candidate().
        std::unordered_set<NodeAddress> votes_;

        // Volatile in the paper, but our state machine is durable, so neither
        // starts at 0 - both are seeded from ARIES recovery.
        std::uint64_t commit_index_ = 0; // highest index known replicated on a majority
        std::uint64_t last_applied_ = 0; // highest index applied to the state machine

        // When the election thread starts an election if nothing resets it
        // first. Set by the constructor via reset_election_timer().
        std::chrono::steady_clock::time_point election_deadline_;

        // Redrawn on every reset, never drawn once at startup: a fixed per-node
        // timeout makes the same two nodes split the vote every round forever.
        // Seed from std::random_device per node - seeding every node identically
        // (a fixed seed in a test harness, or a default-constructed engine) makes
        // them all draw the same sequence and split the vote permanently.
        std::mt19937 timeout_rng_;
};
```

### Locking discipline
Two rules, both of which are load-bearing:

**Never hold `state_mutex` across I/O.** Mutate state under the lock, copy out whatever the call needs into locals, release, then do the network or disk work. `become_candidate()` runs under the lock; the `RequestVote` fan-out that follows does not. Holding it across an RPC serializes every session thread behind a network round trip, and it deadlocks the first time two nodes campaign at each other simultaneously.

**Two deliberate exceptions.** The `AppendEntries` receiver holds it across `sync_through()` (see *Receiving AppendEntries*). And `advance_term()`, `grant_vote()` and `become_candidate()` persist term and vote through `RaftHardStateStore` while the caller holds it. Persisting after unlocking would let a replication thread or another handler read the new term and put it in an RPC before it is on disk; a crash then brings the node back at the old term, free to vote a second time in the term it advertised. The cost is one small file rewrite and two fsyncs, a few times per election.

The apply loop follows the same shape, and it matters more there because its work is long:

```
apply loop:
    lock;   wait on apply_cv until commit_index > last_applied
            copy commit_index and last_applied into locals
    unlock                                  // <- B-tree work and fsync happen here
    ... apply the batch, flush wal ...
    lock;   last_applied <- batch_end
            applied_cv.notify_all()
    unlock
```

Holding the mutex through `flush wal log` would block every RPC handler, every session, and the election thread for the duration of an fsync — long enough to trigger a spurious election on the node doing the most useful work.

**The election timer is a deadline, not a cancellable timer.** `cv.wait_until` releases the mutex while waiting, so any thread can move `election_deadline` forward with a single assignment. The waiter wakes at whatever deadline it captured, re-reads the field, and waits again if it moved:

```
election thread:
    lock(state_mutex)
    loop:
        while state == Leader:
            election_cv.wait(lock)          // leaders send heartbeats instead
        deadline <- election_deadline
        if now() < deadline:
            election_cv.wait_until(lock, deadline)
            continue                        // re-read: it may have moved forward
        become_candidate()                  // bumps term, self-vote, resets timer
        copy term and last log index/term into locals
        unlock; fan out RequestVote; lock
```

No cancellation, no notification on reset, and no lost-wakeup race. The cost is one spurious wakeup per timeout period — four to six per second, which is nothing. `become_follower()` does need `election_cv.notify_all()`, because a stepping-down leader is parked in the `while (state == Leader)` wait and would otherwise never run an election again.

Use `steady_clock`, never `system_clock`: an NTP correction mid-wait must not be able to trigger or suppress an election.

### Seeding `commit_index` and `last_applied` on startup
The paper treats both as volatile and initializes them to 0, because it assumes the state machine is rebuilt from scratch or from a snapshot. Ours is a durable database, so both are seeded from recovery instead:

1. Run ARIES recovery against the WAL — analysis, redo, then undo. Any apply transaction without a `TxnCommit` record is rolled back, so the database never resumes mid-entry and a partially applied entry cannot survive.
2. `last_applied` <- the **highest** `raft_index` appearing in a *committed* apply transaction. Since apply is strictly sequential in index order, this coincides with the last committed apply transaction in the log. Zero when there are none, i.e. a fresh database.
3. `commit_index` <- `last_applied`.
4. `state` <- `Follower`, `leader_raft_address` <- `nullopt`.

Step 3 is safe because we only ever apply committed entries and committed entries stay committed (State Machine Safety), so `last_applied` is always a valid lower bound on `commit_index`.

It is *only* a lower bound. Entries above it may or may not be committed, and **nothing local can distinguish the two cases** — an entry from an older term sitting in our log might be committed, or might be one heartbeat away from being truncated. Only a leader knows, because only a leader holds `replicated_index[]`. So we never try to infer `commit_index` from our own log; it advances the moment we receive `AppendEntries.leaderCommit` from a leader, or derive it ourselves through `advance_commit_index()` after winning an election.

**We do not apply the log tail at startup.** Entries above `last_applied` are replicated but not necessarily committed — a follower appends entries long before they commit, and `AppendEntries` rule 3 truncates and replaces conflicting suffixes as a matter of normal operation, not as an error path. Applying them at boot would be unrecoverable: those apply transactions carry their own `TxnCommit` records, so ARIES will not undo them, and the node diverges permanently. The rule holds everywhere, startup included: **never apply past `commit_index`.**

This requires the `raft_index` field on `CommitPayload` described under *Applying a committed Raft entry*.

## Leader Election
Servers start as followers. A follower that reaches its election deadline without hearing from a leader becomes a candidate and starts an election. The timer itself — what resets it, and why it is a deadline rather than a cancellable timer — is described under *Locking discipline*; this section covers what happens once it fires.

### Messages
```c++
struct RequestVoteRequest {
    std::uint64_t term;            // candidate's term, already incremented
    NodeAddress   candidate;       // candidate's own configured address, verbatim
    std::uint64_t last_log_index;  // for the up-to-dateness check below
    std::uint64_t last_log_term;
};

struct RequestVoteResponse {
    std::uint64_t term;            // responder's current_term, so a stale candidate steps down
    bool          vote_granted;
};
```

### Becoming a candidate
`become_candidate()` runs under `state_mutex` and does four things atomically, which is why it is one method and not four calls: increment the term, vote for itself, set the state, and redraw the election deadline. Splitting them lets a server campaign without having voted for itself, or campaign twice in one term. It also persists the new term and self-vote through `RaftHardStateStore` before returning.

```
election thread, on deadline expiry:
    lock(state_mutex)
    become_candidate()          // advance_term(current_term + 1) clears voted_for,
                                // then voted_for <- self_raft_address, state <- Candidate,
                                // then reset_election_timer(). Persists the term and
                                // self-vote before returning: durable before any RPC goes out
    campaign_term   <- current_term
    last_log_index  <- RaftLog.last_index()
    last_log_term   <- RaftLog.last_term()
    unlock

    // I/O outside the lock. One thread per peer, all in parallel.
    for each peer in peers:
        send RequestVote(campaign_term, self_raft_address, last_log_index, last_log_term)
```

The persist inside `become_candidate()` is not optional. If we advertise term 7 and crash before the term reaches disk, we come back at term 6 and can vote a second time in term 7.

### Receiving a RequestVote
```
on RequestVote(term, candidate, cand_last_index, cand_last_term):
    lock(state_mutex)

    if candidate not in peers:
        reject                                  // config drift; see invariant 3

    if term < current_term:
        return {current_term, vote_granted = false}     // no timer reset

    if term > current_term:
        become_follower(term, nullopt)          // clears voted_for, resets timer

    // Checks voted_for and candidate_log_is_up_to_date(); on a grant, records
    // the vote and persists it before returning, so BEFORE the reply is sent.
    granted <- grant_vote(candidate, cand_last_index, cand_last_term,
                          RaftLog.last_index(), RaftLog.last_term())

    if granted:
        reset_election_timer()                  // only on grant, never on denial

    return {current_term, vote_granted = granted}
```

Granting again when `voted_for == candidate` is deliberate: it makes the RPC idempotent, so a retried `RequestVote` after a dropped response gets the same answer instead of a spurious denial.

**The up-to-dateness check** (Raft §5.4.1) is what guarantees a new leader holds every committed entry. Compare the *last entry* of each log — term first, length only as a tiebreak:

```
candidate_log_is_up_to_date(cand_last_index, cand_last_term):
    my_last_term <- RaftLog.last_term()
    if cand_last_term != my_last_term:
        return cand_last_term > my_last_term         // later term wins outright
    return cand_last_index >= RaftLog.last_index()   // same term: longer or equal wins
```

A longer log does **not** beat a higher last term. A candidate with fifty entries at term 3 is less up to date than one with two entries at term 5, because those fifty are uncommitted leftovers from a leader that was already superseded.

### Counting votes
```
on RequestVoteResponse(from peer, resp):
    lock(state_mutex)

    if resp.term > current_term:
        become_follower(resp.term, nullopt)     // we lost; stop counting
        return

    // Drop stale replies: we may have moved on since sending.
    if resp.term != campaign_term or state != Candidate:
        return

    if resp.vote_granted and record_vote(peer):  // true once votes + our own reach a majority
        win_election()
```

`votes` is a `std::unordered_set<NodeAddress>` scoped to one campaign, so a duplicate reply from the same peer cannot be counted twice. `become_candidate()` clears it.

### Winning
```
win_election():                                 // caller holds state_mutex
    become_leader(RaftLog.last_index())         // repopulates send_next / replicated_index
    replication_cv.notify_all()                 // heartbeat immediately, to suppress rival elections
```

**A new leader does not append a no-op entry.** The paper recommends one, but its purpose is linearizable reads — a leader cannot know which entries are committed until it has committed something in its own term — and it is not required for safety. The current-term rule in `advance_commit_index()` provides that on its own.

The consequence is worth recording, because it is confusing to encounter cold. A leader inheriting uncommitted entries from previous terms cannot commit them by counting replicas, so they sit replicated-but-uncommitted until *some* entry in the current term commits, which then commits them transitively. **The first client write of the new term clears the whole backlog**, so on an active cluster the window is milliseconds; it is only unbounded on an idle one. Nobody is waiting on those entries either — the sessions that proposed them belonged to the previous leader and are gone, and no client was ever told they succeeded, since a session replies only after `last_applied` passes its index.

During that window the new leader would serve reads that do not reflect those entries, which is why read handling proposes its own no-op entry and waits for it to apply before serving — that is where the mechanism belongs, and it is not designed yet.

### Losing, and stepping down
One rule, applied on **every** RPC and **every** RPC response, in every state:

```
if incoming.term > current_term:
    become_follower(incoming.term, leader_addr_if_this_was_AppendEntries)
```

That is the only way a leader or candidate steps down; there is no other path. A leader partitioned from the majority stays leader until it sees a higher term, which is the staleness window noted under *Reads*.

A candidate ends in exactly one of three states:
- **Wins** — a majority granted votes in `campaign_term`.
- **Loses** — receives `AppendEntries` from a leader whose term is `>= campaign_term`, and reverts to follower.
- **Neither** — the vote splits and nobody reaches a majority. The election deadline fires again, the timeout is redrawn from a fresh random value, the term increments, and it campaigns anew. Redrawing is what breaks the tie; a fixed per-node timeout makes the same split recur forever.

## Threads and Lifecycle
Every thread in the server, what it waits on, and when it exists.

| Thread | Count | Lifetime | Waits on | Woken by |
|---|---|---|---|---|
| Connection acceptor | 1 | whole process | `accept()` | a client connecting |
| Session | 1 per client connection | connection open → closed | the socket, then `applied_cv` while committing | client input; the apply loop; truncation |
| Apply loop | exactly 1 | whole process, every server | `apply_cv` | `advance_commit_index()`; step 5 of the `AppendEntries` receiver |
| Replication | 1 per peer | whole process, **parked while not leader** | `replication_cv`, with a `HEARTBEAT_INTERVAL` timeout while leading | `become_leader()`; a new entry appended |
| Election timer | 1 | whole process, **parked while Leader** | `election_cv`, with `election_deadline` as timeout | `become_follower()`; the deadline elapsing |
| RPC handlers | gRPC's pool | whole process | gRPC | inbound `AppendEntries` / `RequestVote` |

### Replication threads are parked, not spawned and joined
They are created once at startup — one per peer, on every server — and park on `replication_cv` whenever `state != Leader`. `become_leader()` notifies them; `become_follower()` does nothing, because each thread re-checks its own predicate on the next wake and parks itself.

The alternative, spawning them in `become_leader()` and joining them in `become_follower()`, is worse on every axis that matters here. Joining means blocking a state transition until each thread returns from whatever it is doing, and what it is doing is frequently an in-flight RPC to an unreachable peer — so the step-down stalls for the RPC timeout, on the path that most needs to be fast. Election churn is exactly the situation where leadership flips repeatedly, and layering thread churn on top of it multiplies the failure modes. A thread blocked on a condition variable costs a stack and no CPU.

Their wait predicate carries both conditions:

```
replication thread for peer i:
    loop:
        lock(state_mutex)
        while state != Leader and not shutting_down:
            replication_cv.wait(lock)                       // parked: not leading
        if shutting_down: return
        // Leading: wait for work, but wake for the heartbeat regardless.
        if nothing to send to peer i:
            replication_cv.wait_for(lock, HEARTBEAT_INTERVAL)
        ... existing body: read prev_term, ScanRead, unlock, send RPC ...
```

A step-down while an RPC is in flight needs no special handling: the reply arrives, and the `sent_term != current_term or state != Leader` guard already discards it.

### Startup order
Order matters in two places, so this is a sequence and not a set:

1. ARIES recovery over the WAL — the database reaches a transactionally consistent state.
2. Open `RaftLog` and `RaftHardStateStore`.
3. Construct `RaftState` from the cluster config, `--self`, and the open `RaftHardStateStore` (it loads `current_term` and `voted_for` itself), seeding `last_applied` and `commit_index` per *Seeding `commit_index` and `last_applied` on startup*. State is `Follower`.
4. Start the apply loop.
5. Start the RPC server. **Before step 7** — a server that campaigns before it can receive `RequestVote` replies cannot win, and worse, cannot answer its peers.
6. Start the replication threads. They park immediately, since we begin as a follower.
7. Start the election timer. This is the first moment the server can campaign, so nothing that a campaign depends on may start after it.
8. Start the connection acceptor. **Last** — clients must not connect before the node can serve them.

### Shutdown order
Shutdown needs a `shutting_down` flag on `RaftState` that **every** wait predicate tests, since a thread parked on a condition variable cannot be interrupted any other way. Set it under `state_mutex` and notify all four condition variables.

1. Stop the acceptor; refuse new connections.
2. Set `shutting_down`, notify `election_cv`, `replication_cv`, `apply_cv`, `applied_cv`.
3. Session threads wake. Any parked in `await_commit` return `Unknown` — the entry may well still commit elsewhere, and that is the honest answer. Roll back their open transactions and release their locks.
4. Join the replication threads. They are parked or between RPCs, so this is bounded.
5. Join the election thread.
6. Let the apply loop **finish its current batch and `flush wal log`** before exiting. Cutting it off mid-batch is safe — the entries are still in the Raft log and replay on restart — but finishing avoids redoing the work.
7. Close `RaftLog`, then the WAL and `KeyStore`.

### Failure handling
Threads differ in how they treat errors, and the difference is deliberate:

- **The apply loop halts the process** on an I/O failure. A committed entry must be applied on every node, so a node that cannot apply one cannot remain in the cluster. It does not skip the entry or report upward. Restart replays it from the Raft log.
- **Replication threads absorb RPC failures.** An unreachable peer is normal operation, not an error: the thread retries on its next heartbeat, and `send_next[i]` is unchanged by a failed send.
- **Session threads absorb their own errors.** A client disconnecting mid-transaction aborts it and releases its locks, as `serve_connection` already does today.

## Gaps not Filled yet
- **Commit Status wire encoding**: `CommitResult { Success, Failed, Unknown }` and where each value is produced are now designed under *Returning the commit result*. What remains is purely the wire format: how the three values plus an optional redirect to the leader's client address (`leader_client_address()`, never the Raft `leader_raft_address`) are encoded in the client response, alongside the existing size-prefixed command protocol.
