#pragma once
#include <Raft/ClusterMember.h>
#include <Raft/NodeAddress.h>
#include <Raft/RaftHardStateStore.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

        // --- Read-index confirmation ----------------------------------------
        // A consistent read may not be answered from local state until a
        // majority has confirmed, since the read arrived, that this node is
        // still leader: an isolated leader is never told it was deposed, so
        // its own state proves nothing. Confirmation is a round of ordinary
        // heartbeats, identified by a counter so that a reply to a round that
        // started BEFORE the read cannot be mistaken for proof.
        //
        // Rounds and acknowledgements are per leadership and never persisted.
        std::uint64_t read_round() const noexcept { return read_round_; }
        std::uint64_t confirmed_read_round() const noexcept { return confirmed_read_round_; }
        std::uint64_t acked_read_round(const NodeAddress& peer) const { return acked_read_round_.at(peer); }

        // The round a read arriving now must wait for: rounds are opened by
        // the replication threads, not by readers, so every read that arrives
        // before the next heartbeat wave shares one round and one set of
        // replies. A reader asks for a round and waits for this number.
        std::uint64_t next_read_round() const noexcept { return read_round_ + 1; }

        // Asks the replication threads to open a round. They call
        // open_read_round() when they build their next request; the caller
        // notifies replication_cv so that happens now rather than at the next
        // HEARTBEAT_INTERVAL.
        // Returns true if this call is what marked a round wanted, so only one
        // reader of a wave notifies the replication threads: with dozens of
        // readers, notify_all() per reader is itself enough contention on
        // state_mutex to delay heartbeats.
        bool request_read_round() noexcept {
            const bool newly_requested = !read_round_wanted_;
            read_round_wanted_ = true;
            // A single-node cluster has no peer to ask: the leader alone is the
            // majority, so the round opens and confirms immediately.
            if (cluster_size_ == 1) {
                ++read_round_;
                confirmed_read_round_ = read_round_;
                read_round_wanted_ = false;
                read_cv.notify_all();
            }
            return newly_requested;
        }
        bool read_round_wanted() const noexcept { return read_round_wanted_; }

        // Called by a replication thread as it builds a request: opens the
        // requested round, if one was requested, and returns the round this
        // request should carry. The first thread to call it opens the round;
        // the others carry the same number, so their replies credit it too.
        std::uint64_t open_read_round() noexcept {
            if (read_round_wanted_) {
                ++read_round_;
                read_round_wanted_ = false;
            }
            return read_round_;
        }

        // One peer's reply to round `round`. A reply proves that peer still
        // recognized this term, which is what leadership confirmation needs -
        // whether or not the AppendEntries itself succeeded. Advances
        // confirmed_read_round_ once this node plus the acking peers form a
        // majority, and notifies the readers waiting on it.
        void record_read_ack(const NodeAddress& peer, std::uint64_t round) {
            std::uint64_t& acked = acked_read_round_.at(peer);
            if (round <= acked) return;
            acked = round;

            // The highest round a majority holds: with the leader counted, that
            // is the (majority - 1)-th largest peer acknowledgement.
            std::vector<std::uint64_t> rounds;
            rounds.reserve(acked_read_round_.size());
            for (const auto& [address, value] : acked_read_round_) rounds.push_back(value);
            std::sort(rounds.begin(), rounds.end(), std::greater<>());
            const std::size_t peers_needed = cluster_size_ / 2 + 1 - 1;
            if (peers_needed == 0 || peers_needed > rounds.size()) return;
            const std::uint64_t confirmed = rounds[peers_needed - 1];
            if (confirmed > confirmed_read_round_) {
                confirmed_read_round_ = confirmed;
                read_cv.notify_all();
            }
        }

        // The index of the no-op this leadership appended on winning. Until it
        // commits, commit_index_ can lag the true committed prefix (only an
        // entry from the leader's own term commits by replica count, Figure 8),
        // so a read taken before then could miss a committed write. 0 means
        // the leader has not appended it yet.
        std::uint64_t leader_term_first_index() const noexcept { return leader_term_first_index_; }
        void set_leader_term_first_index(std::uint64_t index) noexcept {
            leader_term_first_index_ = index;
        }

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

        // Serializes writers of the Raft log's tail: the propose path's
        // append, and the AppendEntries receiver's truncate-and-append. Lock
        // order is append_mutex -> state_mutex.
        //
        // It exists so the leader's append - a write, which on a shared volume
        // can stall behind another file's full sync for milliseconds - is not
        // done under state_mutex, which heartbeats, replication and commit
        // advancement all need. The propose path checks leadership and reads
        // the term under state_mutex while holding this, then appends holding
        // only this. If leadership is lost in between, the entry carries a
        // stale term; no newer-term entry can precede it, because only the
        // AppendEntries receiver writes those and it waits here. A new leader
        // either overwrites the entry or commits it with its own, and
        // await_commit reports either outcome correctly.
        std::mutex append_mutex;

        // Four condition variables rather than one, so a notification wakes
        // only the threads that can actually make progress. All share
        // state_mutex.
        std::condition_variable election_cv;    // election thread: deadline reached, or we stopped being Leader
        std::condition_variable apply_cv;       // apply loop: commit_index > last_applied (notify_one, single waiter)
        std::condition_variable applied_cv;     // sessions: last_applied >= their own idx (notify_all, many waiters)
        std::condition_variable replication_cv; // replication threads: entries appended, or we became leader (notify_all)
        std::condition_variable read_cv;        // readers: a read round was confirmed, or commit_index moved (notify_all)

        // Set under state_mutex at shutdown, then every condition variable is
        // notified. Tested by EVERY wait predicate in the server: a thread
        // parked on a condition variable cannot be stopped any other way.
        bool shutting_down = false;

        // --- Timing constants -----------------------------------------------
        static constexpr auto ELECTION_TIMEOUT_MIN = std::chrono::milliseconds(150);
        static constexpr auto ELECTION_TIMEOUT_MAX = std::chrono::milliseconds(300);
        static constexpr auto HEARTBEAT_INTERVAL   = std::chrono::milliseconds(50);

        // How long a proposing session waits for its entry to apply before it
        // gives up and answers Unknown. Generous relative to the election
        // timeouts on purpose: an entry proposed just before a leader change is
        // usually resolved - committed by the next leader, or truncated - within
        // a heartbeat or two of the new leadership, and reporting Unknown for
        // something that was about to become definite is the worst answer a
        // client without deduplication can receive.
        static constexpr auto COMMIT_TIMEOUT = std::chrono::milliseconds(5000);

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
        // Leader-only, per leadership: read-index confirmation rounds.
        std::unordered_map<NodeAddress, std::uint64_t> acked_read_round_;
        std::uint64_t read_round_ = 0;
        bool read_round_wanted_ = false;
        std::uint64_t confirmed_read_round_ = 0;
        std::uint64_t leader_term_first_index_ = 0;

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
