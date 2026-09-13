#pragma once

#include <Key.h>
#include <Raft/RaftEntry.h>
#include <Value.h>

#include <raft.pb.h>

// The boundary between the wire and the domain types. This is the ONLY header
// under include/ that may include raft.pb.h: anything that includes this one
// inherits a dependency on -Ibuild/gen and the protobuf headers, so RaftState,
// RaftLog and the apply loop must never include it.
//
// Direction matters, and the signatures say which is which:
//
//   to_proto   - domain -> proto. Total; cannot fail. Takes a pointer because
//                protobuf hands out mutable submessages (add_operations(),
//                mutable_key()) and they cannot be returned by value.
//   from_proto - proto -> domain. Partial; throws std::out_of_range or
//                std::invalid_argument on anything the domain types cannot
//                represent: an unknown MutationType, KeyType or ValueType, a
//                MUTATION_PUT carrying no value, or a key or value that fails
//                its own codec's validation. Nothing half-built escapes, since
//                the object is only returned once it is complete.
//
// RPC handlers catch those and return grpc::StatusCode::INVALID_ARGUMENT: a
// malformed peer message is a rejected RPC, never a partially applied entry.
namespace RaftProtoCodec {

void to_proto(const Key& key, stoneleaf::raft::ProtoKey* out);
void to_proto(const Value& value, stoneleaf::raft::ProtoValue* out);
void to_proto(const MutationOp& operation, stoneleaf::raft::Mutation* out);

// Writes term, idx and every operation. The index is carried explicitly rather
// than inferred from position so the receiver can validate it.
void to_proto(const RaftMutationEntry& entry, stoneleaf::raft::RaftEntry* out);

// size is recomputed from data.size(); no length is ever trusted from the wire.
Key from_proto(const stoneleaf::raft::ProtoKey& key);
Value from_proto(const stoneleaf::raft::ProtoValue& value);

// Validates the type with MutationType_IsValid before dispatching: proto3 enums
// are open, so an unknown value from a newer peer arrives intact and a plain
// cast would silently select no case and mis-apply the entry on this node only.
MutationOp from_proto(const stoneleaf::raft::Mutation& operation);

RaftMutationEntry from_proto(const stoneleaf::raft::RaftEntry& entry);

} // namespace RaftProtoCodec
