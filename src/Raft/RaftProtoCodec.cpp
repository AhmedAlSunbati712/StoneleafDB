#include <Raft/RaftProtoCodec.h>

#include <KeyCodec.h>
#include <ValueCodec.h>

#include <limits>
#include <stdexcept>

namespace RaftProtoCodec {
    void to_proto(const Key& key, stoneleaf::raft::ProtoKey* out) {
        out->set_type(static_cast<std::uint32_t>(key.type));
        out->set_data(key.data.data(), key.data.size());
    }
    void to_proto(const Value& value, stoneleaf::raft::ProtoValue* out) {
        out->set_type(static_cast<std::uint32_t>(value.type));
        out->set_data(value.data.data(), value.data.size());
    }
    void to_proto(const MutationOp& operation, stoneleaf::raft::Mutation* out) {
        if (operation.type == RaftMutationType::Put) {
            out->set_type(stoneleaf::raft::MUTATION_PUT);
            const PutMutation& op = std::get<PutMutation>(operation.operation);
            stoneleaf::raft::ProtoKey* pk = out->mutable_key();
            stoneleaf::raft::ProtoValue* pv = out->mutable_value();
            to_proto(op.key, pk);
            to_proto(op.value, pv);

        } else if (operation.type == RaftMutationType::Delete) {
            out->set_type(stoneleaf::raft::MUTATION_DELETE);
            const DeleteMutation& op = std::get<DeleteMutation>(operation.operation);
            stoneleaf::raft::ProtoKey* pk = out->mutable_key();
            to_proto(op.key, pk);
        }
    }

    void to_proto(const RaftMutationEntry& entry, stoneleaf::raft::RaftEntry* out) {
        out->set_term(entry.term);
        out->set_idx(entry.idx);
        for (const MutationOp& operation : entry.operations) {
            stoneleaf::raft::Mutation* p_operation = out->add_operations();
            to_proto(operation, p_operation);
        }
    }


    Key from_proto(const stoneleaf::raft::ProtoKey& key) {
        // uint32 on the wire, uint8 in the domain: check the range before
        // narrowing. A plain cast would turn a newer peer's unknown KeyType
        // into a valid-looking one and decode the bytes wrongly.
        if (key.type() > static_cast<std::uint32_t>(KeyType::Bytes)) {
            throw std::out_of_range("Raft entry carries an unknown KeyType");
        }
        if (key.data().size() > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("Raft entry key is too large");
        }

        Key decoded{
            .type = static_cast<KeyType>(key.type()),
            // Recomputed, never taken from the wire: protobuf's own length
            // prefix is the only length, so the two can never disagree.
            .size = static_cast<std::uint32_t>(key.data().size()),
            .data = std::vector<char>(key.data().begin(), key.data().end()),
        };

        // The encoded bytes still have to be a valid key of that type - the
        // same gate WalPayloadCodec::read_key uses.
        if (!KeyCodec::validate_key(decoded)) {
            throw std::invalid_argument("Raft entry carries an invalid key");
        }
        return decoded;
    }

    Value from_proto(const stoneleaf::raft::ProtoValue& value) {
        if (value.type() > static_cast<std::uint32_t>(ValueType::Char)) {
            throw std::out_of_range("Raft entry carries an unknown ValueType");
        }

        if (value.data().size() > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("Raft entry value is too large");
        }

        Value decoded {
            .type = static_cast<ValueType>(value.type()),
            .size = static_cast<std::uint32_t>(value.data().size()),
            .data = std::vector(value.data().begin(), value.data().end()),
        };

        if (!ValueCodec::validate_value(decoded)) {
            throw std::invalid_argument("Raft entry carries an invalid value");
        }

        return decoded;
    }

    MutationOp from_proto(const stoneleaf::raft::Mutation& operation) {
        // MutationType_IsValid is generated for exactly this: proto3 enums are
        // open, so an unknown value from a newer peer arrives intact. Comparing
        // against RaftMutationType instead would be wrong twice over - the two
        // enums do not share values (MUTATION_DELETE is 2, Delete is 1) - and
        // narrowing first would fold 256 onto 0.
        if (!stoneleaf::raft::MutationType_IsValid(operation.type())) {
            throw std::out_of_range("Raft entry carries an operation with an unknown type");
        }

        // Exceptions from the key and value conversions propagate untouched:
        // catching them only to rethrow would slice off the derived type and
        // the message.
        Key key = from_proto(operation.key());

        switch (operation.type()) {
            case stoneleaf::raft::MUTATION_PUT: {
                // Without this an absent value decodes as a default ProtoValue
                // rather than being rejected.
                if (!operation.has_value()) {
                    throw std::invalid_argument("Raft entry carries a put with no value");
                }
                return MutationOp{
                    .type = RaftMutationType::Put,
                    .operation = PutMutation{
                        .key = std::move(key),
                        .value = from_proto(operation.value()),
                    },
                };
            }
            case stoneleaf::raft::MUTATION_DELETE: {
                // A value on a delete is ignored rather than rejected: the
                // tombstone is unambiguous either way.
                return MutationOp{
                    .type = RaftMutationType::Delete,
                    .operation = DeleteMutation{.key = std::move(key)},
                };
            }
            default:
                // MUTATION_UNSPECIFIED, and anything added to the proto that
                // this build has no case for. Without a default, control could
                // run off the end of the function.
                throw std::out_of_range("Raft entry carries an operation with an unhandled type");
        }
    }

    RaftMutationEntry from_proto(const stoneleaf::raft::RaftEntry& entry) {
        RaftMutationEntry raft_entry{
            .term = entry.term(),
            .idx = entry.idx(),
            .operations = std::vector<MutationOp>{}
        };
        // reserve, not resize: resize would default-construct one operation
        // per mutation and push_back would then append the real ones, leaving
        // twice as many with empty puts in front.
        raft_entry.operations.reserve(entry.operations().size());
        for (const stoneleaf::raft::Mutation& mutation : entry.operations()) {
            raft_entry.operations.push_back(from_proto(mutation));
        }
        return raft_entry;
    }


}