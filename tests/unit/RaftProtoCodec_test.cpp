#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <Raft/RaftProtoCodec.h>
#include <ValueCodec.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

namespace raftpb = stoneleaf::raft;

MutationOp put_op(const Key& key, const Value& value) {
    return {.type = RaftMutationType::Put, .operation = PutMutation{key, value}};
}

MutationOp delete_op(const Key& key) {
    return {.type = RaftMutationType::Delete, .operation = DeleteMutation{key}};
}

const Key& sample_key() {
    static const Key key = KeyCodec::make_string("k1");
    return key;
}

const Value& sample_value() {
    static const Value value = ValueCodec::make_char("v1");
    return value;
}

TEST(RaftProtoCodecTest, RoundTripsEveryKeyType) {
    const std::vector<Key> keys{
        KeyCodec::make_bool(true),
        KeyCodec::make_uint64(4),
        KeyCodec::make_int64(-4),
        KeyCodec::make_string("x"),
        KeyCodec::make_bytes({0, -1, 7}),
    };

    for (const Key& key : keys) {
        raftpb::ProtoKey encoded;
        RaftProtoCodec::to_proto(key, &encoded);
        const Key decoded = RaftProtoCodec::from_proto(encoded);

        EXPECT_EQ(decoded.type, key.type);
        EXPECT_EQ(decoded.size, key.size);
        EXPECT_TRUE(KeyCodec::equal(decoded, key));
    }
}

TEST(RaftProtoCodecTest, RoundTripsEveryValueType) {
    const std::vector<Value> values{
        ValueCodec::make_varuint(4),
        ValueCodec::make_varint(-4),
        ValueCodec::make_bool(true),
        ValueCodec::make_char("x"),
    };

    for (const Value& value : values) {
        raftpb::ProtoValue encoded;
        RaftProtoCodec::to_proto(value, &encoded);
        const Value decoded = RaftProtoCodec::from_proto(encoded);

        EXPECT_EQ(decoded.type, value.type);
        EXPECT_EQ(decoded.size, value.size);
        EXPECT_TRUE(ValueCodec::equal(decoded, value));
    }
}

TEST(RaftProtoCodecTest, KeyBytesSurviveEmbeddedNuls) {
    const Key key = KeyCodec::make_bytes({'a', 0, 'b', 0, 'c'});

    raftpb::ProtoKey encoded;
    RaftProtoCodec::to_proto(key, &encoded);
    ASSERT_EQ(encoded.data().size(), 5u);

    const Key decoded = RaftProtoCodec::from_proto(encoded);
    EXPECT_EQ(decoded.size, 5u);
    EXPECT_TRUE(KeyCodec::equal(decoded, key));
}

TEST(RaftProtoCodecTest, SizeComesFromTheDataNotTheWire) {
    raftpb::ProtoKey encoded;
    encoded.set_type(static_cast<std::uint32_t>(KeyType::Bytes));
    encoded.set_data("ab", 2);

    EXPECT_EQ(RaftProtoCodec::from_proto(encoded).size, 2u);
}

TEST(RaftProtoCodecTest, RejectsUnknownKeyAndValueTypes) {
    raftpb::ProtoKey key;
    key.set_type(9);
    key.set_data("x", 1);
    EXPECT_THROW(RaftProtoCodec::from_proto(key), std::out_of_range);

    raftpb::ProtoValue value;
    value.set_type(9);
    value.set_data("x", 1);
    EXPECT_THROW(RaftProtoCodec::from_proto(value), std::out_of_range);
}

TEST(RaftProtoCodecTest, RejectsBytesThatAreNotValidForTheirType) {
    // A UInt64 key is exactly eight bytes; three is not a key.
    raftpb::ProtoKey key;
    key.set_type(static_cast<std::uint32_t>(KeyType::UInt64));
    key.set_data("xyz", 3);
    EXPECT_THROW(RaftProtoCodec::from_proto(key), std::invalid_argument);
}

TEST(RaftProtoCodecTest, RoundTripsAPut) {
    raftpb::Mutation encoded;
    RaftProtoCodec::to_proto(put_op(sample_key(), sample_value()), &encoded);

    EXPECT_EQ(encoded.type(), raftpb::MUTATION_PUT);
    ASSERT_TRUE(encoded.has_key());
    ASSERT_TRUE(encoded.has_value());

    const MutationOp decoded = RaftProtoCodec::from_proto(encoded);
    ASSERT_EQ(decoded.type, RaftMutationType::Put);
    const PutMutation& put = std::get<PutMutation>(decoded.operation);
    EXPECT_TRUE(KeyCodec::equal(put.key, sample_key()));
    EXPECT_TRUE(ValueCodec::equal(put.value, sample_value()));
}

TEST(RaftProtoCodecTest, RoundTripsADeleteAndCarriesNoValue) {
    raftpb::Mutation encoded;
    RaftProtoCodec::to_proto(delete_op(sample_key()), &encoded);

    EXPECT_EQ(encoded.type(), raftpb::MUTATION_DELETE);
    ASSERT_TRUE(encoded.has_key());
    // mutable_value() would mark the field present and make a tombstone look
    // like it carries a value.
    EXPECT_FALSE(encoded.has_value());

    const MutationOp decoded = RaftProtoCodec::from_proto(encoded);
    ASSERT_EQ(decoded.type, RaftMutationType::Delete);
    EXPECT_TRUE(KeyCodec::equal(std::get<DeleteMutation>(decoded.operation).key, sample_key()));
}

TEST(RaftProtoCodecTest, RejectsAPutWithNoValue) {
    raftpb::Mutation encoded;
    encoded.set_type(raftpb::MUTATION_PUT);
    RaftProtoCodec::to_proto(sample_key(), encoded.mutable_key());

    ASSERT_FALSE(encoded.has_value());
    EXPECT_THROW(RaftProtoCodec::from_proto(encoded), std::invalid_argument);
}

TEST(RaftProtoCodecTest, RejectsUnspecifiedAndUnknownMutationTypes) {
    raftpb::Mutation unspecified;
    unspecified.set_type(raftpb::MUTATION_UNSPECIFIED);
    RaftProtoCodec::to_proto(sample_key(), unspecified.mutable_key());
    EXPECT_THROW(RaftProtoCodec::from_proto(unspecified), std::out_of_range);

    // proto3 enums are open, so a newer peer's value arrives intact rather than
    // being dropped, and must be rejected rather than cast.
    raftpb::Mutation unknown;
    unknown.set_type(static_cast<raftpb::MutationType>(77));
    RaftProtoCodec::to_proto(sample_key(), unknown.mutable_key());
    EXPECT_THROW(RaftProtoCodec::from_proto(unknown), std::out_of_range);
}

TEST(RaftProtoCodecTest, NestedFailuresKeepTheirTypeAndMessage) {
    raftpb::Mutation encoded;
    RaftProtoCodec::to_proto(put_op(sample_key(), sample_value()), &encoded);
    encoded.mutable_key()->set_type(static_cast<std::uint32_t>(KeyType::UInt64));
    encoded.mutable_key()->set_data("xyz", 3);

    // Catching and rethrowing by value would slice this down to std::exception.
    try {
        RaftProtoCodec::from_proto(encoded);
        FAIL() << "expected an invalid key to be rejected";
    } catch (const std::invalid_argument& error) {
        EXPECT_NE(std::string(error.what()).find("invalid key"), std::string::npos);
    }
}

TEST(RaftProtoCodecTest, RoundTripsAnEntryWithoutDuplicatingOperations) {
    const RaftMutationEntry entry{
        .term = 7,
        .idx = 3,
        .operations = {put_op(sample_key(), sample_value()), delete_op(sample_key())},
    };

    raftpb::RaftEntry encoded;
    RaftProtoCodec::to_proto(entry, &encoded);
    ASSERT_EQ(encoded.operations_size(), 2);

    const RaftMutationEntry decoded = RaftProtoCodec::from_proto(encoded);
    EXPECT_EQ(decoded.term, 7u);
    EXPECT_EQ(decoded.idx, 3u);
    // resize() instead of reserve() would leave four, with empty puts in front.
    ASSERT_EQ(decoded.operations.size(), 2u);
    EXPECT_EQ(decoded.operations[0].type, RaftMutationType::Put);
    EXPECT_EQ(decoded.operations[1].type, RaftMutationType::Delete);
}

TEST(RaftProtoCodecTest, RoundTripsANoOpEntry) {
    const RaftMutationEntry entry{.term = 2, .idx = 9, .operations = {}};

    raftpb::RaftEntry encoded;
    RaftProtoCodec::to_proto(entry, &encoded);
    EXPECT_EQ(encoded.operations_size(), 0);

    const RaftMutationEntry decoded = RaftProtoCodec::from_proto(encoded);
    EXPECT_EQ(decoded.term, 2u);
    EXPECT_EQ(decoded.idx, 9u);
    EXPECT_TRUE(decoded.operations.empty());
}

TEST(RaftProtoCodecTest, AnEntryWithABadOperationIsRejected) {
    raftpb::RaftEntry encoded;
    encoded.set_term(1);
    encoded.set_idx(1);
    RaftProtoCodec::to_proto(put_op(sample_key(), sample_value()), encoded.add_operations());

    // A valid key with an unusable type, so this targets the type check. The key
    // is decoded before the type is dispatched, so omitting it would fail on the
    // key instead and test nothing about the type.
    raftpb::Mutation* bad = encoded.add_operations();
    bad->set_type(raftpb::MUTATION_UNSPECIFIED);
    RaftProtoCodec::to_proto(sample_key(), bad->mutable_key());

    EXPECT_THROW(RaftProtoCodec::from_proto(encoded), std::out_of_range);
}

} // namespace
