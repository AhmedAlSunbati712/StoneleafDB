#include <gtest/gtest.h>

#include <Endian.h>
#include <KeyCodec.h>
#include <Raft/RaftEntryCodec.h>
#include <ValueCodec.h>

#include <variant>
#include <vector>

namespace {

TEST(RaftEntryCodecTest, EncodesFixedHeaderAndEmptyNoOp) {
    const auto encoded = RaftEntryCodec::encode({.term = 7, .idx = 12, .operations = {}});

    ASSERT_EQ(encoded.size(), RaftEntryCodec::HEADER_SIZE);
    EXPECT_EQ(get_u64_be(encoded.data() + RaftEntryCodec::TERM_OFFSET), 7u);
    EXPECT_EQ(get_u64_be(encoded.data() + RaftEntryCodec::INDEX_OFFSET), 12u);
    EXPECT_EQ(get_u32_be(encoded.data() + RaftEntryCodec::OPERATION_COUNT_OFFSET), 0u);
    EXPECT_TRUE(RaftEntryCodec::decode(encoded).operations.empty());
}

TEST(RaftEntryCodecTest, RoundTripsPutAndDeleteMutations) {
    RaftMutationEntry entry{
        .term = 3,
        .idx = 9,
        .operations = {
            MutationOp{RaftMutationType::Put, PutMutation{
                KeyCodec::make_string("alpha"), ValueCodec::make_varint(-42)}},
            MutationOp{RaftMutationType::Delete, DeleteMutation{
                KeyCodec::make_bytes(std::vector<char>{'x', '\0', 'y'})}},
        },
    };

    const RaftMutationEntry decoded = RaftEntryCodec::decode(RaftEntryCodec::encode(entry));

    ASSERT_EQ(decoded.term, entry.term);
    ASSERT_EQ(decoded.idx, entry.idx);
    ASSERT_EQ(decoded.operations.size(), 2u);
    const auto& put = std::get<PutMutation>(decoded.operations[0].operation);
    EXPECT_TRUE(KeyCodec::equal(put.key, std::get<PutMutation>(entry.operations[0].operation).key));
    EXPECT_TRUE(ValueCodec::equal(put.value, std::get<PutMutation>(entry.operations[0].operation).value));
    const auto& remove = std::get<DeleteMutation>(decoded.operations[1].operation);
    EXPECT_TRUE(KeyCodec::equal(remove.key, std::get<DeleteMutation>(entry.operations[1].operation).key));
}

TEST(RaftEntryCodecTest, RejectsInvalidHeadersAndMutationShapes) {
    EXPECT_THROW(RaftEntryCodec::encode({.term = 0, .idx = 1}), std::invalid_argument);
    EXPECT_THROW(RaftEntryCodec::encode({.term = 1, .idx = 0}), std::invalid_argument);
    EXPECT_THROW(
        RaftEntryCodec::encode({
            .term = 1,
            .idx = 1,
            .operations = {{RaftMutationType::Put, DeleteMutation{KeyCodec::make_bool(true)}}},
        }),
        std::invalid_argument);

    auto unknown = RaftEntryCodec::encode({
        .term = 1,
        .idx = 1,
        .operations = {{RaftMutationType::Delete, DeleteMutation{KeyCodec::make_bool(true)}}},
    });
    unknown[RaftEntryCodec::HEADER_SIZE] = static_cast<char>(99);
    EXPECT_THROW(RaftEntryCodec::decode(unknown), std::runtime_error);
}

TEST(RaftEntryCodecTest, RejectsTruncationCountsAndTrailingBytes) {
    auto encoded = RaftEntryCodec::encode({.term = 1, .idx = 1});
    EXPECT_THROW(RaftEntryCodec::decode(std::span(encoded).first(10)), std::runtime_error);

    put_u32_be(encoded.data() + RaftEntryCodec::OPERATION_COUNT_OFFSET, 1);
    EXPECT_THROW(RaftEntryCodec::decode(encoded), std::runtime_error);

    encoded = RaftEntryCodec::encode({.term = 1, .idx = 1});
    encoded.push_back('x');
    EXPECT_THROW(RaftEntryCodec::decode(encoded), std::runtime_error);
}

} // namespace
