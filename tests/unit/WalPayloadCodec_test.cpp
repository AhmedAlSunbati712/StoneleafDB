#include <gtest/gtest.h>

#include <Log/WalPayloadCodec.h>
#include <Log/WalRecords.h>
#include <KeyCodec.h>
#include <V2PageCodec.h>
#include <ValueCodec.h>

namespace {
PageEffect effect(PageEffectKind kind = PageEffectKind::Write, std::uint32_t page_num = 7) {
    PageEffect value{.kind = kind, .page_num = page_num};
    V2PageCodec::initialize(value.after_image, page_num, V2PageKind::BTreeLeaf);
    return value;
}

TEST(WalPayloadCodecTest, FactoriesEncodeEveryRecordPayload) {
    const auto key = KeyCodec::make_string("key");
    const auto old = ValueCodec::make_char("old");
    EXPECT_TRUE(std::holds_alternative<BeginPayload>(WalPayloadCodec::decode(WalRecordType::TxnBegin, WalRecords::begin(1).data)));
    for (UndoDescriptor undo : {UndoDescriptor{InsertUndo{key}}, UndoDescriptor{UpdateUndo{key, old}}, UndoDescriptor{DeleteUndo{key, old}}}) {
        auto pending = WalRecords::btree_action(1, 1, {undo, {effect()}});
        auto decoded = std::get<BTreeActionPayload>(WalPayloadCodec::decode(pending.type, pending.data));
        EXPECT_EQ(action_kind(decoded.undo), action_kind(undo)); EXPECT_EQ(decoded.effects[0].page_num, 7u);
    }
    EXPECT_TRUE(std::holds_alternative<CompensationPayload>(WalPayloadCodec::decode(WalRecordType::Compensation,
        WalRecords::compensation(1, 2, {2, 1, {effect(PageEffectKind::Free)}}).data)));
    EXPECT_TRUE(std::holds_alternative<SystemActionPayload>(WalPayloadCodec::decode(WalRecordType::SystemAction,
        WalRecords::system_action({SystemActionKind::BTreeSplit, {effect(PageEffectKind::Allocate)}}).data)));
    EXPECT_TRUE(std::holds_alternative<CommitPayload>(WalPayloadCodec::decode(WalRecordType::TxnCommit, WalRecords::commit(1, 2).data)));
    EXPECT_EQ(std::get<AbortPayload>(WalPayloadCodec::decode(WalRecordType::TxnAbort, WalRecords::abort(1, 2, AbortReason::DeadlockVictim).data)).reason, AbortReason::DeadlockVictim);
    EXPECT_TRUE(std::holds_alternative<EndPayload>(WalPayloadCodec::decode(WalRecordType::TxnEnd, WalRecords::end(1, 2).data)));
}

TEST(WalPayloadCodecTest, RoundTripsEveryKeyAndValueType) {
    std::vector<Key> keys{KeyCodec::make_bool(true), KeyCodec::make_uint64(4), KeyCodec::make_int64(-4), KeyCodec::make_string("x"), KeyCodec::make_bytes({0, -1})};
    std::vector<Value> values{ValueCodec::make_varuint(4), ValueCodec::make_varint(-4), ValueCodec::make_bool(true), ValueCodec::make_char("x")};
    for (const auto& key : keys) for (const auto& value : values) {
        auto encoded = WalPayloadCodec::encode(WalRecordType::BTreeAction, BTreeActionPayload{UpdateUndo{key, value}, {effect()}});
        auto decoded = std::get<BTreeActionPayload>(WalPayloadCodec::decode(WalRecordType::BTreeAction, encoded));
        const auto& undo = std::get<UpdateUndo>(decoded.undo);
        EXPECT_TRUE(KeyCodec::equal(undo.key, key)); EXPECT_TRUE(ValueCodec::equal(undo.old_value, value));
    }
}

TEST(WalPayloadCodecTest, RejectsInvalidCountsEnumsTruncationAndTrailingBytes) {
    auto encoded = WalRecords::system_action({SystemActionKind::BTreeSplit, {effect()}}).data;
    auto invalid_enum = encoded; invalid_enum[0] = 0;
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::SystemAction, invalid_enum), std::runtime_error);
    auto zero_count = encoded; std::fill(zero_count.begin() + 1, zero_count.begin() + 5, 0);
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::SystemAction, zero_count), std::runtime_error);
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::SystemAction, std::span(encoded).first(encoded.size() - 1)), std::runtime_error);
    encoded.push_back(0); EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::SystemAction, encoded), std::runtime_error);
}

TEST(WalPayloadCodecTest, RejectsPageNumberMismatchAndBadStructure) {
    auto mismatch = effect(); mismatch.page_num = 8;
    EXPECT_THROW(WalPayloadCodec::encode(WalRecordType::SystemAction, SystemActionPayload{SystemActionKind::BTreeSplit, {mismatch}}), std::invalid_argument);
    auto invalid = effect(); invalid.after_image[0] = 0;
    EXPECT_THROW(WalPayloadCodec::encode(WalRecordType::SystemAction, SystemActionPayload{SystemActionKind::BTreeSplit, {invalid}}), std::invalid_argument);
}

TEST(WalPayloadCodecTest, FactoriesRejectInvalidTransactionMetadata) {
    EXPECT_THROW(WalRecords::begin(0), std::invalid_argument);
    EXPECT_THROW(WalRecords::commit(1, 0), std::invalid_argument);
    EXPECT_THROW(WalRecords::abort(0, 1, AbortReason::ClientRequest), std::invalid_argument);
}
} // namespace

TEST(WalPayloadCodecTest, CommitRecordCarriesTheRaftIndex) {
    const auto stamped = WalRecords::commit(1, 2, 42).data;
    EXPECT_EQ(stamped.size(), 8u);
    EXPECT_EQ(std::get<CommitPayload>(WalPayloadCodec::decode(WalRecordType::TxnCommit, stamped)).raft_index, 42u);

    // Client transactions apply no Raft entry, so they carry 0.
    EXPECT_EQ(std::get<CommitPayload>(WalPayloadCodec::decode(
        WalRecordType::TxnCommit, WalRecords::commit(1, 2).data)).raft_index, 0u);

    // The whole 64-bit range round-trips, not just small indexes.
    const std::uint64_t highest = 0xFFFFFFFFFFFFFFFFull;
    EXPECT_EQ(std::get<CommitPayload>(WalPayloadCodec::decode(
        WalRecordType::TxnCommit, WalRecords::commit(1, 2, highest).data)).raft_index, highest);
}

TEST(WalPayloadCodecTest, RejectsCommitPayloadsThatAreNotEightBytes) {
    const auto encoded = WalRecords::commit(1, 2, 42).data;

    std::vector<char> truncated(encoded.begin(), encoded.end() - 1);
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::TxnCommit, truncated), std::runtime_error);
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::TxnCommit, std::vector<char>{}), std::runtime_error);

    std::vector<char> trailing = encoded;
    trailing.push_back(0);
    EXPECT_THROW(WalPayloadCodec::decode(WalRecordType::TxnCommit, trailing), std::runtime_error);
}
