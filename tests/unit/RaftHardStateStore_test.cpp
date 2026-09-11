#include <gtest/gtest.h>

#include <Endian.h>
#include <Raft/RaftHardStateStore.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

namespace {

class TempDir {
public:
    TempDir() {
        path = std::filesystem::temp_directory_path() / "stoneleaf-hard-state-XXXXXX";
        std::string value = path.string();
        value.push_back('\0');
        path = ::mkdtemp(value.data());
    }
    ~TempDir() { std::filesystem::remove_all(path); }
    std::filesystem::path path;
};

std::vector<char> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::vector<char>(
        std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

TEST(RaftHardStateStoreTest, MissingStateLoadsTermZeroWithoutVote) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());

    const RaftHardState state = store.load();
    EXPECT_EQ(state.term, 0u);
    EXPECT_FALSE(state.voted_for.has_value());
}

TEST(RaftHardStateStoreTest, PersistsExactLayoutAndReopens) {
    TempDir dir;
    {
        RaftHardStateStore store;
        store.open(dir.path.string());
        store.persist(8, "node-a:7000");
    }

    const auto encoded = read_bytes(dir.path / "state");
    ASSERT_EQ(encoded.size(), 13u + std::string("node-a:7000").size());
    EXPECT_EQ(get_u64_be(encoded.data()), 8u);
    EXPECT_EQ(encoded[8], '\1');
    EXPECT_EQ(get_u32_be(encoded.data() + 9), 11u);

    RaftHardStateStore reopened;
    reopened.open(dir.path.string());
    EXPECT_EQ(reopened.load().term, 8u);
    EXPECT_EQ(reopened.load().voted_for, "node-a:7000");
}

TEST(RaftHardStateStoreTest, AllowsFirstVoteAndClearsItOnlyInHigherTerm) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    store.persist(4, std::nullopt);
    store.persist(4, "node-a:1");
    EXPECT_NO_THROW(store.persist(4, "node-a:1"));
    EXPECT_THROW(store.persist(4, "node-b:1"), std::invalid_argument);
    EXPECT_THROW(store.persist(4, std::nullopt), std::invalid_argument);
    EXPECT_THROW(store.persist(3, std::nullopt), std::invalid_argument);
    EXPECT_NO_THROW(store.persist(5, std::nullopt));
    EXPECT_FALSE(store.load().voted_for.has_value());
}

TEST(RaftHardStateStoreTest, IgnoresStaleTemporaryFile) {
    TempDir dir;
    {
        RaftHardStateStore store;
        store.open(dir.path.string());
        store.persist(2, "node-a:1");
    }
    {
        std::ofstream temporary(dir.path / "state.tmp", std::ios::binary);
        temporary << "partial";
    }

    RaftHardStateStore reopened;
    reopened.open(dir.path.string());
    EXPECT_EQ(reopened.load().term, 2u);
    EXPECT_EQ(reopened.load().voted_for, "node-a:1");
}

TEST(RaftHardStateStoreTest, RejectsMalformedStateFile) {
    TempDir dir;
    {
        std::ofstream state(dir.path / "state", std::ios::binary);
        state << "short";
    }
    RaftHardStateStore store;
    EXPECT_THROW(store.open(dir.path.string()), std::runtime_error);
    EXPECT_FALSE(store.is_open());
}

TEST(RaftHardStateStoreTest, RejectsVoteInTermZeroAndEmptyIdentity) {
    TempDir dir;
    RaftHardStateStore store;
    store.open(dir.path.string());
    EXPECT_THROW(store.persist(0, "node-a:1"), std::invalid_argument);
    EXPECT_THROW(store.persist(1, ""), std::invalid_argument);
}

} // namespace
