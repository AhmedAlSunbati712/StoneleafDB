#include <gtest/gtest.h>

#include <Raft/ClusterConfig.h>

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace {

class TempConfig {
public:
    explicit TempConfig(const std::string& contents) {
        path = std::filesystem::temp_directory_path() / "stoneleaf-cluster-XXXXXX";
        std::string value = path.string();
        value.push_back('\0');
        path = ::mkdtemp(value.data());
        file = path / "cluster.conf";
        std::ofstream(file) << contents;
    }
    ~TempConfig() { std::filesystem::remove_all(path); }

    std::string name() const { return file.string(); }

    std::filesystem::path path;
    std::filesystem::path file;
};

TEST(ClusterConfigTest, ParsesRowsIgnoringCommentsAndBlankLines) {
    TempConfig config(
        "# raft_addr      client_addr\n"
        "\n"
        "10.0.0.1:5001    10.0.0.1:6001\n"
        "   10.0.0.2:5001 10.0.0.2:6001   # the second node\n"
        "\n"
        "localhost:5003   localhost:6003\n");

    const std::vector<ClusterMember> cluster = parse_cluster_config(config.name());

    ASSERT_EQ(cluster.size(), 3u);
    EXPECT_EQ(cluster[0].raft, (NodeAddress{"10.0.0.1", 5001}));
    EXPECT_EQ(cluster[0].database_server, (NodeAddress{"10.0.0.1", 6001}));
    EXPECT_EQ(cluster[1].raft, (NodeAddress{"10.0.0.2", 5001}));
    EXPECT_EQ(cluster[2].raft, (NodeAddress{"localhost", 5003}));
    EXPECT_EQ(cluster[2].database_server, (NodeAddress{"localhost", 6003}));
}

TEST(ClusterConfigTest, RejectsRowsThatAreNotTwoAddresses) {
    TempConfig one_column("10.0.0.1:5001\n");
    EXPECT_THROW(parse_cluster_config(one_column.name()), std::runtime_error);

    TempConfig three_columns("10.0.0.1:5001 10.0.0.1:6001 10.0.0.1:7001\n");
    EXPECT_THROW(parse_cluster_config(three_columns.name()), std::runtime_error);

    TempConfig not_an_address("10.0.0.1:5001 not-an-address\n");
    EXPECT_THROW(parse_cluster_config(not_an_address.name()), std::runtime_error);

    TempConfig bad_port("10.0.0.1:5001 10.0.0.1:99999\n");
    EXPECT_THROW(parse_cluster_config(bad_port.name()), std::runtime_error);
}

TEST(ClusterConfigTest, RejectsDuplicateAddressesInEitherColumn) {
    TempConfig duplicate_raft(
        "10.0.0.1:5001 10.0.0.1:6001\n"
        "10.0.0.1:5001 10.0.0.2:6001\n");
    EXPECT_THROW(parse_cluster_config(duplicate_raft.name()), std::runtime_error);

    TempConfig duplicate_client(
        "10.0.0.1:5001 10.0.0.1:6001\n"
        "10.0.0.2:5001 10.0.0.1:6001\n");
    EXPECT_THROW(parse_cluster_config(duplicate_client.name()), std::runtime_error);
}

TEST(ClusterConfigTest, RejectsAnEmptyOrMissingFile) {
    TempConfig empty("# only a comment\n\n");
    EXPECT_THROW(parse_cluster_config(empty.name()), std::runtime_error);

    EXPECT_THROW(parse_cluster_config("/nonexistent/cluster.conf"), std::runtime_error);
}

} // namespace
