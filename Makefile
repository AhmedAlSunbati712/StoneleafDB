PROTO_DIR = proto
GEN_DIR = build/gen

PKG_CONFIG ?= pkg-config

GRPC_CXXFLAGS := $(shell $(PKG_CONFIG) --cflags grpc++ protobuf)
GRPC_LDLIBS   := $(shell $(PKG_CONFIG) --libs   grpc++ protobuf)

# protoc has to match the protobuf headers we compile against: generated code
# carries a version assertion, so a 3.x protoc against a 36.x runtime does not
# merely warn, it fails to compile. That rules out taking whatever protoc comes
# first on PATH - on a dev box with conda or a vendored toolchain installed, that
# is routinely a different major version from the one pkg-config reports.
#
# So resolve each tool from the prefix of the library it must agree with, and
# fall back to PATH only when that prefix has no binary (which is how a
# distro-packaged layout with a separate -compiler package behaves). Both stay
# overridable from the command line or the environment.
PROTOBUF_PREFIX := $(shell $(PKG_CONFIG) --variable=prefix protobuf 2>/dev/null)
GRPC_PREFIX     := $(shell $(PKG_CONFIG) --variable=prefix grpc++ 2>/dev/null)

PROTOC ?= $(firstword $(wildcard $(PROTOBUF_PREFIX)/bin/protoc) protoc)
GRPC_CPP_PLUGIN ?= $(firstword $(wildcard $(GRPC_PREFIX)/bin/grpc_cpp_plugin) grpc_cpp_plugin)

CXX = g++
CXXFLAGS = -Wall --std=c++23 -Iinclude -Iinclude/disk -Iinclude/encoding -Iinclude/containers -Iinclude/client -I build/gen $(GRPC_CXXFLAGS) -Iinclude/LockManager -Iinclude/TransactionManager -I/opt/homebrew/include -Iinclude/API
LDFLAGS = -L/opt/homebrew/lib
LDLIBS = -lgtest -lgtest_main
AR = ar
ARFLAGS = rcs

LIB = build/libstoneleafdb.a

SRC = \
	src/KeyStore.cpp \
	src/Recovery.cpp \
	src/containers/BTree.cpp \
	src/containers/BTreeCursor.cpp \
	src/containers/BTreeOperation.cpp \
	src/containers/BTreePage.cpp \
	src/containers/DLList.cpp \
	src/client/Client.cpp \
	src/client/Command.cpp \
	src/client/NetCodec.cpp \
	src/client/Session.cpp \
	src/JournalCodec.cpp \
	src/Raft/RaftEntryCodec.cpp \
	src/Raft/RaftHardStateStore.cpp \
	src/Raft/RaftSegment.cpp \
	src/Raft/RaftLog.cpp \
	src/Raft/NodeAddress.cpp \
	src/Raft/RaftState.cpp \
	src/Raft/RaftApplier.cpp \
	src/Raft/ClusterConfig.cpp \
	src/Raft/RaftCommitIndex.cpp \
	src/Raft/RaftProposer.cpp \
	src/Raft/RaftReadIndex.cpp \
	src/Raft/TransactionWriteBuffer.cpp \
	src/storage/Index.cpp \
	src/Log/Segment.cpp \
	src/Log/Store.cpp \
	src/Log/WalRecordCodec.cpp \
	src/Log/WalPayloadCodec.cpp \
	src/Log/WalRecords.cpp \
	src/Log/PendingBTreeAction.cpp \
	src/Log/Log.cpp \
	src/TransactionManager/TransactionManager.cpp \
	src/TransactionManager/WaitForGraph.cpp \
	src/LockMgr.cpp \
	src/LockManager/LockManager.cpp \
	src/PageLatchManager.cpp \
	src/PCache.cpp \
	src/Pager.cpp \
	src/disk/DiskIO.cpp \
	src/encoding/Endian.cpp \
	src/encoding/Crc32c.cpp \
	src/encoding/KeyCodec.cpp \
	src/encoding/ValueCodec.cpp \
	src/DBHeaderCodec.cpp \
	src/V2PageCodec.cpp

OBJ = \
	build/KeyStore.o \
	build/Recovery.o \
	build/containers/BTree.o \
	build/containers/BTreeCursor.o \
	build/containers/BTreeOperation.o \
	build/containers/BTreePage.o \
	build/containers/DLList.o \
	build/client/Client.o \
	build/client/Command.o \
	build/client/NetCodec.o \
	build/client/Session.o \
	build/JournalCodec.o \
	build/Raft/RaftEntryCodec.o \
	build/Raft/RaftHardStateStore.o \
	build/Raft/RaftSegment.o \
	build/Raft/RaftLog.o \
	build/Raft/NodeAddress.o \
	build/Raft/RaftState.o \
	build/Raft/RaftApplier.o \
	build/Raft/ClusterConfig.o \
	build/Raft/RaftCommitIndex.o \
	build/Raft/RaftProposer.o \
	build/Raft/RaftReadIndex.o \
	build/Raft/TransactionWriteBuffer.o \
	build/storage/Index.o \
	build/Log/Segment.o \
	build/Log/Store.o \
	build/Log/WalRecordCodec.o \
	build/Log/WalPayloadCodec.o \
	build/Log/WalRecords.o \
	build/Log/PendingBTreeAction.o \
	build/Log/Log.o \
	build/TransactionManager/TransactionManager.o \
	build/TransactionManager/WaitForGraph.o \
	build/LockMgr.o \
	build/LockManager/LockManager.o \
	build/PageLatchManager.o \
	build/PCache.o \
	build/Pager.o \
	build/disk/DiskIO.o \
	build/encoding/Endian.o \
	build/encoding/Crc32c.o \
	build/encoding/KeyCodec.o \
	build/encoding/ValueCodec.o \
	build/DBHeaderCodec.o \
	build/V2PageCodec.o

PROTO_OBJ = $(GEN_DIR)/raft.pb.o $(GEN_DIR)/raft.grpc.pb.o

UNIT_TEST_SRC := $(wildcard tests/unit/*.cpp)
UNIT_TEST_OBJ := $(patsubst tests/unit/%.cpp,build/tests/unit/%.o,$(UNIT_TEST_SRC))
UNIT_TEST_BIN := $(patsubst tests/unit/%.cpp,build/tests/unit/%,$(UNIT_TEST_SRC))

INTEGRATION_TEST_SRC := $(wildcard tests/integration/*.cpp)
INTEGRATION_TEST_OBJ := $(patsubst tests/integration/%.cpp,build/tests/integration/%.o,$(INTEGRATION_TEST_SRC))
INTEGRATION_TEST_BIN := $(patsubst tests/integration/%.cpp,build/tests/integration/%,$(INTEGRATION_TEST_SRC))

SERVER_SRC = \
        src/server/server.cpp \
        src/server/CommandServer.cpp \
        src/Raft/RaftProtoCodec.cpp \
        src/Raft/RaftServiceImpl.cpp \
        src/Raft/RaftPeerClients.cpp \
        src/Raft/RaftElection.cpp \
        src/Raft/RaftReplicator.cpp
SERVER_OBJ = $(patsubst src/%.cpp,build/%.o,$(SERVER_SRC))
SERVER_BIN = build/stoneleaf-server

BENCHMARK_SRC = benchmarks/LockManager_benchmark.cpp
BENCHMARK_OBJ = build/benchmarks/LockManager_benchmark.o
BENCHMARK_BIN = build/benchmarks/LockManager_benchmark
BENCHMARK_LIB = build/benchmarks/libstoneleafdb.a
BENCHMARK_LIB_OBJ = $(patsubst src/%.cpp,build/benchmarks/lib/%.o,$(SRC))

.PHONY: all server benchmark benchmark-run test test-unit test-integration clean
.SECONDARY: $(UNIT_TEST_OBJ) $(INTEGRATION_TEST_OBJ) $(BENCHMARK_OBJ) $(BENCHMARK_LIB_OBJ)

all: $(LIB) $(UNIT_TEST_BIN) $(INTEGRATION_TEST_BIN)

$(LIB): $(OBJ)
	mkdir -p $(dir $@)
	$(AR) $(ARFLAGS) $@ $^

server: $(SERVER_BIN)

benchmark: $(BENCHMARK_BIN)

benchmark-run: $(BENCHMARK_BIN)
	./$(BENCHMARK_BIN)

$(SERVER_BIN): CXXFLAGS += -pthread
# The Raft RPC sources live here rather than in $(LIB) on purpose: putting them
# in the library would force protobuf and gRPC onto every test binary that links
# it. The server is the only thing that needs them.
$(SERVER_BIN): $(SERVER_OBJ) $(PROTO_OBJ) $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(GRPC_LDLIBS)

build/%.o: src/%.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

build/tests/integration/%.o: tests/integration/%.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

build/tests/integration/CommandServer_test: build/tests/integration/CommandServer_test.o build/server/CommandServer.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) -pthread

build/tests/integration/%: build/tests/integration/%.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS)

build/tests/unit/%.o: tests/unit/%.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

build/benchmarks/%.o: benchmarks/%.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -O3 -DNDEBUG -pthread -c $< -o $@

build/benchmarks/lib/%.o: src/%.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -O3 -DNDEBUG -pthread -c $< -o $@

$(GEN_DIR)/raft.pb.cc $(GEN_DIR)/raft.grpc.pb.cc: $(PROTO_DIR)/raft.proto
	@mkdir -p $(GEN_DIR)
	@command -v $(PROTOC) >/dev/null 2>&1 || { \
	  echo "ERROR: protoc not found (looked for '$(PROTOC)')."; \
	  echo "  Install the protobuf compiler and gRPC plugin:"; \
	  echo "    macOS:  brew install protobuf grpc pkg-config"; \
	  echo "    Debian: apt-get install -y protobuf-compiler protobuf-compiler-grpc \\"; \
	  echo "                               libprotobuf-dev libgrpc++-dev pkg-config"; \
	  echo "  Or override: make PROTOC=/path/to/protoc GRPC_CPP_PLUGIN=/path/to/grpc_cpp_plugin"; \
	  exit 1; }
	@command -v $(GRPC_CPP_PLUGIN) >/dev/null 2>&1 || { \
	  echo "ERROR: grpc_cpp_plugin not found (looked for '$(GRPC_CPP_PLUGIN)')."; \
	  echo "  It ships separately from protoc: 'brew install grpc' or"; \
	  echo "  'apt-get install protobuf-compiler-grpc'."; \
	  exit 1; }
	$(PROTOC) -I $(PROTO_DIR) --cpp_out=$(GEN_DIR) --grpc_out=$(GEN_DIR) \
	          --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN) $<

# protoc writes the headers next to the sources. An empty recipe states that
# without giving make a reason to run protoc a second time.
$(GEN_DIR)/raft.pb.h $(GEN_DIR)/raft.grpc.pb.h: $(GEN_DIR)/raft.pb.cc ;

# Anything including a generated header has to wait for protoc. Without this,
# a fresh clone - or any build after make clean, which deletes build/gen -
# compiles the codec before the header exists.
build/Raft/RaftProtoCodec.o: $(GEN_DIR)/raft.pb.h
build/Raft/RaftServiceImpl.o: $(GEN_DIR)/raft.grpc.pb.h
build/Raft/RaftPeerClients.o: $(GEN_DIR)/raft.grpc.pb.h
build/Raft/RaftElection.o: $(GEN_DIR)/raft.grpc.pb.h
build/Raft/RaftReplicator.o: $(GEN_DIR)/raft.grpc.pb.h
build/server/server.o: $(GEN_DIR)/raft.grpc.pb.h
build/tests/integration/RaftReplication_test.o: $(GEN_DIR)/raft.grpc.pb.h
build/tests/unit/RaftProtoCodec_test.o: $(GEN_DIR)/raft.pb.h
build/tests/integration/RaftService_test.o: $(GEN_DIR)/raft.grpc.pb.h
build/tests/integration/RaftTransport_test.o: $(GEN_DIR)/raft.grpc.pb.h
build/tests/integration/RaftElection_test.o: $(GEN_DIR)/raft.grpc.pb.h

$(GEN_DIR)/%.o: $(GEN_DIR)/%.cc
	$(CXX) --std=c++23 -w $(GRPC_CXXFLAGS) -I$(GEN_DIR) -c $< -o $@

$(BENCHMARK_LIB): $(BENCHMARK_LIB_OBJ)
	mkdir -p $(dir $@)
	$(AR) $(ARFLAGS) $@ $^

$(BENCHMARK_BIN): $(BENCHMARK_OBJ) $(BENCHMARK_LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) -pthread

build/tests/unit/%: build/tests/unit/%.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS)

build/tests/integration/RecoveryWatermark_test: CXXFLAGS += -pthread
build/tests/integration/RecoveryWatermark_test: LDLIBS += -pthread
build/tests/integration/RaftApplier_test: CXXFLAGS += -pthread
build/tests/integration/RaftApplier_test: LDLIBS += -pthread
# Needs the generated message code and protobuf, like CommandServer_test
# needs CommandServer.o. Not in $(LIB) yet: nothing in the library uses the
# codec until the RPC layer lands.
build/tests/unit/RaftProtoCodec_test: build/tests/unit/RaftProtoCodec_test.o build/Raft/RaftProtoCodec.o $(GEN_DIR)/raft.pb.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(GRPC_LDLIBS)

# The RaftService::Service base class lives in raft.grpc.pb.o, the messages in
# raft.pb.o, and the handlers convert through the codec. Explicit rather than
# left to the pattern rule above, which links only $(LIB) - and, as with the
# codec, none of this is in $(LIB) until the server actually starts a gRPC
# server. -pthread because RaftState's mutex and condition variables are live.
build/tests/integration/RaftService_test: CXXFLAGS += -pthread
build/tests/integration/RaftService_test: build/tests/integration/RaftService_test.o build/Raft/RaftServiceImpl.o build/Raft/RaftProtoCodec.o $(GEN_DIR)/raft.pb.o $(GEN_DIR)/raft.grpc.pb.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(GRPC_LDLIBS) -pthread

# Stands up real gRPC servers, so it needs the peer clients too.
build/tests/integration/RaftTransport_test: CXXFLAGS += -pthread
build/tests/integration/RaftTransport_test: build/tests/integration/RaftTransport_test.o build/Raft/RaftServiceImpl.o build/Raft/RaftProtoCodec.o build/Raft/RaftPeerClients.o $(GEN_DIR)/raft.pb.o $(GEN_DIR)/raft.grpc.pb.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(GRPC_LDLIBS) -pthread

# Elections need the whole plane: the handlers to answer, the clients to ask.
build/tests/integration/RaftElection_test: CXXFLAGS += -pthread
build/tests/integration/RaftElection_test: build/tests/integration/RaftElection_test.o build/Raft/RaftElection.o build/Raft/RaftServiceImpl.o build/Raft/RaftProtoCodec.o build/Raft/RaftPeerClients.o $(GEN_DIR)/raft.pb.o $(GEN_DIR)/raft.grpc.pb.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(GRPC_LDLIBS) -pthread

# Replication needs everything elections need, plus the replicator and the
# propose path, and it drives real state machines through the apply loop.
build/tests/integration/RaftReplication_test: CXXFLAGS += -pthread
build/tests/integration/RaftReplication_test: build/tests/integration/RaftReplication_test.o build/server/CommandServer.o build/Raft/RaftReplicator.o build/Raft/RaftElection.o build/Raft/RaftServiceImpl.o build/Raft/RaftProtoCodec.o build/Raft/RaftPeerClients.o $(GEN_DIR)/raft.pb.o $(GEN_DIR)/raft.grpc.pb.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(GRPC_LDLIBS) -pthread

build/tests/unit/KeyLockManager_test: CXXFLAGS += -pthread
build/tests/unit/KeyLockManager_test: LDLIBS += -pthread
build/tests/unit/BTreeOperation_test: CXXFLAGS += -pthread
build/tests/unit/BTreeOperation_test: LDLIBS += -pthread
build/tests/unit/Log_test: CXXFLAGS += -pthread
build/tests/unit/Log_test: LDLIBS += -pthread
build/tests/unit/RaftLog_test: CXXFLAGS += -pthread
build/tests/unit/RaftLog_test: LDLIBS += -pthread
build/tests/unit/TransactionManager_test: CXXFLAGS += -pthread
build/tests/unit/TransactionManager_test: LDLIBS += -pthread
build/tests/unit/WaitForGraph_test: CXXFLAGS += -pthread
build/tests/unit/WaitForGraph_test: LDLIBS += -pthread

test-unit: $(UNIT_TEST_BIN)
	for test_bin in $(UNIT_TEST_BIN); do ./$$test_bin; done

test-integration: $(INTEGRATION_TEST_BIN)
	for test_bin in $(INTEGRATION_TEST_BIN); do ./$$test_bin; done

test: test-unit test-integration

clean:
	rm -rf build
