CXX = g++
CXXFLAGS = -Wall --std=c++23 -Iinclude -Iinclude/disk -Iinclude/encoding -Iinclude/containers -Iinclude/client -Iinclude/LockManager -Iinclude/TransactionManager -I/opt/homebrew/include -Iinclude/API
LDFLAGS = -L/opt/homebrew/lib
LDLIBS = -lgtest -lgtest_main
AR = ar
ARFLAGS = rcs

LIB = build/libstoneleafdb.a

SRC = \
	src/KeyStore.cpp \
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

UNIT_TEST_SRC := $(wildcard tests/unit/*.cpp)
UNIT_TEST_OBJ := $(patsubst tests/unit/%.cpp,build/tests/unit/%.o,$(UNIT_TEST_SRC))
UNIT_TEST_BIN := $(patsubst tests/unit/%.cpp,build/tests/unit/%,$(UNIT_TEST_SRC))

INTEGRATION_TEST_SRC := $(wildcard tests/integration/*.cpp)
INTEGRATION_TEST_OBJ := $(patsubst tests/integration/%.cpp,build/tests/integration/%.o,$(INTEGRATION_TEST_SRC))
INTEGRATION_TEST_BIN := $(patsubst tests/integration/%.cpp,build/tests/integration/%,$(INTEGRATION_TEST_SRC))

SERVER_SRC = \
        src/server/server.cpp \
        src/server/CommandServer.cpp
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
$(SERVER_BIN): $(SERVER_OBJ) $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

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

$(BENCHMARK_LIB): $(BENCHMARK_LIB_OBJ)
	mkdir -p $(dir $@)
	$(AR) $(ARFLAGS) $@ $^

$(BENCHMARK_BIN): $(BENCHMARK_OBJ) $(BENCHMARK_LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) -pthread

build/tests/unit/%: build/tests/unit/%.o $(LIB)
	mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS)

build/tests/unit/KeyLockManager_test: CXXFLAGS += -pthread
build/tests/unit/KeyLockManager_test: LDLIBS += -pthread
build/tests/unit/BTreeOperation_test: CXXFLAGS += -pthread
build/tests/unit/BTreeOperation_test: LDLIBS += -pthread
build/tests/unit/Log_test: CXXFLAGS += -pthread
build/tests/unit/Log_test: LDLIBS += -pthread
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
