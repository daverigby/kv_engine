/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "callbacks.h"
#include "collections/vbucket_manifest.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "tests/module_tests/test_helpers.h"
#include "vbucket_state.h"

#include <benchmark/benchmark.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>

#include <algorithm>
#include <random>

using namespace std::string_literals;

enum Storage {
    COUCHSTORE = 0
#ifdef EP_USE_ROCKSDB
    ,
    ROCKSDB
#endif
};

class MockWriteCallback {
public:
    void operator()(TransactionContext&, KVStore::MutationSetResultState) {
    }
};

class MockCacheCallback : public StatusCallback<CacheLookup> {
public:
    MockCacheCallback(){};
    void callback(CacheLookup& lookup) {
        // I want to simulate DGM scenarios where we have a HT-miss most times.
        // So, here I return what KVStore understands as "Item not in the
        // HashTable, go to the Storage".
        setStatus(ENGINE_SUCCESS);
    };
};

class MockDiskCallback : public StatusCallback<GetValue> {
public:
    MockDiskCallback() : itemCount(0){};
    void callback(GetValue& val) {
        // Just increase the item count
        // Note: this callback is invoked for each item read from the storage.
        //     This is where the real DiskCallback pushes the Item to
        //     the DCP stream.
        itemCount++;
    };
    // Get the number of items found during a scan
    size_t getItemCount() {
        return itemCount;
    }

protected:
    // Number of items found during a scan
    size_t itemCount;
};

/*
 * Benchmark fixture for KVStore.
 */
class KVStoreBench : public benchmark::Fixture {
protected:
    void SetUp(benchmark::State& state) override {
        numItems = state.range(0);
        auto storage = state.range(1);

        // Initialize KVStoreConfig
        Configuration config;
        uint16_t shardId = 0;

        auto configStr = "dbname=KVStoreBench.db"s;
        config.setMaxSize(536870912);
        switch (storage) {
        case COUCHSTORE:
            state.SetLabel("Couchstore");
            config.parseConfiguration((configStr + ";backend=couchdb").c_str(),
                                      get_mock_server_api());
            kvstoreConfig = std::make_unique<KVStoreConfig>(config, shardId);
            break;
#ifdef EP_USE_ROCKSDB
        case ROCKSDB:
            state.SetLabel("CouchRocks");
            config.parseConfiguration((configStr + ";backend=rocksdb").c_str(),
                                      get_mock_server_api());
            kvstoreConfig =
                    std::make_unique<RocksDBKVStoreConfig>(config, shardId);
            break;
#endif
        }

        // Initialize KVStore
        kvstore = setup_kv_store(*kvstoreConfig);

#if 0
        // Load some data
        const std::string key = "key";
        std::string value = "value";
        MockWriteCallback wc;
        Vbid vbid = Vbid(0);
        kvstore->begin(std::make_unique<TransactionContext>());
        for (int i = 1; i <= numItems; i++) {
            Item item(makeStoredDocKey(key + std::to_string(i)),
                      0 /*flags*/,
                      0 /*exptime*/,
                      value.c_str(),
                      value.size(),
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*cas*/,
                      i /*bySeqno*/,
                      vbid);
            kvstore->set(item, wc);
        }
        Collections::VB::Manifest m;
        Collections::VB::Flush f(m);
        kvstore->commit(f);
        // Just check that the VBucket High Seqno has been updated correctly
        EXPECT_EQ(kvstore->getVBucketState(vbid)->highSeqno, numItems);
#endif
    }

    void TearDown(const benchmark::State& state) override {
        std::cerr << "TearDown\n";
        kvstore.reset();
        // cb::io::rmrf(kvstoreConfig->getDBName());
    }

private:
    std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config) {
        // Clean up in case a stale file was left from a previous run.
        try {
            cb::io::rmrf(config.getDBName());
        } catch (const std::system_error&) {
        }

        auto kvstore = KVStoreFactory::create(config);
        vbucket_state state;
        state.transition.state = vbucket_state_active;
        kvstore.rw->snapshotVBucket(
                vbid, state, VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT);
        return std::move(kvstore.rw);
    }

protected:
    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
    Vbid vbid = Vbid(0);
    int numItems;
};

/*
 * Benchmark for KVStore::scan()
 */
BENCHMARK_DEFINE_F(KVStoreBench, Scan)(benchmark::State& state) {
    size_t itemCountTotal = 0;

    while (state.KeepRunning()) {
        // Note: the CacheCallback here just make the code to flow into reading
        // data from disk
        auto cb = std::make_shared<MockDiskCallback>();
        auto cl = std::make_shared<MockCacheCallback>();
        ScanContext* scanContext =
                kvstore->initScanContext(cb,
                                         cl,
                                         vbid,
                                         0 /*startSeqno*/,
                                         DocumentFilter::ALL_ITEMS,
                                         ValueFilter::VALUES_COMPRESSED);
        ASSERT_TRUE(scanContext);

        auto scanStatus = kvstore->scan(scanContext);
        ASSERT_EQ(scanStatus, scan_success);
        auto itemCount = cb->getItemCount();
        ASSERT_EQ(itemCount, numItems);
        itemCountTotal += itemCount;

        kvstore->destroyScanContext(scanContext);
    }

    state.SetItemsProcessed(itemCountTotal);
}

/*
 * Benchmark for KVStore::commit()
 */
BENCHMARK_DEFINE_F(KVStoreBench, Commit)(benchmark::State& state) {
    // KVStoreBench test fixture has populated state.range(0) items initally,
    // use state.range(2) specify how lamny items to mutate in a single commit
    // batch.
    const size_t batchSize = state.range(2);

    // Populate the vectgor of keys, then pre-shuffle the keyspace so we write
    // in a pseudo-random order.
    std::vector<StoredDocKey> keys;
    for (int i = 1; i <= numItems; i++) {
        keys.emplace_back(makeStoredDocKey("key_" + std::to_string(i)));
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    //    std::shuffle(keys.begin(), keys.end(), gen);

    for (const auto& key : keys) {
        std::cout << key << "\n";
    }
    auto nextKey = keys.begin();

    // Setup random generator for a random value.
    using random_bytes_engine =
            std::independent_bits_engine<std::default_random_engine,
                                         CHAR_BIT,
                                         unsigned char>;
    random_bytes_engine randomBytes;
    std::vector<unsigned char> value(500);

    size_t flushCount = 0;
    while (state.KeepRunning()) {
        // Only measuring set()  + commit() time, so pre-prepare data to insert
        // with timing paused.
        state.PauseTiming();

        Vbid vbid = Vbid(0);
        std::vector<Item> items;
        items.reserve(batchSize);
        for (int i = 1; i <= batchSize; i++) {
            // Generate random value.
            std::generate(begin(value), end(value), std::ref(randomBytes));

            items.emplace_back(*nextKey,
                               0 /*flags*/,
                               0 /*exptime*/,
                               value.data(),
                               value.size(),
                               PROTOCOL_BINARY_RAW_BYTES,
                               0 /*cas*/,
                               i /*bySeqno*/,
                               vbid);

            nextKey++;
            if (nextKey == keys.end()) {
                nextKey = keys.begin();
            }
        }
        // Data ready, resume timing and perform set / commit().
        state.ResumeTiming();

        auto start = std::chrono::steady_clock::now();

        kvstore->begin(std::make_unique<TransactionContext>());
        MockWriteCallback wc;
        for (auto& item : items) {
            std::cout << "key: " << item.getKey() << "\n";
            kvstore->set(item, wc);
        }
        Collections::VB::Manifest m;
        Collections::VB::Flush f(m);
        kvstore->commit(f);

        auto end = std::chrono::steady_clock::now();
        if (state.iterations() % 100 == 0) {
            auto duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            end - start);

            printf("%6d duration: %5d us ",
                   state.iterations(),
                   duration.count());
            for (int i = 0; i < duration.count(); i += 100) {
                printf("#");
            }
            printf("\n");
        }

        flushCount += batchSize;

        // Just check that the VBucket High Seqno has been updated correctly
        // EXPECT_EQ(kvstore->getVBucketState(vbid)->highSeqno, numItems);
    }

    state.SetItemsProcessed(flushCount);
}

const int NUM_ITEMS = 100000;

BENCHMARK_REGISTER_F(KVStoreBench, Scan)
        ->Args({NUM_ITEMS, COUCHSTORE})
#ifdef EP_USE_ROCKSDB
        ->Args({NUM_ITEMS, ROCKSDB})
#endif
        ;

// Arg0: Number of items to pre-populate the store with.
// Arg1: Store to use
// Arg2: Number of items to commit in each batch.
// Use an iteration count such that in each case we mutate each key
// the same number of times
// (e.g. 100 items, batch size=1 -> 100 iterations;
//       100 items, batch size=10 -> 10 iterations.

BENCHMARK_REGISTER_F(KVStoreBench, Commit)
        ->Args({100, COUCHSTORE, 1})
        ->Args({100, COUCHSTORE, 2})

        //        ->Args({100, COUCHSTORE, 10})
        //        ->Args({100, COUCHSTORE, 100})

        ->Args({1000, COUCHSTORE, 1})
        ->Args({1000, COUCHSTORE, 2})

        //        ->Args({1000, COUCHSTORE, 10})
        //        ->Args({1000, COUCHSTORE, 100})
        //        ->Args({1000, COUCHSTORE, 1000})

        ->Args({10000, COUCHSTORE, 1})
        ->Args({10000, COUCHSTORE, 2})

        //        ->Args({10000, COUCHSTORE, 10})
        //        ->Args({10000, COUCHSTORE, 100})
        //        ->Args({10000, COUCHSTORE, 1000})
        //        ->Args({10000, COUCHSTORE, 10000})

        ->Args({100000, COUCHSTORE, 1})
        ->Args({100000, COUCHSTORE, 2})
//        ->Args({100000, COUCHSTORE, 10})
//        ->Args({100000, COUCHSTORE, 100})
//        ->Args({100000, COUCHSTORE, 1000})
//        ->Args({100000, COUCHSTORE, 10000})

#if 0
#ifdef EP_USE_ROCKSDB
        ->Args({NUM_ITEMS, ROCKSDB, 1})
        ->Args({NUM_ITEMS, ROCKSDB, 10})
        ->Args({NUM_ITEMS, ROCKSDB, 100})
        ->Args({NUM_ITEMS, ROCKSDB, 1000})
        ->Args({NUM_ITEMS, ROCKSDB, 10000})
#endif

#endif

        ->Iterations(100000)

        // Use RealTime to measure; given a lot of IO is performed and we
        // want to measure Wall Clock time.
        ->UseRealTime();
