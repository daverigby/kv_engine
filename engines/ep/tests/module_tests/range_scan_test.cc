/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/collection_persisted_stats.h"
#include "ep_bucket.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <memcached/range_scan_optional_configuration.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <unordered_set>
#include <vector>

// A handler implementation that just stores the scan key/items in vectors
class TestRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    // As the handler gets moved into the RangeScan, keep references to the
    // containers/status needed for validation
    TestRangeScanHandler(std::vector<std::unique_ptr<Item>>& items,
                         std::vector<StoredDocKey>& keys,
                         cb::engine_errc& status,
                         std::function<void(size_t)>& hook)
        : scannedItems(items),
          scannedKeys(keys),
          status(status),
          testHook(hook) {
    }

    void handleKey(const CookieIface&, DocKey key) override {
        checkKeyIsUnique(key);
        scannedKeys.emplace_back(key);
        testHook(scannedKeys.size());
    }

    void handleItem(const CookieIface&, std::unique_ptr<Item> item) override {
        checkKeyIsUnique(item->getKey());
        scannedItems.emplace_back(std::move(item));
        testHook(scannedItems.size());
    }

    void handleStatus(const CookieIface& cookie,
                      cb::engine_errc status) override {
        EXPECT_TRUE(validateStatus(status));
        this->status = status;
    }

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void checkKeyIsUnique(DocKey key) {
        auto [itr, emplaced] = allKeys.emplace(key);
        EXPECT_TRUE(emplaced) << "Duplicate key returned " << key.to_string();
    }

    // check for allowed/expected status code
    static bool validateStatus(cb::engine_errc code);

    std::vector<std::unique_ptr<Item>>& scannedItems;
    std::vector<StoredDocKey>& scannedKeys;
    std::unordered_set<StoredDocKey> allKeys;
    cb::engine_errc& status;
    std::function<void(size_t)>& testHook;
};

class RangeScanTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string, std::string>> {
public:
    void SetUp() override {
        config_string += generateBackendConfig(std::get<0>(GetParam()));
        config_string += ";item_eviction_policy=" + getEvictionMode();
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
        SingleThreadedEPBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Setup collections and store keys
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 3);
        storeTestKeys();
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) + "_" +
               std::get<2>(info.param);
    }

    std::string getEvictionMode() const {
        return std::get<1>(GetParam());
    }

    bool isKeyOnly() const {
        return std::get<2>(GetParam()) == "key_scan";
    }

    cb::rangescan::KeyOnly getScanType() const {
        return std::get<2>(GetParam()) == "key_scan"
                       ? cb::rangescan::KeyOnly::Yes
                       : cb::rangescan::KeyOnly::No;
    }

    cb::rangescan::Id createScan(
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            std::optional<cb::rangescan::SnapshotRequirements> seqno =
                    std::nullopt,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig =
                    std::nullopt,
            cb::engine_errc expectedStatus = cb::engine_errc::success);

    const std::unordered_set<StoredDocKey> getUserKeys() {
        // Create a number of user prefixed collections and place them in the
        // collection that we will scan.
        std::unordered_set<StoredDocKey> keys;
        for (const auto& k :
             {"user-alan", "useralan", "user.claire", "user::zoe", "users"}) {
            keys.emplace(makeStoredDocKey(k, scanCollection));
        }
        return keys;
    }

    /**
     * generate a vector containing all of the keys which will be stored before
     * the test runs. Tests can then scan for these using various start/end
     * patterns
     */
    const std::vector<StoredDocKey> generateTestKeys() {
        std::vector<StoredDocKey> keys;

        for (const auto& k : getUserKeys()) {
            keys.push_back(k);
            keys.push_back(makeStoredDocKey(k.c_str(), collection2));
            keys.push_back(makeStoredDocKey(k.c_str(), collection3));
        }

        // Add some other keys, one above and below user and then some at
        // further ends of the alphabet
        for (const auto& k : {"useq", "uses", "abcd", "uuu", "uuuu", "xyz"}) {
            keys.push_back(makeStoredDocKey(k, scanCollection));
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        // Some stuff in other collections, no real meaning to this, just other
        // data we should never hit in the scan
        for (const auto& k : {"1000", "718", "ZOOM", "U", "@@@@"}) {
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        return keys;
    }

    void storeTestKeys() {
        for (const auto& key : generateTestKeys()) {
            // store one key with xattrs to check it gets stripped
            if (key == makeStoredDocKey("users", scanCollection)) {
                store_item(vbid,
                           key,
                           createXattrValue(key.to_string()),
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_DATATYPE_XATTR);
            } else {
                // Store key with StoredDocKey::to_string as the value
                store_item(vbid, key, key.to_string());
            }
        }
        flushVBucket(vbid);
    }

    // Run a scan using the relatively low level pieces
    void testRangeScan(
            const std::unordered_set<StoredDocKey>& expectedKeys,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            size_t itemLimit = 0,
            std::chrono::milliseconds timeLimit = std::chrono::milliseconds(0),
            size_t extraContinues = 0);

    void testLessThan(std::string key);

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);

    // Tests all scan against the following collection
    const CollectionID scanCollection = CollectionEntry::vegetable.getId();
    // Tests also have data in these collections, and these deliberately enclose
    // the vegetable collection
    const CollectionID collection2 = CollectionEntry::fruit.getId();
    const CollectionID collection3 = CollectionEntry::dairy.getId();

    std::vector<std::unique_ptr<Item>> scannedItems;
    std::vector<StoredDocKey> scannedKeys;
    // default to some status RangeScan won't use
    cb::engine_errc status{cb::engine_errc::sync_write_ambiguous};
    std::function<void(size_t)> testHook = [](size_t) {};
    std::unique_ptr<TestRangeScanHandler> handler{
            std::make_unique<TestRangeScanHandler>(
                    scannedItems, scannedKeys, status, testHook)};
    CollectionsManifest cm;
};

void RangeScanTest::validateKeyScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedItems.empty());
    EXPECT_EQ(expectedKeys.size(), scannedKeys.size());
    for (const auto& key : scannedKeys) {
        // Expect to find the key
        EXPECT_EQ(1, expectedKeys.count(key));
    }
}

void RangeScanTest::validateItemScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_EQ(expectedKeys.size(), scannedItems.size());
    for (const auto& scanItem : scannedItems) {
        auto itr = expectedKeys.find(scanItem->getKey());
        // Expect to find the key
        EXPECT_NE(itr, expectedKeys.end());
        // And the value of StoredDocKey::to_string should equal the value
        EXPECT_EQ(itr->to_string(), scanItem->getValueView());
    }
}

cb::rangescan::Id RangeScanTest::createScan(
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig,
        cb::engine_errc expectedStatus) {
    // Create a new RangeScan object and give it a handler we can inspect.
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     cid,
                                     start,
                                     end,
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     snapshotReqs,
                                     samplingConfig)
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(expectedStatus, mock_waitfor_cookie(cookie));

    if (expectedStatus != cb::engine_errc::success) {
        return {};
    }

    // Next frontend will add the uuid/scan, client can be informed of the uuid
    auto status = store->createRangeScan(vbid,
                                         cid,
                                         start,
                                         end,
                                         nullptr,
                                         *cookie,
                                         getScanType(),
                                         snapshotReqs,
                                         samplingConfig);
    EXPECT_EQ(cb::engine_errc::success, status.first);

    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    auto scan = epVb.getRangeScan(status.second);
    EXPECT_TRUE(scan);
    return scan->getUuid();
}

// This method drives a range scan through create/continue/cancel for the given
// range. The test drives a range scan serially and the comments indicate where
// a frontend thread would be executing and where a background I/O task would.
void RangeScanTest::testRangeScan(
        const std::unordered_set<StoredDocKey>& expectedKeys,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit,
        size_t extraContinues) {
    // Not smart enough to test both limits yet
    EXPECT_TRUE(!(itemLimit && timeLimit.count()));

    // 1) create a RangeScan to scan the user prefixed keys.
    auto uuid = createScan(cid, start, end);

    // 2) Continue a RangeScan
    // 2.1) Frontend thread would call this method using clients uuid
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(
                      vbid, uuid, *cookie, itemLimit, timeLimit));

    // 2.2) An I/O task now reads data from disk
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // Tests will need more continues if a limit is in-play
    for (size_t count = 0; count < extraContinues; count++) {
        EXPECT_EQ(cb::engine_errc::would_block,
                  store->continueRangeScan(
                          vbid, uuid, *cookie, itemLimit, timeLimit));
        runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                    "RangeScanContinueTask");
        if (count < extraContinues - 1) {
            EXPECT_EQ(cb::engine_errc::range_scan_more, status);
        }
    }

    // 2.3) All expected keys must have been read from disk
    if (isKeyOnly()) {
        validateKeyScan(expectedKeys);
    } else {
        validateItemScan(expectedKeys);
    }
    // status was set to "success"
    EXPECT_EQ(cb::engine_errc::range_scan_complete, status);

    // In this case the scan finished and cleaned up

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->cancelRangeScan(vbid, uuid, *cookie));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(
                      vbid, uuid, *cookie, 0, std::chrono::milliseconds(0)));
}

// Scan for the user prefixed keys
TEST_P(RangeScanTest, user_prefix) {
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
}

TEST_P(RangeScanTest, exclusive_start) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("user-alan", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user-alan", cb::rangescan::KeyType::Exclusive},
                  {"user\xFF"});
}

TEST_P(RangeScanTest, exclusive_end) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanTest, exclusive_end_2) {
    // Check this zero suffixed key isn't included if it's set as the end
    // of an exclusive range
    store_item(vbid, makeStoredDocKey("users\0", scanCollection), "value");
    flushVBucket(vbid);

    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"users\0", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanTest, exclusive_range) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    expectedKeys.erase(makeStoredDocKey("user-alan", scanCollection));

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user-alan", cb::rangescan::KeyType::Exclusive},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanTest, user_prefix_with_item_limit_1) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  1,
                  std::chrono::milliseconds(0),
                  expectedKeys.size());
}

TEST_P(RangeScanTest, user_prefix_with_item_limit_2) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  2,
                  std::chrono::milliseconds(0),
                  expectedKeys.size() / 2);
}

TEST_P(RangeScanTest, user_prefix_with_time_limit) {
    // Replace time with a function that ticks per call, forcing the scan to
    // yield for every item
    RangeScan::setClockFunction([]() {
        static auto now = std::chrono::steady_clock::now();
        now += std::chrono::milliseconds(100);
        return now;
    });
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  0,
                  std::chrono::milliseconds(10),
                  expectedKeys.size());
}

// Test ensures callbacks cover disk read case
TEST_P(RangeScanTest, user_prefix_evicted) {
    for (const auto& key : generateTestKeys()) {
        evict_key(vbid, key);
    }
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  2,
                  std::chrono::milliseconds(0),
                  expectedKeys.size() / 2);
}

// Run a >= user scan by setting the keys to user and the end (255)
TEST_P(RangeScanTest, greater_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeStart = makeStoredDocKey("user", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys, scanCollection, {"user"}, {"\xFF"});
}

// Run a <= user scan y setting the keys to 0 and user\xFF
TEST_P(RangeScanTest, less_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeEnd = makeStoredDocKey("user\xFF", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but <= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {"user\xFF"});
}

// Perform > uuu, this simulates a request for an exclusive start range-scan
TEST_P(RangeScanTest, greater_than) {
    // Here the client could of specified "aaa" and flag to set exclusive-start
    // so we set the start to "skip" aaa and start from the next key

    // This test kind of walks through how a client may be resuming after the
    // scan being destroyed for some reason (restart/rebalance).
    // key "uuu" is the last received key, so they'd like to receive in the next
    // scan all keys greater then uuu, but not uuu itself (exclusive start or >)
    std::string key = "uuu";

    // In this case the client requests exclusive start and we manipulate the
    // key to achieve exactly that by appending the value of 0
    key += char(0);
    auto rangeStart = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeStart as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeStart, rangeStart.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeStart is logically greater than
    // uuu so >= here will select all keys we expect to see in the result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeStart);

    testRangeScan(expectedKeys, scanCollection, {key}, {"\xFF"});
}

// Run a > "user" scan using the KeyType
TEST_P(RangeScanTest, greater_than_using_KeyType) {
    auto expectedKeys = getUserKeys();
    auto rangeStart = makeStoredDocKey("user", scanCollection);
    expectedKeys.erase(rangeStart);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but > will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() > rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user", cb::rangescan::KeyType::Exclusive},
                  {"\xFF"});
}

// scan for less than key
void RangeScanTest::testLessThan(std::string key) {
    // In this case the client requests an exclusive end and we manipulate the
    // key by changing the suffix character, pop or subtract based on value
    if (key.back() == char(0)) {
        key.pop_back();
    } else {
        key.back()--;
    }

    auto rangeEnd = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeEnd as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeEnd, rangeEnd.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeEnd is logically less than
    // the input key  so <=here will select all keys we expect to see in the
    // result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeEnd);

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {key});
}

TEST_P(RangeScanTest, less_than) {
    testLessThan("uuu");
}

TEST_P(RangeScanTest, less_than_with_zero_suffix) {
    std::string key = "uuu";
    key += char(0);
    testLessThan(key);
}

// Run a < "users" scan using the KeyType
TEST_P(RangeScanTest, less_than_using_KeyType) {
    auto expectedKeys = getUserKeys();
    auto rangeEnd = makeStoredDocKey("users", scanCollection);
    expectedKeys.erase(rangeEnd);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() < rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"\0"},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

// Test that we reject continue whilst a scan is already being continued
TEST_P(RangeScanTest, continue_must_be_serialised) {
    auto uuid = createScan(scanCollection, {"a"}, {"b"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    EXPECT_TRUE(epVb.getRangeScan(uuid)->isContinuing());

    // Cannot continue again
    EXPECT_EQ(cb::engine_errc::too_busy,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));

    // But can cancel
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, true));

    // must run the cancel (so for magma we can shutdown)
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");
}

// Create and then straight to cancel
TEST_P(RangeScanTest, create_cancel) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, true));
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_TRUE(scannedItems.empty());
    // No status pushed through, the handler->status only applies to continue
    // expect status to still be our initialisation value
    EXPECT_EQ(cb::engine_errc::sync_write_ambiguous, status);
}

TEST_P(RangeScanTest, create_no_data) {
    // create will fail if no data is in range
    createScan(scanCollection,
               {"0"},
               {"0\xFF"},
               {},
               {/* no sampling config*/},
               cb::engine_errc::no_such_key);
}

// Check that if a scan has been continue (but is waiting to run), it can be
// cancelled. When the task runs the scan cancels.
TEST_P(RangeScanTest, create_continue_is_cancelled) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));

    // Cancel
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, true));

    // Note that at the moment continue and cancel are creating new tasks.
    // run them both and future changes will clean this up.
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // First task cancels, nothing read
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_TRUE(scannedItems.empty());
    // and handler was notified of the cancel status
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, status);

    // set to some status we don't use
    status = cb::engine_errc::sync_write_pending;

    // second task runs, but won't do anything
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // no change
    EXPECT_EQ(cb::engine_errc::sync_write_pending, status);
}

// Test that a scan doesn't keep on reading if a cancel occurs during the I/O
// task run
TEST_P(RangeScanTest, create_continue_is_cancelled_2) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));

    // Set a hook which will cancel when the 2nd key is read
    testHook = [&vb, uuid, this](size_t count) {
        EXPECT_LT(count, 3); // never reach third key
        if (count == 2) {
            EXPECT_EQ(cb::engine_errc::success,
                      vb->cancelRangeScan(uuid, cookie, true));
        }
    };

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, status);

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vb->cancelRangeScan(uuid, cookie, true));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));

    // Scan only read 2 of the possible keys
    if (isKeyOnly()) {
        EXPECT_EQ(2, scannedKeys.size());
    } else {
        EXPECT_EQ(2, scannedItems.size());
    }
    // And set our status to cancelled
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, status);

    // must run the cancel (so for magma we can shutdown)
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");
}

TEST_P(RangeScanTest, snapshot_does_not_contain_seqno_0) {
    auto vb = store->getVBucket(vbid);
    // require persisted upto 0 and something found at 0
    cb::rangescan::SnapshotRequirements reqs{
            vb->failovers->getLatestUUID(),
            0, /* persieted up to 0 */
            std::nullopt,
            true /* something must exist at seqno 0*/};
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::not_stored);
}

TEST_P(RangeScanTest, snapshot_does_not_contain_seqno) {
    auto vb = store->getVBucket(vbid);
    // Store, capture high-seqno and update so it's gone from the snapshot
    store_item(vbid, StoredDocKey("update_me", scanCollection), "1");
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    store_item(vbid, StoredDocKey("update_me", scanCollection), "2");
    flushVBucket(vbid);
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::not_stored);
}

TEST_P(RangeScanTest, snapshot_upto_seqno) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             false};
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, false));
}

TEST_P(RangeScanTest, snapshot_contains_seqno) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, false));
}

// There is no wait option, so a future seqno is a failure
TEST_P(RangeScanTest, future_seqno_fails) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             false};
    // This error is detected on first invocation, no need for ewouldblock
    // this seqno check occurs in EPBucket, so don't test via VBucket
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {/* no sampling config*/})
                      .first);
}

TEST_P(RangeScanTest, vb_uuid_check) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{
            1, uint64_t(vb->getHighSeqno()), std::nullopt, false};
    // This error is detected on first invocation, no need for ewouldblock
    // uuid check occurs in EPBucket, so don't test via VBucket
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {/* no sampling config*/})
                      .first);
}

TEST_P(RangeScanTest, random_sample_not_enough_items) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // Request more samples than keys, which is not allowed
    auto sampleSize = stats[scanCollection].itemCount + 1;
    createScan(scanCollection,
               {"\0", 1},
               {"\xFF"},
               {/* no snapshot requirements */},
               cb::rangescan::SamplingConfiguration{sampleSize, 0},
               cb::engine_errc::out_of_range);
}

TEST_P(RangeScanTest, random_sample) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample up to 1/2 of the keys from the collection, we may not get
    // exactly 50% of the keys returned though.
    auto sampleSize = stats[scanCollection].itemCount / 2;

    // key ranges covers all keys in scanCollection, kv_engine will do this
    // not the client. Note the seed chosen here ensures a reasonable number
    // of keys are returned. Given that the unit tests only store a small number
    // of keys, it's possible to find a seed which returned nothing.
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});

    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(
                      uuid, *cookie, 0, std::chrono::milliseconds(0)));

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // the chosen seed, results in 1 less key than desired
    if (isKeyOnly()) {
        EXPECT_EQ(sampleSize - 1, scannedKeys.size());
    } else {
        EXPECT_EQ(sampleSize - 1, scannedItems.size());
    }
}

TEST_P(RangeScanTest, random_sample_with_limit_1) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample 1/2 of the keys from the collection
    auto sampleSize = stats[scanCollection].itemCount / 2;

    // key ranges covers all keys in scanCollection, kv_engine will do this
    // not the client
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});

    auto vb = store->getVBucket(vbid);

    // 1 key returned per continue (limit=1)
    // + 1 extra continue will bring it to 'self cancel'
    for (size_t ii = 0; ii < sampleSize; ii++) {
        EXPECT_EQ(cb::engine_errc::would_block,
                  vb->continueRangeScan(
                          uuid, *cookie, 1, std::chrono::milliseconds(0)));

        runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                    "RangeScanContinueTask");
    }

    // See comments RangeScanTest::random_sample regarding sampleSize adjustment
    if (isKeyOnly()) {
        EXPECT_EQ(sampleSize - 1, scannedKeys.size());
    } else {
        EXPECT_EQ(sampleSize - 1, scannedItems.size());
    }
}

TEST_P(RangeScanTest, not_my_vbucket) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              store->createRangeScan(Vbid(4),
                                     scanCollection,
                                     {"\0", 1},
                                     {"\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
}

TEST_P(RangeScanTest, unknown_collection) {
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(vbid,
                                     CollectionEntry::meat.getId(),
                                     {"\0", 1},
                                     {"\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
}

// Test that the collection going away after part 1 of create, cleans up
TEST_P(RangeScanTest, scan_cancels_after_create) {
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));

    // Drop scanCollection
    EXPECT_EQ(scanCollection, CollectionEntry::vegetable.getId());
    auto* cookie2 = create_mock_cookie();
    setCollections(cookie2, cm.remove(CollectionEntry::vegetable));

    // Second part of create runs and fails
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
    destroy_mock_cookie(cookie2);

    // Task was scheduled to cancel (close the snapshot)
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // Can't get hold of the scan object as we never got the uuid
}

// Test that if the vbucket changes after create, but before we run the create
// task, it is detected when snapshot_requirements are in use
TEST_P(RangeScanTest, scan_detects_vbucket_change) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             false};
    vb.reset();

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);

    // destroy and create the vbucket - a new uuid will be created
    EXPECT_TRUE(store->resetVBucket(vbid));

    // Need to poke a few functions to get the create ready to run.
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "Removing (dead) vb:0 from memory and disk");

    // Force a state change to active, generating a new UUID. As the collection
    // state is not "epoch", this new vbucket will pick up 3 mutations, so we
    // use flushVBucket directly and not setVBucketStateAndRunPersistTask as
    // the latter expects 0 items flushed.
    setVBucketState(vbid, vbucket_state_active);
    flushVBucket(vbid); // ensures vb on disk has the new uuid

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    // task detected the vbucket has changed and aborted
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, mock_waitfor_cookie(cookie));
}

// Test that if the vbucket changes after during the continue phase, the scan
// stops. In theory we could still keep scanning as we have the correct
// snapshot open, but some event has occurred that the scan client should be
// aware of, the scan would also need to ignore any in-memory values, simpler
// to just end the scan and report the vbucket change to the client.
// Note for this test, no snapshot requirements are needed.
TEST_P(RangeScanTest, scan_detects_vbucket_change_during_continue) {
    // Create the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           {/* no snapshot requirements */},
                           {/* no sampling*/});

    // Continue
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(
                      vbid, uuid, *cookie, 0, std::chrono::milliseconds(0)));

    // destroy and create the vbucket - a new uuid will be created
    EXPECT_TRUE(store->resetVBucket(vbid));

    // Need to poke a few functions to get the create ready to run.
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "Removing (dead) vb:0 from memory and disk");

    // Force a state change to active, generating a new UUID. As the collection
    // state is not "epoch", this new vbucket will pick up 3 mutations, so we
    // use flushVBucket directly and not setVBucketStateAndRunPersistTask as
    // the latter expects 0 items flushed.
    setVBucketState(vbid, vbucket_state_active);
    // No need to flush in this test as only in memory VB can be inspected for
    // the uuid change

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // The scan task detected that it was cancelled
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, status);

    // scan has gone now
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(
                      vbid, uuid, *cookie, 0, std::chrono::milliseconds(0)));
}

TEST_P(RangeScanTest, wait_for_persistence_success) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::chrono::milliseconds(100),
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     nullptr,
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);

    // store our item and flush (so the waitForPersistence is notified)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    EXPECT_EQ(1, vb->getHighPriorityChkSize());
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
    EXPECT_EQ(0, vb->getHighPriorityChkSize());

    // Now the task will move to create, we can drive the scan using our wrapper
    // it will do the next ewouldblock phase finally creating the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);

    // Close the scan
    EXPECT_EQ(cb::engine_errc::success,
              vb->cancelRangeScan(uuid, cookie, false));
}

TEST_P(RangeScanTest, wait_for_persistence_fails) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. No timeout so fails on
    // the first crack of the whip
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             false};

    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);
}

TEST_P(RangeScanTest, wait_for_persistence_timeout) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    // set the timeout to 0, so first flush will expire
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 2),
                                             std::chrono::milliseconds(0),
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     std::move(handler),
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);

    // store an item and flush (so the waitForPersistence is notified and
    // expired)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::temporary_failure, mock_waitfor_cookie(cookie));
}

bool TestRangeScanHandler::validateStatus(cb::engine_errc code) {
    switch (code) {
    case cb::engine_errc::success:
    case cb::engine_errc::not_my_vbucket:
    case cb::engine_errc::unknown_collection:
    case cb::engine_errc::range_scan_cancelled:
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::range_scan_complete:
    case cb::engine_errc::failed:
        return true;
    case cb::engine_errc::no_such_key:
    case cb::engine_errc::key_already_exists:
    case cb::engine_errc::no_memory:
    case cb::engine_errc::not_stored:
    case cb::engine_errc::invalid_arguments:
    case cb::engine_errc::not_supported:
    case cb::engine_errc::would_block:
    case cb::engine_errc::too_big:
    case cb::engine_errc::too_many_connections:
    case cb::engine_errc::disconnect:
    case cb::engine_errc::no_access:
    case cb::engine_errc::temporary_failure:
    case cb::engine_errc::out_of_range:
    case cb::engine_errc::rollback:
    case cb::engine_errc::no_bucket:
    case cb::engine_errc::too_busy:
    case cb::engine_errc::authentication_stale:
    case cb::engine_errc::delta_badval:
    case cb::engine_errc::locked:
    case cb::engine_errc::locked_tmpfail:
    case cb::engine_errc::predicate_failed:
    case cb::engine_errc::cannot_apply_collections_manifest:
    case cb::engine_errc::unknown_scope:
    case cb::engine_errc::durability_impossible:
    case cb::engine_errc::sync_write_in_progress:
    case cb::engine_errc::sync_write_ambiguous:
    case cb::engine_errc::dcp_streamid_invalid:
    case cb::engine_errc::durability_invalid_level:
    case cb::engine_errc::sync_write_re_commit_in_progress:
    case cb::engine_errc::sync_write_pending:
    case cb::engine_errc::stream_not_found:
    case cb::engine_errc::opaque_no_match:
    case cb::engine_errc::scope_size_limit_exceeded:
        return false;
    };
    throw std::invalid_argument(
            "TestRangeScanHandler::validateStatus: code does not represent a "
            "legal error code: " +
            std::to_string(int(code)));
}

auto scanConfigValues =
        ::testing::Combine(::testing::Values("persistent_couchdb"
#ifdef EP_USE_MAGMA
                                             ,
                                             "persistent_magma",
                                             "persistent_nexus_couchstore_magma"
#endif
                                             ),
                           ::testing::Values("value_only", "full_eviction"),
                           ::testing::Values("key_scan", "value_scan"));

INSTANTIATE_TEST_SUITE_P(RangeScanFullAndValueEviction,
                         RangeScanTest,
                         scanConfigValues,
                         RangeScanTest::PrintToStringParamName);