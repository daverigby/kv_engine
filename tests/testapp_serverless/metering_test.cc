/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>
#include <protocol/connection/async_client_connection.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <serverless/config.h>
#include <deque>

namespace cb::test {

class MeteringTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("metering");
        conn->dropPrivilege(cb::rbac::Privilege::Unmetered);
        conn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
    }

    static void TearDownTestCase() {
        conn.reset();
    }

protected:
    static std::unique_ptr<MemcachedConnection> conn;
};

std::unique_ptr<MemcachedConnection> MeteringTest::conn;

/// Verify that the unmetered privilege allows to execute commands
/// were its usage isn't being metered.
TEST_F(MeteringTest, UnmeteredPrivilege) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("metering");

    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");

    Document doc;
    doc.info.id = "UnmeteredPrivilege";
    doc.value = "This is the value";
    admin->mutate(doc, Vbid{0}, MutationType::Set);
    admin->get("UnmeteredPrivilege", Vbid{0});

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");

    EXPECT_EQ(before["ru"].get<std::size_t>(), after["ru"].get<std::size_t>());
    EXPECT_EQ(before["wu"].get<std::size_t>(), after["wu"].get<std::size_t>());
    EXPECT_EQ(before["num_commands_with_metered_units"].get<std::size_t>(),
              after["num_commands_with_metered_units"].get<std::size_t>());

    // Drop the privilege and verify that the counters increase
    admin->dropPrivilege(cb::rbac::Privilege::Unmetered);
    admin->get("UnmeteredPrivilege", Vbid{0});
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");

    EXPECT_EQ(1,
              after["ru"].get<std::size_t>() - before["ru"].get<std::size_t>());
    EXPECT_EQ(before["wu"].get<std::size_t>(), after["wu"].get<std::size_t>());
    EXPECT_EQ(1,
              after["num_commands_with_metered_units"].get<std::size_t>() -
                      before["num_commands_with_metered_units"]
                              .get<std::size_t>());
}

TEST_F(MeteringTest, UnitsReported) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("bucket-0", "bucket-0");
    conn->selectBucket("bucket-0");
    conn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
    conn->setReadTimeout(std::chrono::seconds{3});

    DocumentInfo info;
    info.id = "UnitsReported";

    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("This is a document");
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    ASSERT_TRUE(rsp.isSuccess());

    auto ru = rsp.getReadUnits();
    auto wu = rsp.getWriteUnits();

    ASSERT_FALSE(ru.has_value()) << "mutate should not use RU";
    ASSERT_TRUE(wu.has_value()) << "mutate should use WU";
    ASSERT_EQ(1, *wu) << "The value should be 1 WU";
    wu.reset();

    rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, info.id});

    ru = rsp.getReadUnits();
    wu = rsp.getWriteUnits();

    ASSERT_TRUE(ru.has_value()) << "get should use RU";
    ASSERT_FALSE(wu.has_value()) << "get should not use WU";
    ASSERT_EQ(1, *ru) << "The value should be 1 RU";
}

class DcpDrain {
public:
    DcpDrain(const std::string host,
             const std::string port,
             const std::string username,
             const std::string password,
             const std::string bucketname)
        : host(std::move(host)),
          port(std::move(port)),
          username(std::move(username)),
          password(std::move(password)),
          bucketname(std::move(bucketname)),
          connection(AsyncClientConnection::create(base)) {
        connection->setIoErrorListener(
                [this](AsyncClientConnection::Direction dir,
                       const folly::AsyncSocketException& ex) {
                    error = ex.what();
                    base.terminateLoopSoon();
                });
    }

    void drain() {
        connect();
        // We need to use PLAIN auth as we're using the external auth
        // service
        connection->authenticate(username, password, "PLAIN");
        setFeatures();
        selectBucket();

        openDcp();

        setControlMessages();

        sendStreamRequest();

        connection->setFrameReceivedListener(
                [this](const auto& header) { onFrameReceived(header); });

        // Now loop until we're done
        base.loopForever();
        if (error) {
            throw std::runtime_error(*error);
        }
    }

    size_t getNumMutations() const {
        return num_mutations;
    }

    size_t getRu() const {
        return ru;
    }

protected:
    void onFrameReceived(const cb::mcbp::Header& header) {
        if (header.isRequest()) {
            onRequest(header.getRequest());
        } else {
            onResponse(header.getResponse());
        }
    }

    void onResponse(const cb::mcbp::Response& res) {
        if (res.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamReq) {
            if (!cb::mcbp::isStatusSuccess(res.getStatus())) {
                error = "onResponse::DcpStreamReq returned error: " +
                        ::to_string(res.getStatus());
                base.terminateLoopSoon();
            }
        } else {
            error = "onResponse(): Unexpected message received: " +
                    res.toJSON(false).dump();
            base.terminateLoopSoon();
        }
    }

    std::size_t calcRu(std::size_t size) {
        return (size + 1023) / 1024;
    }

    void onRequest(const cb::mcbp::Request& req) {
        if (req.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamEnd) {
            base.terminateLoopSoon();
        }

        switch (req.getClientOpcode()) {
        case cb::mcbp::ClientOpcode::DcpStreamEnd:
            base.terminateLoopSoon();
            break;
        case cb::mcbp::ClientOpcode::DcpNoop:
            handleDcpNoop(req);
            break;
        case cb::mcbp::ClientOpcode::DcpMutation:
            ++num_mutations;
            ru += calcRu(req.getValue().size() + req.getKey().size());
            break;
        case cb::mcbp::ClientOpcode::DcpDeletion:
            ++num_deletions;
            ru += calcRu(req.getValue().size() + req.getKey().size());
            break;
        case cb::mcbp::ClientOpcode::DcpExpiration:
            ++num_expirations;
            ru += calcRu(req.getValue().size() + req.getKey().size());
            break;

        case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
            break;

        case cb::mcbp::ClientOpcode::DcpAddStream:
        case cb::mcbp::ClientOpcode::DcpCloseStream:
        case cb::mcbp::ClientOpcode::DcpStreamReq:
        case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
        case cb::mcbp::ClientOpcode::DcpFlush_Unsupported:
        case cb::mcbp::ClientOpcode::DcpSetVbucketState:
        case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
        case cb::mcbp::ClientOpcode::DcpControl:
        case cb::mcbp::ClientOpcode::DcpSystemEvent:
        case cb::mcbp::ClientOpcode::DcpPrepare:
        case cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged:
        case cb::mcbp::ClientOpcode::DcpCommit:
        case cb::mcbp::ClientOpcode::DcpAbort:
        case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
        case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
            // fallthrough
        default:
            error = "Received unexpected message: " + req.toJSON(false).dump();
            base.terminateLoopSoon();
        }
    }

    void connect() {
        connection->setConnectListener([this]() { base.terminateLoopSoon(); });
        connection->connect(host, port);
        base.loopForever();
        if (error) {
            throw std::runtime_error("DcpDrain::connect: " + *error);
        }
    }

    void setFeatures() {
        using cb::mcbp::Feature;

        const std::vector<Feature> requested{{Feature::MUTATION_SEQNO,
                                              Feature::XATTR,
                                              Feature::XERROR,
                                              Feature::SNAPPY,
                                              Feature::JSON,
                                              Feature::Tracing,
                                              Feature::Collections,
                                              Feature::ReportUnitUsage}};

        auto enabled = connection->hello("serverless", "MeterDCP", requested);
        if (enabled != requested) {
            throw std::runtime_error(
                    "DcpDrain::setFeatures(): Failed to enable the "
                    "requested "
                    "features");
        }
    }

    void selectBucket() {
        const auto rsp = connection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, bucketname});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    "DcpDrain::selectBucket: " + ::to_string(rsp.getStatus()) +
                    " " + rsp.getDataString());
        }
    }

    void openDcp() {
        const auto rsp = connection->execute(BinprotDcpOpenCommand{
                "MeterDcpName", cb::mcbp::request::DcpOpenPayload::Producer});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    "DcpDrain::openDcp: " + ::to_string(rsp.getStatus()) + " " +
                    rsp.getDataString());
        }
    }

    void setControlMessages() {
        auto setCtrlMessage = [this](const std::string& key,
                                     const std::string& value) {
            const auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::DcpControl, key, value});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        };
        std::vector<std::pair<std::string, std::string>> controls{
                {"set_priority", "high"},
                {"supports_cursor_dropping_vulcan", "true"},
                {"supports_hifi_MFU", "true"},
                {"send_stream_end_on_client_close_stream", "true"},
                {"enable_expiry_opcode", "true"},
                {"set_noop_interval", "1"},
                {"enable_noop", "true"}};
        for (const auto& [k, v] : controls) {
            setCtrlMessage(k, v);
        }
    }

    void sendStreamRequest() {
        BinprotDcpStreamRequestCommand cmd;
        cmd.setDcpFlags(DCP_ADD_STREAM_FLAG_TO_LATEST);
        cmd.setDcpReserved(0);
        cmd.setDcpStartSeqno(0);
        cmd.setDcpEndSeqno(~0);
        cmd.setDcpVbucketUuid(0);
        cmd.setDcpSnapStartSeqno(0);
        cmd.setDcpSnapEndSeqno(0);
        cmd.setVBucket(Vbid(0));

        connection->send(cmd);
    }

    void handleDcpNoop(const cb::mcbp::Request& header) {
        cb::mcbp::Response resp = {};
        resp.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.setOpaque(header.getOpaque());
        resp.setOpcode(header.getClientOpcode());

        auto iob = folly::IOBuf::createCombined(sizeof(resp));
        std::memcpy(iob->writableData(), &resp, sizeof(resp));
        iob->append(sizeof(resp));
        connection->send(std::move(iob));
    }

    const std::string host;
    const std::string port;
    const std::string username;
    const std::string password;
    const std::string bucketname;
    folly::EventBase base;
    std::unique_ptr<AsyncClientConnection> connection;
    std::optional<std::string> error;
    std::size_t num_mutations = 0;
    std::size_t num_deletions = 0;
    std::size_t num_expirations = 0;
    std::size_t ru = 0;
};

/// Test that we meter all operations according to their spec (well, there
/// is no spec at the moment ;)
///
/// To make sure that we don't sneak in a new opcode without considering if
/// it should be metered or not the code loops over all available opcodes
/// and call a function which performs a switch (so the compiler will barf
/// out if we don't handle the case). By doing so one must explicitly think
/// if the new opcode needs to be metered or not.
TEST_F(MeteringTest, DISABLED_OpsMetered) {
    using namespace cb::mcbp;
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->dropPrivilege(cb::rbac::Privilege::Unmetered);

    auto executeWithExpectedCU = [&admin](std::function<void()> func,
                                          size_t ru,
                                          size_t wu) {
        nlohmann::json before;
        admin->stats([&before](auto k,
                               auto v) { before = nlohmann::json::parse(v); },
                     "bucket_details metering");
        func();
        nlohmann::json after;
        admin->stats(
                [&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");
        EXPECT_EQ(ru, after["ru"].get<size_t>() - before["ru"].get<size_t>());
        EXPECT_EQ(wu, after["wu"].get<size_t>() - before["wu"].get<size_t>());
    };

    auto testOpcode = [&executeWithExpectedCU](MemcachedConnection& conn,
                                               ClientOpcode opcode) {
        auto createDocument = [&conn](std::string key,
                                      std::string value,
                                      MutationType op = MutationType::Set,
                                      uint64_t cas = 0) {
            Document doc;
            doc.info.id = std::move(key);
            doc.info.cas = cas;
            doc.value = std::move(value);
            return conn.mutate(doc, Vbid{0}, op);
        };

        BinprotResponse rsp;
        switch (opcode) {
        case ClientOpcode::Flush:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq:
        case ClientOpcode::Getq:
        case ClientOpcode::Getk:
        case ClientOpcode::Getkq:
        case ClientOpcode::Gatq:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::GetqMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelqWithMeta:
        case ClientOpcode::Rget_Unsupported:
        case ClientOpcode::Rset_Unsupported:
        case ClientOpcode::Rsetq_Unsupported:
        case ClientOpcode::Rappend_Unsupported:
        case ClientOpcode::Rappendq_Unsupported:
        case ClientOpcode::Rprepend_Unsupported:
        case ClientOpcode::Rprependq_Unsupported:
        case ClientOpcode::Rdelete_Unsupported:
        case ClientOpcode::Rdeleteq_Unsupported:
        case ClientOpcode::Rincr_Unsupported:
        case ClientOpcode::Rincrq_Unsupported:
        case ClientOpcode::Rdecr_Unsupported:
        case ClientOpcode::Rdecrq_Unsupported:
        case ClientOpcode::TapConnect_Unsupported:
        case ClientOpcode::TapMutation_Unsupported:
        case ClientOpcode::TapDelete_Unsupported:
        case ClientOpcode::TapFlush_Unsupported:
        case ClientOpcode::TapOpaque_Unsupported:
        case ClientOpcode::TapVbucketSet_Unsupported:
        case ClientOpcode::TapCheckpointStart_Unsupported:
        case ClientOpcode::TapCheckpointEnd_Unsupported:
        case ClientOpcode::ResetReplicationChain_Unsupported:
        case ClientOpcode::SnapshotVbStates_Unsupported:
        case ClientOpcode::VbucketBatchCount_Unsupported:
        case ClientOpcode::NotifyVbucketUpdate_Unsupported:
        case ClientOpcode::ChangeVbFilter_Unsupported:
        case ClientOpcode::CheckpointPersistence_Unsupported:
        case ClientOpcode::SetDriftCounterState_Unsupported:
        case ClientOpcode::GetAdjustedTime_Unsupported:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DeregisterTapClient_Unsupported:
            // Just verify that we don't support them
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_EQ(Status::NotSupported, rsp.getStatus()) << opcode;

            // SASL commands aren't being metered (and not necessairly bound
            // to a bucket so its hard to check as we don't know where it'll
        // go
        case ClientOpcode::SaslListMechs:
        case ClientOpcode::SaslAuth:
        case ClientOpcode::SaslStep:
            break;

        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::SelectBucket:
            break;

            // Quit close the connection so its hard to test (and it would
        // be weird if someone updated the code to start collecting data)
        case ClientOpcode::Quit:
            executeWithExpectedCU(
                    [&conn]() {
                        conn.sendCommand(
                                BinprotGenericCommand{ClientOpcode::Quit});
                        // Allow some time for the connection to disconnect
                        std::this_thread::sleep_for(
                                std::chrono::milliseconds{500});
                    },
                    0,
                    0);
            conn.reconnect();
            conn.authenticate("@admin", "password");
            conn.selectBucket("metering");
            conn.dropPrivilege(cb::rbac::Privilege::Unmetered);
            conn.setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
            conn.setReadTimeout(std::chrono::seconds{3});
            break;

        case ClientOpcode::ListBuckets:
        case ClientOpcode::Version:
        case ClientOpcode::Noop:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::GetFailoverLog:
        case ClientOpcode::CollectionsGetManifest:
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_TRUE(rsp.isSuccess()) << opcode;
            EXPECT_FALSE(rsp.getReadUnits()) << opcode;
            EXPECT_FALSE(rsp.getWriteUnits()) << opcode;
            break;

        case ClientOpcode::Set:
        case ClientOpcode::Add:
        case ClientOpcode::Replace:
        case ClientOpcode::Append:
        case ClientOpcode::Prepend:
            // Tested in MeterDocumentSimpleMutations
            break;
        case ClientOpcode::Delete:
            // Tested in MeterDocumentDelete
            break;
        case ClientOpcode::Increment:
        case ClientOpcode::Decrement:
            // Tested in TestArithmeticMethods
            break;
        case ClientOpcode::Stat:
            executeWithExpectedCU([&conn]() { conn.stats(""); }, 0, 0);
            break;
        case ClientOpcode::Verbosity:
            rsp = conn.execute(BinprotVerbosityCommand{0});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::Touch:
        case ClientOpcode::Gat:
            // Tested in MeterDocumentTouch
            break;
        case ClientOpcode::Hello:
            executeWithExpectedCU(
                    [&conn]() {
                        conn.setFeature(Feature::AltRequestSupport, true);
                    },
                    0,
                    0);
            break;

        case ClientOpcode::Get:
        case ClientOpcode::GetReplica:
            // Tested in MeterDocumentGet
            break;

        case ClientOpcode::GetLocked:
        case ClientOpcode::UnlockKey:
            // Tested in MeterDocumentLocking
            break;

        case ClientOpcode::ObserveSeqno:
            do {
                uint64_t uuid = 0;
                conn.stats(
                        [&uuid](auto k, auto v) {
                            if (k == "vb_0:uuid") {
                                uuid = std::stoull(v);
                            }
                        },
                        "vbucket-details 0");
                rsp = conn.execute(BinprotObserveSeqnoCommand{Vbid{0}, uuid});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                EXPECT_FALSE(rsp.getWriteUnits());
            } while (false);
            break;
        case ClientOpcode::Observe:
            do {
                createDocument("ClientOpcode::Observe", "myvalue");
                std::vector<std::pair<Vbid, std::string>> keys;
                keys.emplace_back(std::make_pair<Vbid, std::string>(
                        Vbid{0}, "ClientOpcode::Observe"));
                rsp = conn.execute(BinprotObserveCommand{std::move(keys)});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                EXPECT_FALSE(rsp.getWriteUnits());
            } while (false);
            break;
        case ClientOpcode::GetMeta:
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::GetMeta"});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus()) << rsp.getStatus();
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            createDocument("ClientOpcode::GetMeta", "myvalue");
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::GetMeta"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::GetRandomKey:
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_NE(0, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::SeqnoPersistence:
            break;
        case ClientOpcode::GetKeys:
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, std::string{"\0", 1}});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::CollectionsGetID:
            break;
        case ClientOpcode::CollectionsGetScopeID:
            break;

        case ClientOpcode::SubdocGet:
            createDocument("ClientOpcode::SubdocGet",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocGet", "hello"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocExists:
            createDocument("ClientOpcode::SubdocExists",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocExists", "hello"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocDictAdd:
        case ClientOpcode::SubdocDictUpsert:
            createDocument("ClientOpcode::SubdocDictAdd",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocDictAdd", "add", "true"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocDelete:
            createDocument("ClientOpcode::SubdocDelete",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocDelete", "hello"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocReplace:
            createDocument("ClientOpcode::SubdocReplace",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocReplace",
                                         "hello",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocArrayPushLast:
        case ClientOpcode::SubdocArrayPushFirst:
        case ClientOpcode::SubdocArrayAddUnique:
            createDocument("ClientOpcode::SubdocArrayPush",
                           R"({ "hello" : ["world"]})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocArrayPush",
                                         "hello",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocArrayInsert:
            createDocument("ClientOpcode::SubdocArrayPush",
                           R"({ "hello" : ["world"]})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocArrayPush",
                                         "hello.[0]",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocCounter:
            createDocument("ClientOpcode::SubdocCounter",
                           R"({ "counter" : 0})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocCounter", "counter", "1"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            ASSERT_TRUE(rsp.getWriteUnits());
            EXPECT_EQ(1, *rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocGetCount:
            createDocument("ClientOpcode::SubdocGetCount",
                           R"({ "array" : [0,1,2,3,4]})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocGetCount", "array"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(1, *rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::SubdocMultiLookup:
            do {
                createDocument(
                        "ClientOpcode::SubdocMultiLookup",
                        R"({ "array" : [0,1,2,3,4], "hello" : "world"})");

                rsp = conn.execute(BinprotSubdocMultiLookupCommand{
                        "ClientOpcode::SubdocMultiLookup",
                        {
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array.[0]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array.[1]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array[4]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "hello"},
                        },
                        ::mcbp::subdoc::doc_flag::None});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                ASSERT_TRUE(rsp.getReadUnits());
                EXPECT_EQ(1, *rsp.getReadUnits());
                EXPECT_FALSE(rsp.getWriteUnits());
            } while (false);
            break;
        case ClientOpcode::SubdocMultiMutation:
            do {
                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocMultiMutation",
                        {
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo",
                                 "true"},
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo1",
                                 "true"},
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo2",
                                 "true"},
                        },
                        ::mcbp::subdoc::doc_flag::Mkdoc});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                ASSERT_TRUE(rsp.getWriteUnits());
                EXPECT_EQ(1, *rsp.getWriteUnits());
            } while (false);
            break;
        case ClientOpcode::SubdocReplaceBodyWithXattr:
            do {
                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocReplaceBodyWithXattr",
                        {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                          SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                          "tnx.op.staged",
                          R"({"couchbase": {"version": "cheshire-cat", "next_version": "unknown"}})"},
                         {cb::mcbp::ClientOpcode::SubdocDictUpsert,
                          SUBDOC_FLAG_NONE,
                          "couchbase",
                          R"({"version": "mad-hatter", "next_version": "cheshire-cat"})"}},
                        ::mcbp::subdoc::doc_flag::Mkdoc});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                ASSERT_TRUE(rsp.getWriteUnits());
                EXPECT_EQ(1, *rsp.getWriteUnits());

                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocReplaceBodyWithXattr",
                        {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
                          SUBDOC_FLAG_XATTR_PATH,
                          "tnx.op.staged",
                          {}},
                         {cb::mcbp::ClientOpcode::SubdocDelete,
                          SUBDOC_FLAG_XATTR_PATH,
                          "tnx.op.staged",
                          {}}},
                        ::mcbp::subdoc::doc_flag::None});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                ASSERT_TRUE(rsp.getReadUnits());
                EXPECT_EQ(1, *rsp.getReadUnits());
                ASSERT_TRUE(rsp.getWriteUnits());
                EXPECT_EQ(1, *rsp.getWriteUnits());
            } while (false);
            break;

        case ClientOpcode::GetCmdTimer:
            rsp = conn.execute(
                    BinprotGetCmdTimerCommand{"metering", ClientOpcode::Noop});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;

        case ClientOpcode::GetErrorMap:
            rsp = conn.execute(BinprotGetErrorMapCommand{});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;

            // MetaWrite ops require meta write privilege... probably not
        // something we'll need initially...
        case ClientOpcode::SetWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::ReturnMeta:
            break;

            // @todo create a test case for range scans
        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::RangeScanContinue:
        case ClientOpcode::RangeScanCancel:
            break;

            // We need a special unit test for DCP
        case ClientOpcode::DcpOpen:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::DcpPrepare:
        case ClientOpcode::DcpSeqnoAcknowledged:
        case ClientOpcode::DcpCommit:
        case ClientOpcode::DcpAbort:
        case ClientOpcode::DcpSeqnoAdvanced:
        case ClientOpcode::DcpOsoSnapshot:
            break;

            // The following are "internal"/advanced commands not intended
        // for the average users. We may add unit tests at a later time for
        // them
        case ClientOpcode::IoctlGet:
        case ClientOpcode::IoctlSet:
        case ClientOpcode::ConfigValidate:
        case ClientOpcode::ConfigReload:
        case ClientOpcode::AuditPut:
        case ClientOpcode::AuditConfigReload:
        case ClientOpcode::Shutdown:
        case ClientOpcode::SetBucketUnitThrottleLimits:
        case ClientOpcode::SetBucketDataLimitExceeded:
        case ClientOpcode::SetVbucket:
        case ClientOpcode::GetVbucket:
        case ClientOpcode::DelVbucket:
        case ClientOpcode::GetAllVbSeqnos:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::SetParam:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::Ifconfig:
        case ClientOpcode::CreateCheckpoint:
        case ClientOpcode::LastClosedCheckpoint:
        case ClientOpcode::CompactDb:
        case ClientOpcode::SetClusterConfig:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::EvictKey:
        case ClientOpcode::Scrub:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh:
        case ClientOpcode::SetCtrlToken:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::UpdateExternalUserPermissions:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::AuthProvider:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::Invalid:
            break;
        }
    };

    auto connection = cluster->getConnection(0);
    connection->authenticate("@admin", "password");
    connection->selectBucket("metering");
    connection->dropPrivilege(cb::rbac::Privilege::Unmetered);
    connection->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
    connection->setReadTimeout(std::chrono::seconds{3});

    for (int ii = 0; ii < 0x100; ++ii) {
        auto opcode = ClientOpcode(ii);
        if (is_valid_opcode(opcode)) {
            testOpcode(*connection, opcode);
        }
    }

    executeWithExpectedCU(
            [&connection]() {
                DcpDrain instance("127.0.0.1",
                                  std::to_string(connection->getPort()),
                                  "@admin",
                                  "password",
                                  "metering");
                instance.drain();
                EXPECT_NE(0, instance.getNumMutations());
                EXPECT_NE(0, instance.getRu());
            },
            0,
            0);

    /// but when running as another user it should meter
    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");
    DcpDrain instance("127.0.0.1",
                      std::to_string(connection->getPort()),
                      "metering",
                      "metering",
                      "metering");
    instance.drain();
    EXPECT_NE(0, instance.getNumMutations());
    EXPECT_NE(0, instance.getRu());

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");
    EXPECT_EQ(instance.getRu(),
              after["ru"].get<size_t>() - before["ru"].get<size_t>());
    EXPECT_EQ(0, after["wu"].get<size_t>() - before["wu"].get<size_t>());
}

static void writeDocument(MemcachedConnection& conn,
                          std::string id,
                          std::string value,
                          std::string xattr_path = {},
                          std::string xattr_value = {},
                          Vbid vbid = Vbid{0},
                          bool remove = false) {
    if (remove) {
        (void)conn.execute(BinprotRemoveCommand{id});
    }

    if (xattr_path.empty()) {
        Document doc;
        doc.info.id = std::move(id);
        doc.value = std::move(value);
        conn.mutate(doc, vbid, MutationType::Set);
    } else {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(id);
        cmd.setVBucket(vbid);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        SUBDOC_FLAG_XATTR_PATH,
                        xattr_path,
                        xattr_value);
        cmd.addMutation(
                cb::mcbp::ClientOpcode::Set, SUBDOC_FLAG_NONE, "", value);
        cmd.addDocFlag(::mcbp::subdoc::doc_flag::Mkdoc);
        auto rsp = conn.execute(cmd);
        if (!rsp.isSuccess()) {
            throw ConnectionError("Subdoc failed", rsp);
        }
    }
}

TEST_F(MeteringTest, MeterArithmeticMethods) {
    auto& sconfig = cb::serverless::Config::instance();

    auto incrCmd = BinprotIncrDecrCommand{cb::mcbp::ClientOpcode::Increment,
                                          "TestArithmeticMethods",
                                          Vbid{0},
                                          1ULL,
                                          0ULL,
                                          0};
    auto decrCmd = BinprotIncrDecrCommand{cb::mcbp::ClientOpcode::Decrement,
                                          "TestArithmeticMethods",
                                          Vbid{0},
                                          1ULL,
                                          0ULL,
                                          0};

    // Operating on a document which isn't a numeric value should
    // account for X ru's and fail
    std::string key = "TestArithmeticMethods";
    std::string value;
    value.resize(1024 * 1024);
    std::fill(value.begin(), value.end(), 'a');
    value.front() = '"';
    value.back() = '"';
    writeDocument(*conn, key, value);

    auto rsp = conn->execute(incrCmd);
    EXPECT_EQ(cb::mcbp::Status::DeltaBadval, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(sconfig.to_ru(value.size() + key.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits().has_value());

    rsp = conn->execute(decrCmd);
    EXPECT_EQ(cb::mcbp::Status::DeltaBadval, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(sconfig.to_ru(value.size() + key.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits().has_value());

    // When creating a value as part of incr/decr it should cost 1WU and no RU
    conn->remove(key, Vbid{0});
    rsp = conn->execute(incrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits().has_value());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    conn->remove(key, Vbid{0});
    rsp = conn->execute(decrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits().has_value());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    // Operating on a document without XAttrs should account 1WU during
    // create and 1RU + 1WU during update (it is 1 because it only contains
    // the body and we don't support an interger which consumes 4k digits ;)
    writeDocument(*conn, key, "10");
    rsp = conn->execute(incrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    rsp = conn->execute(decrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    // Let's up the game and operate on a document with XAttrs which spans 1RU.
    // We should then consume 2RU (one for the XAttr and one for the actual
    // number). It'll then span into more WUs as they're 1/4 of the size of
    // the RU.
    writeDocument(*conn, key, "10", "xattr", value);
    rsp = conn->execute(incrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(sconfig.to_ru(value.size() + key.size()), *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(sconfig.to_wu(value.size() + key.size()), *rsp.getWriteUnits());

    rsp = conn->execute(decrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(sconfig.to_ru(value.size() + key.size()), *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(sconfig.to_wu(value.size() + key.size()), *rsp.getWriteUnits());

    // So far, so good.. According to the spec Durability is supported and
    // should cost 2 WU.
    // @todo Metering of durable writes not implemented yet
    conn->remove(key, Vbid{0});
    writeDocument(*conn, key, "10");

    DurabilityFrameInfo fi(cb::durability::Level::Majority);

    incrCmd.addFrameInfo(fi);
    rsp = conn->execute(incrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    decrCmd.addFrameInfo(fi);
    rsp = conn->execute(decrCmd);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits().has_value());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_TRUE(rsp.getWriteUnits().has_value());
    EXPECT_EQ(1, *rsp.getWriteUnits());
}

TEST_F(MeteringTest, MeterDocumentDelete) {
    auto& sconfig = cb::serverless::Config::instance();

    const std::string id = "MeterDocumentDelete";
    auto command = BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, id};
    // Delete of a non-existing document should be free
    auto rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Delete of a single document should cost 1WU
    writeDocument(*conn, id, "Hello");
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    // But if it contains XAttrs we need to read the document to prune those
    // and end up with a single write unit
    std::string xattr_value;
    xattr_value.resize(8192);
    std::fill(xattr_value.begin(), xattr_value.end(), 'a');
    xattr_value.front() = '"';
    xattr_value.back() = '"';
    writeDocument(*conn, id, "Hello", "xattr", xattr_value);

    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    // lets just add 100 to the xattr value to account for key; xattr path,
    // and some "overhead".. we're going to round to the nearest 4k anyway.
    EXPECT_EQ(sconfig.to_ru(xattr_value.size() + 100), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(1, *rsp.getWriteUnits());

    // If the object contains system xattrs those will be persisted and
    // increase the WU size.
    writeDocument(*conn, id, "Hello", "_xattr", xattr_value);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    // lets just add 100 to the xattr value to account for key; xattr path,
    // and some "overhead".. we're going to round to the nearest 4k anyway.
    EXPECT_EQ(sconfig.to_ru(xattr_value.size() + 100), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(xattr_value.size() + 100), *rsp.getWriteUnits());

    // @todo add Durability test once we implement metering of that on
    //       the server
}

/// The MeterDocumentGet is used to test Get and GetReplica to ensure
/// that we meter correctly on them.
TEST_F(MeteringTest, MeterDocumentGet) {
    auto& sconfig = cb::serverless::Config::instance();

    auto bucket = cluster->getBucket("metering");
    auto rconn = bucket->getConnection(Vbid(0), vbucket_state_replica, 1);
    rconn->authenticate("@admin", "password");
    rconn->selectBucket("metering");
    rconn->dropPrivilege(cb::rbac::Privilege::Unmetered);
    rconn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);

    // Start off by creating the documents we want to test on. We'll be
    // using different document names as I want to run the same test
    // on the replica (with GetReplica) and by creating them all up front
    // they can replicate in the background while we're testing the other

    const std::string id = "MeterDocumentGet";
    std::string document_value;
    document_value.resize(6144);
    std::fill(document_value.begin(), document_value.end(), 'a');
    std::string xattr_value;
    xattr_value.resize(8192);
    std::fill(xattr_value.begin(), xattr_value.end(), 'a');
    xattr_value.front() = '"';
    xattr_value.back() = '"';

    writeDocument(*conn, id, document_value);
    writeDocument(*conn, id + "-xattr", document_value, "xattr", xattr_value);

    // Get of a non-existing document should not cost anything
    auto rsp = conn->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Get,
                                                   id + "-missing"});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Get of a single document without xattrs costs the size of the document
    rsp = conn->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, id});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // If the document contains XAttrs (system or user) those are accounted
    // for as well.
    rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, id + "-xattr"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(xattr_value.size() + document_value.size() +
                            id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Lets verify on the replicas...
    rsp = rconn->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::GetReplica, id + "-missing"});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    do {
        rsp = rconn->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::GetReplica, id});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    do {
        rsp = rconn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetReplica, id + "-xattr"});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(cb::mcbp::Datatype::Raw, cb::mcbp::Datatype(rsp.getDatatype()));
    EXPECT_EQ(document_value, rsp.getDataString());
    EXPECT_EQ(sconfig.to_ru(xattr_value.size() + document_value.size() +
                            id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

TEST_F(MeteringTest, MeterDocumentLocking) {
    auto& sconfig = cb::serverless::Config::instance();

    const std::string id = "MeterDocumentLocking";
    const auto getl = BinprotGetAndLockCommand{id};
    auto rsp = conn->execute(getl);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    std::string document_value;
    document_value.resize(sconfig.readUnitSize - 5);
    std::fill(document_value.begin(), document_value.end(), 'a');

    writeDocument(*conn, id, document_value);
    rsp = conn->execute(getl);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    auto unl = BinprotUnlockCommand{id, Vbid{0}, rsp.getCas()};
    rsp = conn->execute(unl);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

TEST_F(MeteringTest, MeterDocumentTouch) {
    auto& sconfig = cb::serverless::Config::instance();
    const std::string id = "MeterDocumentTouch";

    // Touch of non-existing document should fail and is free
    auto rsp = conn->execute(BinprotTouchCommand{id, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Gat should fail and free
    rsp = conn->execute(BinprotGetAndTouchCommand{id, Vbid{0}, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    std::string document_value;
    document_value.resize(sconfig.readUnitSize - 5);
    std::fill(document_value.begin(), document_value.end(), 'a');
    writeDocument(*conn, id, document_value);

    // Touch of a document is a full read and write of the document on the
    // server, but no data returned
    rsp = conn->execute(BinprotTouchCommand{id, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
              *rsp.getWriteUnits());
    EXPECT_TRUE(rsp.getDataString().empty());
    rsp = conn->execute(BinprotGetAndTouchCommand{id, Vbid{0}, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
              *rsp.getWriteUnits());
    EXPECT_EQ(document_value, rsp.getDataString());
}

TEST_F(MeteringTest, MeterDocumentSimpleMutations) {
    auto& sconfig = cb::serverless::Config::instance();

    const std::string id = "MeterDocumentSimpleMutations";
    std::string document_value;
    std::string xattr_path = "xattr";
    std::string xattr_value;
    document_value.resize(sconfig.readUnitSize - 10);
    std::fill(document_value.begin(), document_value.end(), 'a');
    xattr_value.resize(sconfig.readUnitSize - 10);
    std::fill(xattr_value.begin(), xattr_value.end(), 'a');
    xattr_value.front() = '"';
    xattr_value.back() = '"';

    BinprotMutationCommand command;
    command.setKey(id);
    command.addValueBuffer(document_value);

    // Set of an nonexistent document shouldn't cost any RUs and the
    // size of the new document's WUs
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

    // Using Set on an existing document is a replace and will be tested
    // later on.

    // Add of an existing document should fail, and cost 1RU to read the
    // metadata
    command.setMutationType(MutationType::Add);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
    // @todo it currently don't cost an RU - fix this
    EXPECT_FALSE(rsp.getReadUnits());
    //    ASSERT_TRUE(rsp.getReadUnits());
    //    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
    //    rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    // Add of a new document should cost the same as a set (no read, just write)
    conn->remove(id, Vbid{0});
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

    // Replace of the document should cost 1 ru (for the metadata read)
    // then X WUs
    command.setMutationType(MutationType::Replace);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    // @todo it currently don't cost the 1 ru for the metadata read!
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

    // But if we try to replace a document containing XATTRs we would
    // need to read the full document in order to replace, and it should
    // cost the size of the full size of the old document and the new one
    // (containing the xattrs)
    writeDocument(*conn, id, document_value, xattr_path, xattr_value);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    // Trying to replace a document with incorrect CAS should cost 1 RU and
    // no WU
    command.setCas(1);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    // @todo it currently fails and return the size of the old document!
    // EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
    command.setCas(0);

    // Trying to replace a nonexisting document should not cost anything
    conn->remove(id, Vbid{0});
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::NotStored, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::NotStored, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    writeDocument(*conn, id, document_value);
    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
              *rsp.getWriteUnits());

    writeDocument(*conn, id, document_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
              *rsp.getWriteUnits());

    // And if we have XATTRs they should be copied as well
    writeDocument(*conn, id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    writeDocument(*conn, id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    // @todo add test cases for durability
}

} // namespace cb::test