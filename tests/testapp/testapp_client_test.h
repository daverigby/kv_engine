/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "testapp.h"
#include <boost/optional/optional_fwd.hpp>
#include <memcached/durability_spec.h>
#include <algorithm>

/**
 * Test fixture for testapp tests; parameterised on the TransportProtocol (IPv4,
 * Ipv6); (Plain, SSL).
 */
class TestappClientTest
    : public TestappTest,
      public ::testing::WithParamInterface<TransportProtocols> {
public:
    bool isTlsEnabled() const override;
};

enum class XattrSupport { Yes, No };
std::ostream& operator<<(std::ostream& os, const XattrSupport& xattrSupport);
std::string to_string(const XattrSupport& xattrSupport);

/**
 * Test fixture for Extended Attribute (XATTR) tests.
 * Parameterized on:
 * - TransportProtocol (IPv4, Ipv6); (Plain, SSL);
 * - XATTR On/Off
 * - Client JSON On/Off.
 * - Client SNAPPY On/Off.
 */
class TestappXattrClientTest : public TestappTest,
                               public ::testing::WithParamInterface<
                                       ::testing::tuple<TransportProtocols,
                                                        XattrSupport,
                                                        ClientJSONSupport,
                                                        ClientSnappySupport>> {
protected:
    TestappXattrClientTest() : xattrOperationStatus(cb::mcbp::Status::Success) {
    }

    void SetUp() override;

    BinprotSubdocResponse getXattr(MemcachedConnection& conn,
                                   const std::string& path,
                                   bool deleted = false);
    void createXattr(MemcachedConnection& conn,
                     const std::string& path,
                     const std::string& value,
                     bool macro = false);

    bool isTlsEnabled() const override;
    ClientJSONSupport hasJSONSupport() const override;
    ClientSnappySupport hasSnappySupport() const override;

    // What response datatype do we expect for documents which are JSON?
    // Will be JSON only if the client successfully negotiated JSON feature.
    cb::mcbp::Datatype expectedJSONDatatype() const;

    /**
     * What response datatype do we expect for documents which are JSON and
     * were stored as Snappy if client supports it?
     * Will be:
     * - JSON only if the client successfully negotiated JSON feature
     *   without Snappy.
     * - JSON+Snappy if client negotiated both JSON and Snappy (and
     *   sent a compressed document).
     */
    cb::mcbp::Datatype expectedJSONSnappyDatatype() const;

    /**
     * Helper function to check datatype is what we expect for this test config;
     * and if datatype says JSON, validate the value /is/ JSON.
     */
    static ::testing::AssertionResult hasCorrectDatatype(
            const Document& doc, cb::mcbp::Datatype expectedType);

    static ::testing::AssertionResult hasCorrectDatatype(
            cb::mcbp::Datatype expectedType,
            cb::mcbp::Datatype actualType,
            std::string_view value);

    /**
     * Replaces document `name` with a document containing the given
     * body and XATTRs.
     *
     * @param xattrList list of XATTR key / value pairs to store.
     * @param compressValue Should the value be compress before being sent?
     */
    void setBodyAndXattr(
            MemcachedConnection& connection,
            const std::string& startValue,
            std::initializer_list<std::pair<std::string, std::string>>
                    xattrList,
            bool compressValue);

    /**
     * Replaces document `name` with a document containing the given
     * body and XATTRs.
     * If Snappy support is available (hasSnappySupport), will store as a
     * compressed document.
     * @param xattrList list of XATTR key / value pairs to store.
     */
    void setBodyAndXattr(
            MemcachedConnection& connection,
            const std::string& value,
            std::initializer_list<std::pair<std::string, std::string>>
                    xattrList);

    void setClusterSessionToken(uint64_t new_value);

    /// Perform the specified subdoc command; returning the response.
    BinprotSubdocResponse subdoc(
            MemcachedConnection& conn,
            cb::mcbp::ClientOpcode opcode,
            const std::string& key,
            const std::string& path,
            const std::string& value = {},
            protocol_binary_subdoc_flag flag = SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag docFlag = mcbp::subdoc::doc_flag::None,
            const std::optional<cb::durability::Requirements>& durReqs = {});

    /// Perform the specified subdoc multi-mutation command; returning the
    /// response.
    BinprotSubdocResponse subdocMultiMutation(
            MemcachedConnection& conn, BinprotSubdocMultiMutationCommand cmd);

    cb::mcbp::Status xattr_upsert(MemcachedConnection& conn,
                                  const std::string& path,
                                  const std::string& value);

protected:
    Document document;
    cb::mcbp::Status xattrOperationStatus;
};

struct PrintToStringCombinedName {
    std::string operator()(const ::testing::TestParamInfo<
                           ::testing::tuple<TransportProtocols,
                                            XattrSupport,
                                            ClientJSONSupport,
                                            ClientSnappySupport>>& info) const;
};
