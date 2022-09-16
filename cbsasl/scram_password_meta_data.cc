/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/scram_password_meta_data.h>
#include <platform/base64.h>

namespace cb::sasl::pwdb {

ScramPasswordMetaData::ScramPasswordMetaData(const nlohmann::json& obj) {
    if (!obj.is_object()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): must be an object");
    }

    std::string stored_key;
    std::string server_key;

    for (const auto& [label, value] : obj.items()) {
        if (label == "hashes") {
            for (const auto& it : value) {
                if (!it.is_object()) {
                    throw std::invalid_argument(
                            "ScramPasswordMetaData(): hashes entry must be "
                            "object");
                }
                keys.emplace_back(Keys{
                        cb::base64::decode(it["stored_key"].get<std::string>()),
                        cb::base64::decode(
                                it["server_key"].get<std::string>())});
            }
        } else if (label == "stored_key") {
            stored_key = cb::base64::decode(value.get<std::string>());
        } else if (label == "server_key") {
            server_key = cb::base64::decode(value.get<std::string>());
        } else if (label == "iterations") {
            iteration_count = value.get<std::size_t>();
        } else if (label == "salt") {
            salt = value.get<std::string>();
            // verify that it is a legal base64 encoding
            cb::base64::decode(value.get<std::string>());
        } else {
            throw std::invalid_argument(
                    "ScramPasswordMetaData(): Invalid attribute: \"" + label +
                    "\"");
        }
    }

    if (!stored_key.empty() && !server_key.empty()) {
        keys.emplace_back(Keys{std::move(stored_key), std::move(server_key)});
    }

    if (keys.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): stored-key and server-key must be "
                "present");
    }

    if (salt.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): salt must be present");
    }
    if (iteration_count == 0) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): iteration_count must be present");
    }
}

nlohmann::json ScramPasswordMetaData::to_json() const {
    auto ret = nlohmann::json{{"iterations", iteration_count}, {"salt", salt}};
    ret["hashes"] = nlohmann::json::array();
    for (const auto& key : keys) {
        ret["hashes"].push_back(nlohmann::json{
                {"server_key", cb::base64::encode(key.server_key)},
                {"stored_key", cb::base64::encode(key.stored_key)}});
    }
    return ret;
}

} // namespace cb::sasl::pwdb
