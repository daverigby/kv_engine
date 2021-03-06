/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "network_interface.h"

#include <logger/logger.h>
#include <platform/dirutils.h>

#include <nlohmann/json.hpp>
#include <utilities/json_utilities.h>

static void handle_interface_port(NetworkInterface& ifc,
                                  nlohmann::json::const_iterator it) {
    ifc.port = in_port_t(cb::jsonGet<size_t>(it));
}

static void handle_interface_host(NetworkInterface& ifc,
                                  nlohmann::json::const_iterator it) {
    ifc.host = cb::jsonGet<std::string>(it);
}

static void handle_interface_tag(NetworkInterface& ifc,
                                 nlohmann::json::const_iterator it) {
    ifc.tag = cb::jsonGet<std::string>(it);
}

/**
 * Set the given NetworkInterface::Protocol based on the value of `obj`,
 * or throw std::invalid_argument if obj is not a valid setting.
 */
static void handle_interface_protocol(NetworkInterface::Protocol& proto,
                                      const char* proto_name,
                                      nlohmann::json::const_iterator it) {
    if (it.value().type() == nlohmann::json::value_t::string) {
        const std::string value(cb::jsonGet<std::string>(it));
        if (value == "required") {
            proto = NetworkInterface::Protocol::Required;
        } else if (value == "optional") {
            proto = NetworkInterface::Protocol::Optional;
        } else if (value == "off") {
            proto = NetworkInterface::Protocol::Off;
        } else {
            throw std::invalid_argument("\"" + std::string(proto_name) +
                                        "\" has an unrecognized string value "
                                        "\"" +
                                        value + R"(")");
        }
        // Backwards compatibility - map True -> Optional, False -> Off
    } else if (it.value().type() == nlohmann::json::value_t::boolean) {
        bool value = cb::jsonGet<bool>(it);
        if (value) {
            proto = NetworkInterface::Protocol::Optional;
        } else {
            proto = NetworkInterface::Protocol::Off;
        }
    } else {
        // Throw a type error instead of invalid argument for consistency
        // with other handlers
        cb::throwJsonTypeError("\"" + std::string(proto_name) +
                               "\" must be a string or boolean value");
    }
}

static void handle_interface_ipv4(NetworkInterface& ifc,
                                  nlohmann::json::const_iterator it) {
    handle_interface_protocol(ifc.ipv4, "ipv4", it);
}

static void handle_interface_ipv6(NetworkInterface& ifc,
                                  nlohmann::json::const_iterator it) {
    handle_interface_protocol(ifc.ipv6, "ipv6", it);
}

static void handle_interface_ssl(NetworkInterface& ifc,
                                 nlohmann::json::const_iterator it) {
    if (it.value().type() != nlohmann::json::value_t::object) {
        throw std::invalid_argument(R"("ssl" must be an object)");
    }
    ifc.ssl.key = cb::jsonGet<std::string>(it.value(), "key");
    ifc.ssl.cert = cb::jsonGet<std::string>(it.value(), "cert");

    if (!cb::io::isFile(ifc.ssl.key)) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                R"("ssl:key":')" + ifc.ssl.key + "'");
    }

    if (!cb::io::isFile(ifc.ssl.cert)) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                R"("ssl:cert":')" + ifc.ssl.cert + "'");
    }
}

static void handle_interface_system(NetworkInterface& ifc,
                                    nlohmann::json::const_iterator it) {
    ifc.system = cb::jsonGet<bool>(it);
}

NetworkInterface::NetworkInterface(const nlohmann::json& json) {
    struct interface_config_tokens {
        /**
         * The key in the configuration
         */
        std::string key;

        /**
         * A callback method used by the interface object when we're parsing
         * the config attributes.
         *
         * @param ifc the interface object to update
         * @param obj the current object in the configuration we're looking at
         * @throws std::invalid_argument if it something is wrong with the
         *         entry
         */
        void (*handler)(NetworkInterface& ifc,
                        nlohmann::json::const_iterator it);
    };

    std::vector<interface_config_tokens> handlers = {
            {"tag", handle_interface_tag},
            {"port", handle_interface_port},
            {"host", handle_interface_host},
            {"ipv4", handle_interface_ipv4},
            {"ipv6", handle_interface_ipv6},
            {"ssl", handle_interface_ssl},
            {"system", handle_interface_system},
    };

    for (auto it = json.begin(); it != json.end(); ++it) {
        std::string key = it.key();
        bool found = false;
        for (auto& handler : handlers) {
            if (handler.key == key) {
                handler.handler(*this, it);
                found = true;
                break;
            }
        }

        if (!found) {
            LOG_INFO(R"(Unknown token "{}" in config ignored.)", key);
        }
    }
}

std::string to_string(const NetworkInterface::Protocol& proto) {
    switch (proto) {
    case NetworkInterface::Protocol::Off:
        return "off";
    case NetworkInterface::Protocol::Optional:
        return "optional";
    case NetworkInterface::Protocol::Required:
        return "required";
    }
    return "<invalid>";
}
