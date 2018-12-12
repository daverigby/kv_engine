/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "connection.h"

#include "buckets.h"
#include "connections.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "runtime.h"
#include "server_event.h"
#include "settings.h"
#include "ssl_utils.h"
#include "tracing.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cbassert.h>
#include <platform/checked_snprintf.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <utilities/logtags.h>
#include <gsl/gsl>

#include <cctype>
#include <exception>
#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

/// The TLS packet is using the following format:
/// Byte 0 - Content type
/// Byte 1 and 2 - Version
/// Byte 3 and 4 - length
///   n bytes of user payload
///   m bytes of MAC
///   o bytes of padding for block ciphers
/// Create a constant which represents the maxium amount of data we
/// may put in a single TLS frame
static constexpr std::size_t TlsFrameSize = 16 * 1024;

std::string to_string(Connection::Priority priority) {
    switch (priority) {
    case Connection::Priority::High:
        return "High";
    case Connection::Priority::Medium:
        return "Medium";
    case Connection::Priority::Low:
        return "Low";
    }
    throw std::invalid_argument("No such priority: " +
                                std::to_string(int(priority)));
}

bool Connection::setTcpNoDelay(bool enable) {
    if (socketDescriptor == INVALID_SOCKET) {
        // Our unit test run without a connected socket (and there is
        // no point of running setsockopt on an invalid socket and
        // get the error message from there).. But we don't want them
        // (the unit tests) to flood the console with error messages
        // that setsockopt failed
        return false;
    }

    const int flags = enable ? 1 : 0;
    int error = cb::net::setsockopt(socketDescriptor,
                                    IPPROTO_TCP,
                                    TCP_NODELAY,
                                    reinterpret_cast<const void*>(&flags),
                                    sizeof(flags));

    if (error != 0) {
        std::string errmsg = cb_strerror(cb::net::get_socket_error());
        LOG_WARNING("setsockopt(TCP_NODELAY): {}", errmsg);
        nodelay = false;
        return false;
    } else {
        nodelay = enable;
    }

    return true;
}

nlohmann::json Connection::toJSON() const {
    nlohmann::json ret;

    ret["connection"] = cb::to_hex(uint64_t(this));

    if (socketDescriptor == INVALID_SOCKET) {
        ret["socket"] = "disconnected";
        return ret;
    }

    ret["socket"] = socketDescriptor;
    ret["yields"] = yields.load();
    ret["protocol"] = "memcached";
    ret["peername"] = getPeername().c_str();
    ret["sockname"] = getSockname().c_str();
    ret["parent_port"] = parent_port;
    ret["bucket_index"] = getBucketIndex();
    ret["internal"] = isInternal();

    if (authenticated) {
        if (internal) {
            // We want to be able to map these connections, and given
            // that it is internal we don't reveal any user data
            ret["username"] = username;
        } else {
            ret["username"] = cb::tagUserData(username);
        }
    }

    ret["refcount"] = refcount;

    nlohmann::json features = nlohmann::json::array();
    if (isSupportsMutationExtras()) {
        features.push_back("mutation extras");
    }
    if (isXerrorSupport()) {
        features.push_back("xerror");
    }
    if (nodelay) {
        features.push_back("tcp nodelay");
    }
    if (allowUnorderedExecution()) {
        features.push_back("unordered execution");
    }
    if (tracingEnabled) {
        features.push_back("tracing");
    }

    if (isCollectionsSupported()) {
        features.push_back("collections");
    }

    if (isDuplexSupported()) {
        features.push_back("duplex");
    }

    if (isClustermapChangeNotificationSupported()) {
        features.push_back("CCN");
    }

    ret["features"] = features;

    ret["thread"] = getThread().index;
    ret["priority"] = to_string(priority);

    if (clustermap_revno == -2) {
        ret["clustermap_revno"] = "unknown";
    } else {
        ret["clustermap_revno"] = clustermap_revno;
    }

    ret["total_cpu_time"] = std::to_string(total_cpu_time.count());
    ret["min_sched_time"] = std::to_string(min_sched_time.count());
    ret["max_sched_time"] = std::to_string(max_sched_time.count());

    nlohmann::json arr = nlohmann::json::array();
    for (const auto& c : cookies) {
        arr.push_back(c->toJSON());
    }
    ret["cookies"] = arr;

    if (agentName.front() != '\0') {
        ret["agent_name"] = std::string(agentName.data());
    }
    if (connectionId.front() != '\0') {
        ret["connection_id"] = std::string(connectionId.data());
    }

    ret["sasl_enabled"] = saslAuthEnabled;
    ret["dcp"] = isDCP();
    ret["dcp_xattr_aware"] = isDcpXattrAware();
    ret["dcp_no_value"] = isDcpNoValue();
    ret["max_reqs_per_event"] = max_reqs_per_event;
    ret["nevents"] = numEvents;
    ret["state"] = getStateName();

    if (read) {
        ret["read"] = read->to_json();
    }

    if (write) {
        ret["write"] = write->to_json();
    }

    ret["write_and_go"] = std::string(stateMachine.getStateName(write_and_go));

    nlohmann::json iovobj;
    iovobj["size"] = iov.size();
    iovobj["used"] = iovused;
    ret["iov"] = iovobj;

    nlohmann::json msg;
    msg["used"] = msglist.size();
    msg["curr"] = msgcurr;
    msg["bytes"] = msgbytes;
    ret["msglist"] = msg;

    nlohmann::json ilist;
    ilist["size"] = reservedItems.size();
    ret["itemlist"] = ilist;

    nlohmann::json talloc;
    talloc["size"] = temp_alloc.size();
    ret["temp_alloc_list"] = talloc;

    ret["ssl"] = client_ctx != nullptr;
    ret["total_recv"] = totalRecv;
    ret["total_send"] = totalSend;

    ret["datatype"] = mcbp::datatype::to_string(datatype.getRaw()).c_str();

    return ret;
}

void Connection::setDCP(bool dcp) {
    Connection::dcp = dcp;

    if (isSslEnabled()) {
        try {
            // Make sure that we have space for up to a single TLS frame
            // in our send buffer (so that we can stick all of the mutations
            // in that buffer)
            write->ensureCapacity(TlsFrameSize);
        } catch (const std::bad_alloc&) {
        }
    }
}

void Connection::restartAuthentication() {
    if (authenticated && domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(username);
    }
    sasl_conn.reset();
    setInternal(false);
    authenticated = false;
    username = "";
}

cb::engine_errc Connection::dropPrivilege(cb::rbac::Privilege privilege) {
    if (privilegeContext.dropPrivilege(privilege)) {
        return cb::engine_errc::success;
    }

    return cb::engine_errc::no_access;
}

cb::rbac::PrivilegeAccess Connection::checkPrivilege(
        cb::rbac::Privilege privilege, Cookie& cookie) {
    cb::rbac::PrivilegeAccess ret;
    unsigned int retries = 0;
    const unsigned int max_retries = 100;

    while ((ret = privilegeContext.check(privilege)) ==
                   cb::rbac::PrivilegeAccess::Stale &&
           retries < max_retries) {
        ++retries;
        const auto opcode = cookie.getRequest(Cookie::PacketContent::Header)
                                    .getClientOpcode();
        const std::string command(to_string(opcode));

        // The privilege context we had could have been a dummy entry
        // (created when the client connected, and used until the
        // connection authenticates). Let's try to automatically update it,
        // but let the client deal with whatever happens after
        // a single update.
        try {
            privilegeContext = cb::rbac::createContext(
                    getUsername(), getDomain(), all_buckets[bucketIndex].name);
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext =
                    cb::rbac::createContext(getUsername(), getDomain(), "");
            LOG_INFO(
                    "{}: RBAC: Connection::checkPrivilege({}) {} No access "
                    "to "
                    "bucket [{}]. command: [{}] new privilege set: {}",
                    getId(),
                    to_string(privilege),
                    getDescription(),
                    all_buckets[bucketIndex].name,
                    command,
                    privilegeContext.to_string());
        } catch (const cb::rbac::Exception& error) {
            LOG_WARNING(
                    "{}: RBAC: Connection::checkPrivilege({}) {}: An "
                    "exception occurred. command: [{}] bucket: [{}] UUID:"
                    "[{}] message: {}",
                    getId(),
                    to_string(privilege),
                    getDescription(),
                    command,
                    all_buckets[bucketIndex].name,
                    cookie.getEventId(),
                    error.what());
            // Add a textual error as well
            cookie.setErrorContext("An exception occurred. command: [" +
                                   command + "]");
            return cb::rbac::PrivilegeAccess::Fail;
        }
    }

    if (retries == max_retries) {
        LOG_INFO(
                "{}: RBAC: Gave up rebuilding privilege context after {} "
                "times. Let the client handle the stale authentication "
                "context",
                getId(),
                retries);

    } else if (retries > 1) {
        LOG_INFO("{}: RBAC: Had to rebuild privilege context {} times",
                 getId(),
                 retries);
    }

    if (ret == cb::rbac::PrivilegeAccess::Fail) {
        const auto opcode = cookie.getRequest(Cookie::PacketContent::Header)
                                    .getClientOpcode();
        const std::string command(to_string(opcode));
        const std::string privilege_string = cb::rbac::to_string(privilege);
        const std::string context = privilegeContext.to_string();

        if (Settings::instance().isPrivilegeDebug()) {
            audit_privilege_debug(*this,
                                  command,
                                  all_buckets[bucketIndex].name,
                                  privilege_string,
                                  context);

            LOG_INFO(
                    "{}: RBAC privilege debug:{} command:[{}] bucket:[{}] "
                    "privilege:[{}] context:{}",
                    getId(),
                    getDescription(),
                    command,
                    all_buckets[bucketIndex].name,
                    privilege_string,
                    context);

            return cb::rbac::PrivilegeAccess::Ok;
        } else {
            LOG_INFO(
                    "{} RBAC {} missing privilege {} for {} in bucket:[{}] "
                    "with context: "
                    "{} UUID:[{}]",
                    getId(),
                    getDescription(),
                    privilege_string,
                    command,
                    all_buckets[bucketIndex].name,
                    context,
                    cookie.getEventId());
            // Add a textual error as well
            cookie.setErrorContext("Authorization failure: can't execute " +
                                   command + " operation without the " +
                                   privilege_string + " privilege");
        }
    }

    return ret;
}

Bucket& Connection::getBucket() const {
    return all_buckets[getBucketIndex()];
}

EngineIface* Connection::getBucketEngine() const {
    return getBucket().getEngine();
}

ENGINE_ERROR_CODE Connection::remapErrorCode(ENGINE_ERROR_CODE code) const {
    if (xerror_support) {
        return code;
    }

    // Check our whitelist
    switch (code) {
    case ENGINE_SUCCESS: // FALLTHROUGH
    case ENGINE_KEY_ENOENT: // FALLTHROUGH
    case ENGINE_KEY_EEXISTS: // FALLTHROUGH
    case ENGINE_ENOMEM: // FALLTHROUGH
    case ENGINE_NOT_STORED: // FALLTHROUGH
    case ENGINE_EINVAL: // FALLTHROUGH
    case ENGINE_ENOTSUP: // FALLTHROUGH
    case ENGINE_EWOULDBLOCK: // FALLTHROUGH
    case ENGINE_E2BIG: // FALLTHROUGH
    case ENGINE_DISCONNECT: // FALLTHROUGH
    case ENGINE_NOT_MY_VBUCKET: // FALLTHROUGH
    case ENGINE_TMPFAIL: // FALLTHROUGH
    case ENGINE_ERANGE: // FALLTHROUGH
    case ENGINE_ROLLBACK: // FALLTHROUGH
    case ENGINE_EBUSY: // FALLTHROUGH
    case ENGINE_DELTA_BADVAL: // FALLTHROUGH
    case ENGINE_PREDICATE_FAILED:
    case ENGINE_FAILED:
        return code;

    case ENGINE_LOCKED:
        return ENGINE_KEY_EEXISTS;
    case ENGINE_LOCKED_TMPFAIL:
        return ENGINE_TMPFAIL;
    case ENGINE_UNKNOWN_COLLECTION:
    case ENGINE_COLLECTIONS_MANIFEST_IS_AHEAD:
        return isCollectionsSupported() ? code : ENGINE_EINVAL;

    case ENGINE_EACCESS:break;
    case ENGINE_NO_BUCKET:break;
    case ENGINE_AUTH_STALE:break;
    case ENGINE_DURABILITY_INVALID_LEVEL:
    case ENGINE_DURABILITY_IMPOSSIBLE:
    case ENGINE_SYNC_WRITE_PENDING:
        break;
    case ENGINE_SYNC_WRITE_IN_PROGRESS:
    case ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS:
        // we can return tmpfail to old clients and have them retry the
        // operation
        return ENGINE_TMPFAIL;
    case ENGINE_SYNC_WRITE_AMBIGUOUS:
    case ENGINE_DCP_STREAMID_INVALID:
        break;
    }

    // Seems like the rest of the components in our system isn't
    // prepared to receive access denied or authentincation stale.
    // For now we should just disconnect them
    auto errc = cb::make_error_condition(cb::engine_errc(code));
    LOG_WARNING(
            "{} - Client {} not aware of extended error code ({}). "
            "Disconnecting",
            getId(),
            getDescription().c_str(),
            errc.message().c_str());

    return ENGINE_DISCONNECT;
}

void Connection::resetUsernameCache() {
    if (sasl_conn.isInitialized()) {
        username = sasl_conn.getUsername();
        domain = sasl_conn.getDomain();
    } else {
        username = "unknown";
        domain = cb::sasl::Domain::Local;
    }

    updateDescription();
}

void Connection::updateDescription() {
    description.assign("[ " + getPeername() + " - " + getSockname());
    if (authenticated) {
        description += " (";
        if (isInternal()) {
            description += "System, ";
        }
        description += cb::tagUserData(getUsername());

        if (domain == cb::sasl::Domain::External) {
            description += " (LDAP)";
        }
        description += ")";
    } else {
        description += " (not authenticated)";
    }
    description += " ]";
}

void Connection::setBucketIndex(int bucketIndex) {
    Connection::bucketIndex.store(bucketIndex, std::memory_order_relaxed);

    // Update the privilege context. If a problem occurs within the RBAC
    // module we'll assign an empty privilege context to the connection.
    try {
        if (authenticated) {
            // The user have logged in, so we should create a context
            // representing the users context in the desired bucket.
            privilegeContext = cb::rbac::createContext(
                    username, getDomain(), all_buckets[bucketIndex].name);
        } else if (is_default_bucket_enabled() &&
                   strcmp("default", all_buckets[bucketIndex].name) == 0) {
            // We've just connected to the _default_ bucket, _AND_ the client
            // is unknown.
            // Personally I think the "default bucket" concept is a really
            // really bad idea, but we need to be backwards compatible for
            // a while... lets look up a profile named "default" and
            // assign that. It should only contain access to the default
            // bucket.
            privilegeContext = cb::rbac::createContext(
                    "default", getDomain(), all_buckets[bucketIndex].name);
        } else {
            // The user has not authenticated, and this isn't for the
            // "default bucket". Assign an empty profile which won't give
            // you any privileges.
            privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
    }

    if (bucketIndex == 0) {
        // If we're connected to the no bucket we should return
        // no bucket instead of EACCESS. Lets give the connection all
        // possible bucket privileges
        privilegeContext.setBucketPrivileges();
    }
}

void Connection::addCpuTime(std::chrono::nanoseconds ns) {
    total_cpu_time += ns;
    min_sched_time = std::min(min_sched_time, ns);
    max_sched_time = std::max(min_sched_time, ns);
}

void Connection::enqueueServerEvent(std::unique_ptr<ServerEvent> event) {
    server_events.push(std::move(event));
}

void Connection::read_callback(bufferevent*, void* ctx) {
    auto* instance = reinterpret_cast<Connection*>(ctx);
    auto& thread = instance->getThread();

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::read_callback::threadLock",
                          SlowMutexThreshold);

    // Remove the connection from the list of pending io's (in case the
    // object was scheduled to run in the dispatcher before the
    // callback for the worker thread is executed.
    //
    {
        std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
        auto iter = thread.pending_io.map.find(instance);
        if (iter != thread.pending_io.map.end()) {
            for (const auto& pair : iter->second) {
                if (pair.first) {
                    pair.first->setAiostat(pair.second);
                    pair.first->setEwouldblock(false);
                }
            }
            thread.pending_io.map.erase(iter);
        }
    }

    // Remove the connection from the notification list if it's there
    thread.notification.remove(instance);

    run_event_loop(instance, EV_READ);
}

void Connection::write_callback(bufferevent*, void* ctx) {
    auto* instance = reinterpret_cast<Connection*>(ctx);
    auto& thread = instance->getThread();
    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::write_callback::threadLock",
                          SlowMutexThreshold);
    // Remove the connection from the list of pending io's (in case the
    // object was scheduled to run in the dispatcher before the
    // callback for the worker thread is executed.
    {
        std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
        auto iter = thread.pending_io.map.find(instance);
        if (iter != thread.pending_io.map.end()) {
            for (const auto& pair : iter->second) {
                if (pair.first) {
                    pair.first->setAiostat(pair.second);
                    pair.first->setEwouldblock(false);
                }
            }
            thread.pending_io.map.erase(iter);
        }
    }

    // Remove the connection from the notification list if it's there
    thread.notification.remove(instance);
    run_event_loop(instance, EV_WRITE);
}

void Connection::event_callback(bufferevent*, short event, void* ctx) {
    auto* instance = reinterpret_cast<Connection*>(ctx);
    bool term = false;

    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        LOG_DEBUG("{}: McbpConnection::on_event: Socket EOF: {}",
                  instance->getId(),
                  evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        term = true;
    }

    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        LOG_INFO("{}: McbpConnection::on_event: Socket error: {}",
                 instance->getId(),
                 evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        term = true;
    }

    if (term) {
        auto& thread = instance->getThread();
        TRACE_LOCKGUARD_TIMED(thread.mutex,
                              "mutex",
                              "Connection::event_callback::threadLock",
                              SlowMutexThreshold);

        // Remove the connection from the list of pending io's (in case the
        // object was scheduled to run in the dispatcher before the
        // callback for the worker thread is executed.
        //
        {
            std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
            auto iter = thread.pending_io.map.find(instance);
            if (iter != thread.pending_io.map.end()) {
                for (const auto& pair : iter->second) {
                    if (pair.first) {
                        pair.first->setAiostat(pair.second);
                        pair.first->setEwouldblock(false);
                    }
                }
                thread.pending_io.map.erase(iter);
            }
        }

        // Remove the connection from the notification list if it's there
        thread.notification.remove(instance);

        if (instance->getState() != StateMachine::State::pending_close) {
            instance->setState(StateMachine::State::closing);
        }
        run_event_loop(instance, EV_WRITE | EV_READ);
    }
}

void Connection::shrinkBuffers() {
    // We share the buffers with the thread, so we don't need to worry
    // about the read and write buffer.

    if (msglist.size() > MSG_LIST_HIGHWAT) {
        try {
            msglist.resize(MSG_LIST_INITIAL);
            msglist.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING("{}: Failed to shrink msglist down to {} elements.",
                        getId(),
                        MSG_LIST_INITIAL);
        }
    }

    if (iov.size() > IOV_LIST_HIGHWAT) {
        try {
            iov.resize(IOV_LIST_INITIAL);
            iov.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING("{}: Failed to shrink iov down to {} elements.",
                        getId(),
                        IOV_LIST_INITIAL);
        }
    }
}

void Connection::setAuthenticated(bool authenticated) {
    Connection::authenticated = authenticated;
    if (authenticated) {
        updateDescription();
        privilegeContext = cb::rbac::createContext(username, getDomain(), "");
    } else {
        resetUsernameCache();
        privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
    }
}

bool Connection::tryAuthFromSslCert(const std::string& userName) {
    username.assign(userName);
    domain = cb::sasl::Domain::Local;

    try {
        auto context =
                cb::rbac::createInitialContext(getUsername(), getDomain());
        setAuthenticated(true);
        setInternal(context.second);
        audit_auth_success(*this);
        LOG_INFO(
                "{}: Client {} authenticated as '{}' via X509 "
                "certificate",
                getId(),
                getPeername(),
                cb::UserDataView(getUsername()));
        // Connections authenticated by using X.509 certificates should not
        // be able to use SASL to change it's identity.
        saslAuthEnabled = false;
    } catch (const cb::rbac::NoSuchUserException& e) {
        setAuthenticated(false);
        LOG_WARNING("{}: User [{}] is not defined as a user in Couchbase",
                    getId(),
                    cb::UserDataView(e.what()));
        return false;
    }
    return true;
}

ssize_t Connection::sendmsg(struct msghdr* m) {
    // In the bufferevent prototype we copy everything we wanted to send off
    // the IO vector, and put it into the bufferevents structures. This is
    // far from optimal, as it copies all the data into yet another buffer.
    //
    // @todo
    // In the following patches we'll move away from this, but that would
    // most likely result in state machinery changes (and grow the patch)
    // so it is kept like this to minimize the patch moving over to
    // bufferevents
    //
    ssize_t res = 0;
    for (size_t ii = 0; ii < size_t(m->msg_iovlen); ++ii) {
        int nw = bufferevent_write(
                bev.get(), m->msg_iov[ii].iov_base, m->msg_iov[ii].iov_len);
        if (nw == -1) {
            break;
        }

        res += ssize_t(m->msg_iov[ii].iov_len);
        totalSend += m->msg_iov[ii].iov_len;
    }

    return res > 0 ? res : -1;
}

/**
 * Adjust the msghdr by "removing" n bytes of data from it.
 *
 * @param m the msgheader to update
 * @param nbytes
 * @return the number of bytes left in the current iov entry
 */
size_t adjust_msghdr(cb::Pipe& pipe, struct msghdr* m, ssize_t nbytes) {
    auto rbuf = pipe.rdata();

    // We've written some of the data. Remove the completed
    // iovec entries from the list of pending writes.
    while (m->msg_iovlen > 0 && nbytes >= ssize_t(m->msg_iov->iov_len)) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(m->msg_iov->iov_len);
            rbuf = pipe.rdata();
        }
        nbytes -= (ssize_t)m->msg_iov->iov_len;
        m->msg_iovlen--;
        m->msg_iov++;
    }

    // Might have written just part of the last iovec entry;
    // adjust it so the next write will do the rest.
    if (nbytes > 0) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(nbytes);
        }
        m->msg_iov->iov_base =
                (void*)((unsigned char*)m->msg_iov->iov_base + nbytes);
        m->msg_iov->iov_len -= nbytes;
    }

    return m->msg_iov->iov_len;
}

Connection::TransmitResult Connection::transmit() {
    while (msgcurr < msglist.size() && msglist[msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        msgcurr++;
    }

    if (msgcurr < msglist.size()) {
        struct msghdr* m = &msglist[msgcurr];
        auto res = sendmsg(m);
        if (res > 0) {
            get_thread_stats(this)->bytes_written += res;

            if (adjust_msghdr(*write, m, res) == 0) {
                msgcurr++;
                if (msgcurr == msglist.size()) {
                    return TransmitResult::Complete;
                }
            }

            return TransmitResult::Incomplete;
        }
        return TransmitResult::SoftError;
    }

    return TransmitResult::Complete;
}

/**
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start
 * looking at the data I've got after a number of reallocs...
 */
Connection::TryReadResult Connection::tryReadNetwork() {
    // Drain the buffer used by libevent by moving the data into our
    // internal read buffer. This cause an extra memory copy, and will be
    // refactored in the future.
    // @todo this code will be fixed as part of getting rid of of the read
    // buffer
    auto nb = evbuffer_get_length(bufferevent_get_input(bev.get()));
    if (nb == 0) {
        return TryReadResult::NoDataReceived;
    }

    try {
        read->ensureCapacity(nb);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(
                "{}: Failed to allocate memory for package header. Closing "
                "connection {}",
                getId(),
                getDescription());
        return TryReadResult::Error;
    }

    auto* event = bev.get();
    auto nr = read->produce([event](cb::byte_buffer buffer) -> ssize_t {
        return ssize_t(bufferevent_read(event, buffer.data(), buffer.size()));
    });

    if (nr < 0) {
        return TryReadResult::Error;
    }

    get_thread_stats(this)->bytes_read += nr;
    return TryReadResult::DataReceived;
}

bool Connection::useCookieSendResponse(std::size_t size) const {
    // The limit for when to copy the data into a separate response
    // buffer instead of using the IO vector to send the data. The limit
    // is currently hardcoded, but it should probably be possible to tune
    // the value. (when the cost of copying exceeds the cost of creating
    // two (or 3) extra TLS frames. Start by keep it fixed to see if it
    // makes any difference in showfast.
    static constexpr std::size_t SslCopyLimit = 4096;
    return isSslEnabled() && size < SslCopyLimit;
}

bool Connection::dcpUseWriteBuffer(size_t size) const {
    return isSslEnabled() && size < write->wsize();
}

void Connection::addMsgHdr(bool reset) {
    if (reset) {
        msgcurr = 0;
        msglist.clear();
        iovused = 0;
    }

    msglist.emplace_back();

    struct msghdr& msg = msglist.back();

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(&msg, 0, sizeof(struct msghdr));

    msg.msg_iov = &iov.data()[iovused];

    msgbytes = 0;
    STATS_MAX(this, msgused_high_watermark, gsl::narrow<int>(msglist.size()));
}

void Connection::addIov(const void* buf, size_t len) {
    if (len == 0) {
        return;
    }

    struct msghdr* m = &msglist.back();

    /* We may need to start a new msghdr if this one is full. */
    if (m->msg_iovlen == IOV_MAX) {
        addMsgHdr(false);
    }

    ensureIovSpace();

    // Update 'm' as we may have added an additional msghdr
    m = &msglist.back();
    // If this entry is right after the previous one we can just
    // extend the previous entry instead of adding a new one
    bool addNewEntry = true;
    if (m->msg_iovlen > 0) {
        auto& prev = m->msg_iov[m->msg_iovlen - 1];
        const auto* p = static_cast<const char*>(prev.iov_base) + prev.iov_len;
        if (buf == p) {
            prev.iov_len += len;
            addNewEntry = false;
        }
    }

    if (addNewEntry) {
        m->msg_iov[m->msg_iovlen].iov_base = (void*)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;
        ++iovused;
        STATS_MAX(this, iovused_high_watermark, gsl::narrow<int>(getIovUsed()));
        m->msg_iovlen++;
    }

    msgbytes += len;
}

void Connection::releaseReservedItems() {
    auto* bucketEngine = getBucket().getEngine();
    for (auto* it : reservedItems) {
        bucketEngine->release(it);
    }
    reservedItems.clear();
}

void Connection::ensureIovSpace() {
    if (iovused < iov.size()) {
        // There is still size in the list
        return;
    }

    // Try to double the size of the array
    iov.resize(iov.size() * 2);

    /* Point all the msghdr structures at the new list. */
    size_t ii;
    int iovnum;
    for (ii = 0, iovnum = 0; ii < msglist.size(); ii++) {
        msglist[ii].msg_iov = &iov[iovnum];
        iovnum += msglist[ii].msg_iovlen;
    }
}

Connection::Connection(FrontEndThread& thr)
    : socketDescriptor(INVALID_SOCKET),
      connectedToSystemPort(false),
      base(nullptr),
      thread(thr),
      peername("unknown"),
      sockname("unknown"),
      stateMachine(*this),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)) {
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    setConnectionId(peername.c_str());
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);
}

Connection::Connection(SOCKET sfd,
                       event_base* b,
                       const ListeningPort& ifc,
                       FrontEndThread& thr)
    : socketDescriptor(sfd),
      connectedToSystemPort(ifc.system),
      base(b),
      thread(thr),
      parent_port(ifc.port),
      peername(cb::net::getpeername(socketDescriptor)),
      sockname(cb::net::getsockname(socketDescriptor)),
      stateMachine(*this),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)) {
    setTcpNoDelay(true);
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);
    setConnectionId(peername.c_str());

    if (ifc.isSslPort()) {
        // Trond Norbye - 20171003
        //
        // @todo figure out if the SSL_CTX needs to have the same lifetime
        //       as the created ssl object. It could be that we could keep
        //       the SSL_CTX as part of the runtime and then reuse it
        //       across all of the SSL connections when we initialize them.
        //       If we do that we don't have to reload the SSL certificates
        //       and parse the PEM format every time we accept a client!
        //       (which we shouldn't be doing!!!!)
        server_ctx = SSL_CTX_new(SSLv23_server_method());
        SSL_CTX_set_options(server_ctx, Settings::instance().getSslProtocolMask());
        SSL_CTX_set_mode(server_ctx,
                         SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                         SSL_MODE_ENABLE_PARTIAL_WRITE);

        if (!SSL_CTX_use_certificate_chain_file(server_ctx,
                                                ifc.sslCert.c_str()) ||
            !SSL_CTX_use_PrivateKey_file(
                    server_ctx, ifc.sslKey.c_str(), SSL_FILETYPE_PEM)) {
            throw std::runtime_error("Failed to enable ssl!");
        }
        set_ssl_ctx_ciphers(server_ctx,
                            Settings::instance().getSslCipherList(),
                            Settings::instance().getSslCipherSuites());
        int ssl_flags = 0;
        switch (Settings::instance().getClientCertMode()) {
        case cb::x509::Mode::Mandatory:
            ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
            // FALLTHROUGH
        case cb::x509::Mode::Enabled: {
            ssl_flags |= SSL_VERIFY_PEER;
            auto* certNames = SSL_load_client_CA_file(ifc.sslCert.c_str());
            if (certNames == nullptr) {
                LOG_WARNING(nullptr,
                            "Failed to read SSL cert {}",
                            ifc.sslCert.c_str());
                throw std::runtime_error("Failed to read ssl cert!");
            }
            SSL_CTX_set_client_CA_list(server_ctx, certNames);
            SSL_CTX_load_verify_locations(
                    server_ctx, ifc.sslCert.c_str(), nullptr);
            SSL_CTX_set_verify(server_ctx, ssl_flags, nullptr);
            break;
        }
        case cb::x509::Mode::Disabled:
            break;
        }

        client_ctx = SSL_new(server_ctx);
        bev.reset(bufferevent_openssl_socket_new(
                base, sfd, client_ctx, BUFFEREVENT_SSL_ACCEPTING, 0));
        // Given that we want to be able to inspect the client certificate
        // as part of the connection establishment, we start off in another
        // state (we might want to kill the connection if the clients isn't
        // accepted).
        setState(StateMachine::State::ssl_init);
    } else {
        bev.reset(bufferevent_socket_new(base, sfd, 0));
    }

    bufferevent_setcb(bev.get(),
                      Connection::read_callback,
                      Connection::write_callback,
                      Connection::event_callback,
                      static_cast<void*>(this));

    bufferevent_enable(bev.get(), EV_READ);
}

Connection::~Connection() {
    if (connectedToSystemPort) {
        --stats.system_conns;
    }
    if (authenticated && domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(username);
    }

    releaseReservedItems();
    for (auto* ptr : temp_alloc) {
        cb_free(ptr);
    }

    if (client_ctx) {
        SSL_free(client_ctx);
    }

    if (server_ctx) {
        SSL_CTX_free(server_ctx);
    }

    if (socketDescriptor != INVALID_SOCKET) {
        LOG_DEBUG("{} - Closing socket descriptor", getId());
        safe_close(socketDescriptor);
    }
}

void Connection::EventDeleter::operator()(bufferevent* ev) {
    if (ev != nullptr) {
        bufferevent_free(ev);
    }
}

void Connection::setState(StateMachine::State next_state) {
    stateMachine.setCurrentState(next_state);
}

void Connection::runStateMachinery() {
    // Check for stuck clients
    auto currentSendBufferSize = getSendQueueSize();
    // is the send buffer stuck?
    if (currentSendBufferSize == 0) {
        sendQueueInfo.size = currentSendBufferSize;
    } else {
        if (sendQueueInfo.size != currentSendBufferSize) {
            sendQueueInfo.size = currentSendBufferSize;
            sendQueueInfo.last = std::chrono::steady_clock::now();
        } else {
            const auto limit = (getBucket().state == Bucket::State::Ready)
                                       ? std::chrono::seconds(29)
                                       : std::chrono::seconds(1);
            if ((std::chrono::steady_clock::now() - sendQueueInfo.last) >
                limit) {
                LOG_WARNING(
                        "{}: send buffer stuck at {} for ~{} seconds. Shutting "
                        "down connection {}",
                        getId(),
                        sendQueueInfo.size,
                        limit.count(),
                        getDescription());
                // We've not had any progress on the socket for "n" secs
                // Forcibly shut down the connection!
                sendQueueInfo.term = true;
                setState(StateMachine::State::closing);
            }
        }
    }

    if (Settings::instance().getVerbose() > 1) {
        do {
            LOG_DEBUG("{} - Running task: {}",
                      getId(),
                      stateMachine.getCurrentStateName());
        } while (stateMachine.execute());
    } else {
        while (stateMachine.execute()) {
            // empty
        }
    }
}

void Connection::setAgentName(cb::const_char_buffer name) {
    auto size = std::min(name.size(), agentName.size() - 1);
    std::copy(name.begin(), name.begin() + size, agentName.begin());
    agentName[size] = '\0';
}

void Connection::setConnectionId(cb::const_char_buffer uuid) {
    auto size = std::min(uuid.size(), connectionId.size() - 1);
    std::copy(uuid.begin(), uuid.begin() + size, connectionId.begin());
    // the uuid string shall always be zero terminated
    connectionId[size] = '\0';
}

bool Connection::shouldDelete() {
    return getState() == StateMachine::State ::destroyed;
}

void Connection::setInternal(bool internal) {
    Connection::internal = internal;
}

size_t Connection::getNumberOfCookies() const {
    size_t ret = 0;
    for (const auto& cookie : cookies) {
        if (cookie) {
            ++ret;
        }
    }

    return ret;
}

bool Connection::isPacketAvailable() const {
    auto buffer = read->rdata();

    if (buffer.size() < sizeof(cb::mcbp::Request)) {
        // we don't have the header, so we can't even look at the body
        // length
        return false;
    }

    const auto* req = reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
    return buffer.size() >= sizeof(cb::mcbp::Request) + req->getBodylen();
}

bool Connection::processServerEvents() {
    if (server_events.empty()) {
        return false;
    }

    const auto before = getState();

    // We're waiting for the next command to arrive from the client
    // and we've got a server event to process. Let's start
    // processing the server events (which might toggle our state)
    if (server_events.front()->execute(*this)) {
        server_events.pop();
    }

    return getState() != before;
}

void Connection::runEventLoop(short) {
    conn_loan_buffers(this);
    numEvents = max_reqs_per_event;

    try {
        runStateMachinery();
    } catch (const std::exception& e) {
        bool logged = false;
        if (getState() == StateMachine::State::execute ||
            getState() == StateMachine::State::validate) {
            try {
                // Converting the cookie to json -> string could probably
                // cause too much memory allcation. We don't want that to
                // cause us to crash..
                std::stringstream ss;
                nlohmann::json array = nlohmann::json::array();
                for (const auto& cookie : cookies) {
                    if (cookie) {
                        try {
                            array.push_back(cookie->toJSON());
                        } catch (const std::exception&) {
                            // ignore
                        }
                    }
                }
                LOG_ERROR(
                        R"({}: exception occurred in runloop during packet execution. Cookie info: {} - closing connection ({}): {})",
                        getId(),
                        array.dump(),
                        getDescription(),
                        e.what());
                logged = true;
            } catch (const std::bad_alloc&) {
                // none
            }
        }

        if (!logged) {
            try {
                LOG_ERROR(
                        R"({}: exception occurred in runloop (state: "{}") - closing connection ({}): {})",
                        getId(),
                        getStateName(),
                        getDescription(),
                        e.what());
            } catch (const std::exception&) {
                // Ditch logging.. just shut down the connection
            }
        }

        setState(StateMachine::State::closing);
        /*
         * In addition to setting the state to conn_closing
         * we need to move execution foward by executing
         * conn_closing() and the subsequent functions
         * i.e. conn_pending_close() or conn_immediate_close()
         */
        try {
            runStateMachinery();
        } catch (const std::exception& e) {
            try {
                LOG_ERROR(
                        R"({}: exception occurred in runloop whilst attempting to close connection ({}): {})",
                        getId(),
                        getDescription(),
                        e.what());
            } catch (const std::exception&) {
                // Drop logging
            }
        }
    }

    conn_return_buffers(this);
}

bool Connection::close() {
    bool ewb = false;
    uint32_t rc = refcount;

    for (auto& cookie : cookies) {
        if (cookie) {
            rc += cookie->getRefcount();
            if (cookie->isEwouldblock()) {
                ewb = true;
            } else {
                cookie->reset();
            }
        }
    }

    if (getState() == StateMachine::State::closing) {
        // We don't want any network notifications anymore. Start by disabling
        // all read notifications (We may have data in the write buffers we
        // want to send. It seems like we don't immediately send the data over
        // the socket when writing to a bufferevent. it is scheduled to be sent
        // once we return from the dispatch function for the read event. If
        // we nuke the connection now, the error message we tried to send back
        // to the client won't be sent).
        disableReadEvent();
        cb::net::shutdown(socketDescriptor, SHUT_RD);

        // Release all reserved items!
        releaseReservedItems();
    }

    // Notify interested parties that the connection is currently being
    // disconnected
    propagateDisconnect();

    if (isDCP()) {
        // DCP channels work a bit different.. they use the refcount
        // to track if it has a reference in the engine
        ewb = false;
    }

    if (rc > 1 || ewb || havePendingData()) {
        LOG_WARNING("{}: Delay shutdown: refcount: {} ewb: {} pendingData: {}",
                    getId(),
                    rc,
                    ewb,
                    getSendQueueSize());
        setState(StateMachine::State::pending_close);
        return false;
    }
    setState(StateMachine::State::immediate_close);
    return true;
}

void Connection::propagateDisconnect() const {
    for (auto& cookie : cookies) {
        if (cookie) {
            perform_callbacks(ON_DISCONNECT, nullptr, cookie.get());
        }
    }
}

bool Connection::signalIfIdle() {
    for (const auto& c : cookies) {
        if (c->isEwouldblock()) {
            return false;
        }
    }

    if (stateMachine.isIdleState()) {
        thread.notification.push(this);
        notify_thread(thread);
        return true;
    }

    return false;
}

void Connection::setPriority(Connection::Priority priority) {
    Connection::priority.store(priority);
    switch (priority) {
    case Priority::High:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::High);
        return;
    case Priority::Medium:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Medium);
        return;
    case Priority::Low:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Low);
        return;
    }
    throw std::invalid_argument("Unkown priority: " +
                                std::to_string(int(priority)));
}

bool Connection::selectedBucketIsXattrEnabled() const {
    auto* bucketEngine = getBucketEngine();
    if (bucketEngine) {
        return Settings::instance().isXattrEnabled() &&
               bucketEngine->isXattrEnabled();
    }
    return Settings::instance().isXattrEnabled();
}

void Connection::disableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == EV_READ) {
        if (bufferevent_disable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "McbpConnection::disableReadEvent: Failed to disable read "
                    "events");
        }
    }
}

void Connection::enableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == 0) {
        if (bufferevent_enable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "McbpConnection::enableReadEvent: Failed to enable read "
                    "events");
        }
    }
}

bool Connection::havePendingData() const {
    if (sendQueueInfo.term) {
        return false;
    }

    return getSendQueueSize() != 0;
}

size_t Connection::getSendQueueSize() const {
    return evbuffer_get_length(bufferevent_get_output(bev.get()));
}

ENGINE_ERROR_CODE Connection::add_packet_to_send_pipe(
        cb::const_byte_buffer packet) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, packet, &ret](cb::byte_buffer buffer) -> size_t {
        if (buffer.size() < packet.size()) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());
        addIov(buffer.data(), packet.size());
        return packet.size();
    });

    return ret;
}

////////////////////////////////////////////////////////////////////////////
//                                                                        //
//                   DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////

ENGINE_ERROR_CODE Connection::get_failover_log(uint32_t opaque, Vbid vbucket) {
    cb::mcbp::Request req = {};
    req.setMagic(cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpGetFailoverLog);
    req.setOpaque(opaque);
    req.setVBucket(vbucket);

    return add_packet_to_send_pipe(req.getFrame());
}

ENGINE_ERROR_CODE Connection::stream_req(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         uint64_t start_seqno,
                                         uint64_t end_seqno,
                                         uint64_t vbucket_uuid,
                                         uint64_t snap_start_seqno,
                                         uint64_t snap_end_seqno,
                                         const std::string& request_value) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpStreamReqPayload;

    auto size = sizeof(Request) + sizeof(DcpStreamReqPayload) +
                request_value.size();

    std::vector<uint8_t> buffer(size);

    Framebuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    DcpStreamReqPayload payload;
    payload.setFlags(flags);
    payload.setStartSeqno(start_seqno);
    payload.setEndSeqno(end_seqno);
    payload.setVbucketUuid(vbucket_uuid);
    payload.setSnapStartSeqno(snap_start_seqno);
    payload.setSnapEndSeqno(snap_end_seqno);

    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});

    if (request_value.empty()) {
        builder.setValue(request_value);
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::add_stream_rsp(uint32_t opaque,
                                             uint32_t dialogopaque,
                                             cb::mcbp::Status status) {
    cb::mcbp::response::DcpAddStreamPayload extras;
    extras.setOpaque(dialogopaque);
    uint8_t buffer[sizeof(cb::mcbp::Response) + sizeof(extras)];
    cb::mcbp::ResponseBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpAddStream);
    builder.setStatus(status);
    builder.setOpaque(opaque);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::marker_rsp(uint32_t opaque,
                                         cb::mcbp::Status status) {
    cb::mcbp::Response response{};
    response.setMagic(cb::mcbp::Magic::ClientResponse);
    response.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    response.setExtlen(0);
    response.setStatus(status);
    response.setBodylen(0);
    response.setOpaque(opaque);

    return add_packet_to_send_pipe(
            {reinterpret_cast<const uint8_t*>(&response), sizeof(response)});
}

ENGINE_ERROR_CODE Connection::set_vbucket_state_rsp(uint32_t opaque,
                                                    cb::mcbp::Status status) {
    uint8_t buffer[sizeof(cb::mcbp::Response)];
    cb::mcbp::ResponseBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setStatus(status);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::stream_end(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpStreamEndPayload;
    uint8_t buffer[sizeof(Request) + sizeof(DcpStreamEndPayload) +
                   sizeof(cb::mcbp::DcpStreamIdFrameInfo)];

    Framebuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamEnd);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    DcpStreamEndPayload payload;
    payload.setFlags(flags);

    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});

    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        boost::optional<uint64_t> high_completed_seqno,
        cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2Payload;
    auto size = sizeof(Request) + sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                (high_completed_seqno ? sizeof(DcpSnapshotMarkerV2Payload)
                                      : sizeof(DcpSnapshotMarkerV1Payload));

    std::vector<uint8_t> buffer(size);

    Framebuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }

    if (high_completed_seqno) {
        DcpSnapshotMarkerV2Payload payload;
        payload.setStartSeqno(start_seqno);
        payload.setEndSeqno(end_seqno);
        payload.setFlags(flags);
        payload.setHighCompletedSeqno(*high_completed_seqno);
        builder.setExtras(
                {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});
    } else {
        DcpSnapshotMarkerV1Payload payload;
        payload.setStartSeqno(start_seqno);
        payload.setEndSeqno(end_seqno);
        payload.setFlags(flags);
        builder.setExtras(
                {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::mutation(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       uint8_t nru,
                                       cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Failed to get item info", getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    auto key = info.key;
    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpMutationPayload extras(
            by_seqno,
            rev_seqno,
            info.flags,
            gsl::narrow<uint32_t>(info.exptime),
            lock_time,
            nru);

    cb::mcbp::DcpStreamIdFrameInfo frameExtras(sid);

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);
    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
        if (sid) {
            builder.setFramingExtras(frameExtras.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Failed to grow item array", getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    cb::mcbp::Request req = {};
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(extras)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(
            sizeof(extras) + key.size() + value.size() +
            (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    if (sid) {
        req.setFramingExtraslen(sizeof(cb::mcbp::DcpStreamIdFrameInfo));
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &req, &frameExtras, &extras, &value, &ret, &key, sid](
                           cb::byte_buffer wbuf) -> size_t {
        size_t headerSize = sizeof(extras) + sizeof(req);
        if (sid) {
            headerSize += sizeof(frameExtras);
        }
        if (wbuf.size() < headerSize) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        auto nextWbuf = std::copy_n(reinterpret_cast<const uint8_t*>(&req),
                                    sizeof(req),
                                    wbuf.begin());

        if (sid) {
            // Add the optional stream-ID
            nextWbuf =
                    std::copy_n(reinterpret_cast<const uint8_t*>(&frameExtras),
                                sizeof(frameExtras),
                                nextWbuf);
        }

        nextWbuf = std::copy_n(reinterpret_cast<const uint8_t*>(&extras),
                               sizeof(extras),
                               nextWbuf);

        // Add the header (which includes extras, optional frame-extra and
        // optional nmeta)
        addIov(wbuf.data(), headerSize);

        // Add the key
        addIov(key.data(), key.size());

        // Add the value
        addIov(value.data(), value.size());

        return headerSize;
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::deletionInner(const item_info& info,
                                            cb::const_byte_buffer packet,
                                            const DocKey& key) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &packet, &info, &ret, &key](
                           cb::byte_buffer buffer) -> size_t {
        if (buffer.size() <
            (packet.size() +
             cb::mcbp::unsigned_leb128<CollectionIDType>::getMaxSize())) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());

        // Add the header + collection-ID (stored in buffer)
        addIov(buffer.data(), packet.size());

        // Add the key
        addIov(key.data(), key.size());

        // Add the optional payload (xattr)
        if (info.nbytes > 0) {
            addIov(info.value[0].iov_base, info.nbytes);
        }

        return packet.size();
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::deletion(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::mcbp::DcpStreamId sid) {
    // Should be using the V2 callback
    if (isCollectionsSupported()) {
        LOG_WARNING("{}: Connection::deletion: called when collections-enabled",
                    getId());
        return ENGINE_FAILED;
    }

    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::deletion: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    cb::mcbp::request::DcpDeletionV1Payload extdata(by_seqno, rev_seqno);

    const auto total = sizeof(extdata) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());

        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extdata.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::deletion: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    using cb::mcbp::Request;
    using cb::mcbp::request::DcpDeletionV1Payload;
    uint8_t blob[sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                 sizeof(cb::mcbp::DcpStreamIdFrameInfo)];
    auto& req = *reinterpret_cast<Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(DcpDeletionV1Payload)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(
            sizeof(DcpDeletionV1Payload) + key.size() + info.nbytes +
            (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    auto* ptr = blob + sizeof(Request);
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        req.setFramingExtraslen(buf.size());
    }

    std::copy(extdata.getBuffer().begin(), extdata.getBuffer().end(), ptr);
    cb::const_byte_buffer packetBuffer{
            blob,
            sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                    (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)};

    return deletionInner(info, packetBuffer, key);
}

ENGINE_ERROR_CODE Connection::deletion_v2(uint32_t opaque,
                                          cb::unique_item_ptr it,
                                          Vbid vbucket,
                                          uint64_t by_seqno,
                                          uint64_t rev_seqno,
                                          uint32_t delete_time,
                                          cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::deletion_v2: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpDeletionV2Payload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::deletion_v2: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    // Make blob big enough for either delete or expiry
    uint8_t blob[sizeof(cb::mcbp::Request) + sizeof(extras) +
                 sizeof(frameInfo)] = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         info.nbytes + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    return deletionInner(info, {blob, size}, key);
}

ENGINE_ERROR_CODE Connection::expiration(uint32_t opaque,
                                         cb::unique_item_ptr it,
                                         Vbid vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         uint32_t delete_time,
                                         cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::expiration: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpExpirationPayload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::expiration: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    // Make blob big enough for either delete or expiry
    uint8_t blob[sizeof(cb::mcbp::Request) + sizeof(extras) +
                 sizeof(frameInfo)] = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         info.nbytes + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    return deletionInner(info, {blob, size}, key);
}

ENGINE_ERROR_CODE Connection::set_vbucket_state(uint32_t opaque,
                                                Vbid vbucket,
                                                vbucket_state_t state) {
    if (!is_valid_vbucket_state_t(state)) {
        return ENGINE_EINVAL;
    }

    cb::mcbp::request::DcpSetVBucketState extras;
    extras.setState(static_cast<uint8_t>(state));
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::noop(uint32_t opaque) {
    uint8_t buffer[sizeof(cb::mcbp::Request)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpNoop);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::buffer_acknowledgement(uint32_t opaque,
                                                     Vbid vbucket,
                                                     uint32_t buffer_bytes) {
    cb::mcbp::request::DcpBufferAckPayload extras;
    extras.setBufferBytes(buffer_bytes);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::control(uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value) {
    std::vector<uint8_t> buffer;
    buffer.resize(sizeof(cb::mcbp::Request) + key.size() + value.size());
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});

    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    builder.setOpaque(opaque);
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    builder.setValue(
            {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::system_event(uint32_t opaque,
                                           Vbid vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           mcbp::systemevent::version version,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData,
                                           cb::mcbp::DcpStreamId sid) {
    cb::mcbp::request::DcpSystemEventPayload extras(bySeqno, event, version);
    std::vector<uint8_t> buffer;
    buffer.resize(sizeof(cb::mcbp::Request) + sizeof(extras) + key.size() +
                  eventData.size() + sizeof(cb::mcbp::DcpStreamIdFrameInfo));
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});

    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSystemEvent);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setDatatype(cb::mcbp::Datatype::Raw);
    builder.setExtras(extras.getBuffer());
    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }
    builder.setKey(key);
    builder.setValue(eventData);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::get_error_map(uint32_t opaque, uint16_t version) {
    cb::mcbp::request::GetErrmapPayload body;
    body.setVersion(version);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(body)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
    builder.setOpaque(opaque);
    builder.setValue(body.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::prepare(uint32_t opaque,
                                      cb::unique_item_ptr it,
                                      Vbid vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t lock_time,
                                      uint8_t nru,
                                      DocumentState document_state,
                                      cb::durability::Level level) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::prepare: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer buffer{root, info.value[0].iov_len};

    auto key = info.key;

    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpPreparePayload extras(
            by_seqno,
            rev_seqno,
            info.flags,
            gsl::narrow<uint32_t>(info.exptime),
            lock_time,
            nru);
    if (document_state == DocumentState::Deleted) {
        extras.setDeleted(uint8_t(1));
    }
    extras.setDurabilityLevel(level);

    size_t total = sizeof(extras) + key.size() + buffer.size() +
                   sizeof(cb::mcbp::Request);
    if (dcpUseWriteBuffer(total)) {
        // Format a local copy and send
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpPrepare);
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));
        builder.setValue(buffer);

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    // Use an IO vector instead
    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::prepare: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    cb::mcbp::Request req = {};
    req.setMagic(cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpPrepare);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(extras)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(
            gsl::narrow<uint32_t>(sizeof(extras) + key.size() + buffer.size()));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &req, &extras, &buffer, &ret, &key](
                           cb::byte_buffer wbuf) -> size_t {
        const size_t total = sizeof(extras) + sizeof(req);
        if (wbuf.size() < total) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy_n(reinterpret_cast<const uint8_t*>(&req),
                    sizeof(req),
                    wbuf.begin());
        std::copy_n(reinterpret_cast<const uint8_t*>(&extras),
                    sizeof(extras),
                    wbuf.begin() + sizeof(req));

        // Add the header
        addIov(wbuf.data(), sizeof(req) + sizeof(extras));

        // Add the key
        addIov(key.data(), key.size());

        // Add the value
        addIov(buffer.data(), buffer.size());
        return total;
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::seqno_acknowledged(uint32_t opaque,
                                                 Vbid vbucket,
                                                 uint64_t prepared_seqno) {
    cb::mcbp::request::DcpSeqnoAcknowledgedPayload extras(prepared_seqno);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::commit(uint32_t opaque,
                                     Vbid vbucket,
                                     const DocKey& key_,
                                     uint64_t prepare_seqno,
                                     uint64_t commit_seqno) {
    cb::mcbp::request::DcpCommitPayload extras(prepare_seqno, commit_seqno);
    auto key = key_;
    if (!isCollectionsSupported()) {
        // The client doesn't support collections, don't send an encoded key
        key = key.makeDocKeyWithoutCollectionID();
    }
    const size_t totalBytes =
            sizeof(cb::mcbp::Request) + sizeof(extras) + key.size();
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpCommit);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    builder.setKey(cb::const_char_buffer(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::abort(uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKey& key_,
                                    uint64_t prepared_seqno,
                                    uint64_t abort_seqno) {
    cb::mcbp::request::DcpAbortPayload extras(prepared_seqno, abort_seqno);
    auto key = key_;
    if (!isCollectionsSupported()) {
        // The client doesn't support collections, don't send an encoded key
        key = key.makeDocKeyWithoutCollectionID();
    }
    const size_t totalBytes =
            sizeof(cb::mcbp::Request) + sizeof(extras) + key.size();
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpAbort);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    builder.setKey(cb::const_char_buffer(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

////////////////////////////////////////////////////////////////////////////
//                                                                        //
//               End DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////
