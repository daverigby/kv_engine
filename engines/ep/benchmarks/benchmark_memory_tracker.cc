/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "benchmark_memory_tracker.h"
#include <objectregistry.h>
#include <utility.h>

#include <engines/ep/src/bucket_logger.h>
#include <platform/cb_arena_malloc.h>

#include <platform/cb_malloc.h>
#include <algorithm>

std::atomic<BenchmarkMemoryTracker*> BenchmarkMemoryTracker::instance;
std::mutex BenchmarkMemoryTracker::instanceMutex;
std::atomic<size_t> BenchmarkMemoryTracker::maxTotalAllocation;
std::atomic<size_t> BenchmarkMemoryTracker::currentAlloc;

BenchmarkMemoryTracker::~BenchmarkMemoryTracker() {
    cb_remove_new_hook(&NewHook);
    cb_remove_delete_hook(&DeleteHook);
}

BenchmarkMemoryTracker* BenchmarkMemoryTracker::getInstance() {
    BenchmarkMemoryTracker* tmp = instance.load();
    if (tmp == nullptr) {
        std::lock_guard<std::mutex> lock(instanceMutex);
        tmp = instance.load();
        if (tmp == nullptr) {
            tmp = new BenchmarkMemoryTracker();
            instance.store(tmp);
            instance.load()->connectHooks();
        }
    }
    return tmp;
}

void BenchmarkMemoryTracker::destroyInstance() {
    std::lock_guard<std::mutex> lock(instanceMutex);
    BenchmarkMemoryTracker* tmp = instance.load();
    if (tmp != nullptr) {
        delete tmp;
        instance = nullptr;
    }
}

void BenchmarkMemoryTracker::connectHooks() {
    if (cb_add_new_hook(&NewHook)) {
        EP_LOG_DEBUG("Registered add hook");
        if (cb_add_delete_hook(&DeleteHook)) {
            EP_LOG_DEBUG("Registered delete hook");
            return;
        }
        cb_remove_new_hook(&NewHook);
    }
    EP_LOG_WARN("Failed to register allocator hooks");
}
void BenchmarkMemoryTracker::NewHook(const void* ptr, size_t) {
    if (ptr != nullptr) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = cb::ArenaMalloc::malloc_usable_size(p);
        currentAlloc += alloc;
        maxTotalAllocation.store(
                std::max(currentAlloc.load(), maxTotalAllocation.load()));
    }
}
void BenchmarkMemoryTracker::DeleteHook(const void* ptr) {
    if (ptr != nullptr) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = cb::ArenaMalloc::malloc_usable_size(p);
        currentAlloc -= alloc;
    }
}

void BenchmarkMemoryTracker::reset() {
    currentAlloc.store(0);
    maxTotalAllocation.store(0);
}
