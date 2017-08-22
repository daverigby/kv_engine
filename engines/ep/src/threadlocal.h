/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef SRC_THREADLOCAL_H_
#define SRC_THREADLOCAL_H_ 1

#include <mutex>
#include <thread>
#include <unordered_map>

// thread local variable dtor
using ThreadLocalDestructor = void (*)(void*);

/**
 * Container of thread-local data
 * @tparam T type to be stored
 * @tparam destructor Optional destructor to be invoked on thread-local on
 *         thread exit / ThreadLocal destruction (separate template
 *         specialisation for nullptr)
 */
template<typename T, ThreadLocalDestructor destructor = nullptr>
class ThreadLocal {
public:
    ThreadLocal()
      : tls(destructorWrapper) {
    }

    ~ThreadLocal() {
        for (const auto& value : values) {
            auto* as_void = reinterpret_cast<void*>(value.second->data);
            if (as_void != nullptr) {
                destructor(as_void);
            }
            delete value.second;
        }
    }

    void set(T data) {
        // Delete the old container
        DataWrapper* old = tls.get();
        if (old) {
            delete old;
        }

        // Set the new wrapped value in TLS
        auto* wrapper = new DataWrapper({data, this});
        tls.set(wrapper);

        // Update the map
        {
            std::lock_guard<std::mutex> lh(mutex);
            values[std::this_thread::get_id()] = wrapper;
        }
    }

    T get() const {
        auto* v = tls.get();
        if (v == nullptr) {
            // Explicitly reinterpreting 0 since nullptr
            // can't be reinterpret cast
            return reinterpret_cast<T>(0);
        }
        return v->data;
    }

    void operator =(T newValue) {
        set(newValue);
    }

    operator T() const {
        return tls.get()->data;
    }

private:
    struct DataWrapper {
        T data;
        ThreadLocal<T, destructor>* owner;
    };

    static void destructorWrapper(void* v) {
        auto* value = static_cast<DataWrapper*>(v);
        {
            std::lock_guard<std::mutex> lh(value->owner->mutex);
            auto it = value->owner->values.find(std::this_thread::get_id());
            if (it != value->owner->values.end()) {
                value->owner->values.erase(it);
            }
        }
        destructor(value->data);
        delete value;
    }

    ThreadLocal<DataWrapper*, nullptr> tls;
    std::mutex mutex;
    std::unordered_map<std::thread::id, DataWrapper*> values;
};

#ifdef WIN32
#include "threadlocal_win32.h"
template <typename T>
class ThreadLocal<T, nullptr> : public ThreadLocalWin32<T> {
    using ThreadLocalWin32<T>::ThreadLocalWin32;
};
#else
#include "threadlocal_posix.h"
template <typename T>
class ThreadLocal<T, nullptr> : public ThreadLocalPosix<T> {
    using ThreadLocalPosix<T>::ThreadLocalPosix;
};
#endif

/**
 * Container for a thread-local pointer.
 */
template <typename T, ThreadLocalDestructor destructor = nullptr>
class ThreadLocalPtr : public ThreadLocal<T*, destructor> {
    using base_class = ThreadLocal<T*, destructor>;
public:
    using ThreadLocal<T*, destructor>::ThreadLocal;

    T *operator ->() {
        return base_class::get();
    }

    T operator *() {
        return *base_class::get();
    }

    void operator =(T *newValue) {
        this->set(newValue);
    }
};

#endif  // SRC_THREADLOCAL_H_
