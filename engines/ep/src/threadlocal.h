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

#ifdef WIN32
#include "threadlocal_win32.h"
template <typename T>
using ThreadLocalBase = ThreadLocalWin32<T>;
#else
#include "threadlocal_posix.h"
template <typename T>
using ThreadLocalBase = ThreadLocalPosix<T>;
#endif

/**
 * Container of thread-local data
 * @tparam T type to be stored
 */
template <typename T>
class ThreadLocal {
public:
    ThreadLocal()
      : tls(destructorWrapper) {
    }

    ~ThreadLocal() {
    }

    void set(T data) {
        // Set the new wrapped value in TLS (deleting any old wrapped value).
        tls.reset(new DataWrapper({data, this}));

        // Update the map
        {
            std::lock_guard<std::mutex> lh(mutex);
            values[std::this_thread::get_id()] = tls.get();
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
    /**
     * Object which owns the actual thread-local user data; plus a pointer
     * back to the ThreadLocal object responsible for owning the object. This
     * is used to ensure the underlying object is deleted when the owning
     * object is deleted.
     */
    struct DataWrapper {
        T data;
        ThreadLocal<T>* owner;
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

    ThreadLocal<std::unique_ptr<DataWrapper>> tls;
    std::mutex mutex;
    std::unordered_map<std::thread::id, T> values;
};

/**
 * Container for a thread-local pointer which is non-owning - i.e. when
 * ThreadLocalPtr is deleted the pointed-to object is not destructed.
 */
template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
    using base_class = ThreadLocal<T*>;
public:
    using ThreadLocal<T*>::ThreadLocal;

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

/**
 * Container for a thread-local pointer which is owning - i.e. when
 * ThreadLocalPtr is deleted the pointed-to object will automatically be
 * deleted.
 */
template <typename T, class Deleter = std::default_delete<T>>
class OwningThreadLocalPtr : public ThreadLocal<std::unique_ptr<T, Deleter>> {
    using base_class = ThreadLocal<std::unique_ptr<T, Deleter>>;

public:
    using base_class::ThreadLocal;

    T* operator->() {
        return base_class::get();
    }

    T operator*() {
        return *base_class::get();
    }

    void operator=(T* newValue) {
        this->set(newValue);
    }
};

#endif // SRC_THREADLOCAL_H_
