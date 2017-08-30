#include "threadlocal.h"

#include "memory_tracker.h"

#include <gtest/gtest.h>
#include <platform/make_unique.h>
#include "daemon/alloc_hooks.h"

#include <thread>

/**
 * Return an implementation of the allocator hooks API which is connected to
 * the underlying AllocHooks implementation.
 */
ALLOCATOR_HOOKS_API* getWorkingHooksAPI() {
    static ALLOCATOR_HOOKS_API hooksApi;
    hooksApi.add_new_hook = AllocHooks::add_new_hook;
    hooksApi.remove_new_hook = AllocHooks::remove_new_hook;
    hooksApi.add_delete_hook = AllocHooks::add_delete_hook;
    hooksApi.remove_delete_hook = AllocHooks::remove_delete_hook;
    hooksApi.get_extra_stats_size = AllocHooks::get_extra_stats_size;
    hooksApi.get_allocator_stats = AllocHooks::get_allocator_stats;
    hooksApi.get_allocation_size = AllocHooks::get_allocation_size;
    hooksApi.get_detailed_stats = AllocHooks::get_detailed_stats;
    hooksApi.release_free_memory = AllocHooks::release_free_memory;
    hooksApi.enable_thread_cache = AllocHooks::enable_thread_cache;
    hooksApi.get_allocator_property = AllocHooks::get_allocator_property;
    return &hooksApi;
}

class ThreadLocalPtrTest : public ::testing::Test {
protected:
    void SetUp() override {
        tracker = MemoryTracker::getInstance(*getWorkingHooksAPI());
    }

    size_t getBytesAllocated() {
        tracker->updateStats();
        return tracker->getTotalBytesAllocated();
    }

protected:
    MemoryTracker* tracker;
};

// Create a thread-local object in the main thread, check basic get and set
// work.
TEST_F(ThreadLocalPtrTest, GetSet) {
    ThreadLocalPtr<std::string> tls;
    tls = new std::string{"main thread"};
    EXPECT_EQ("main thread", *tls);
}

// Test that destructing the ThreadLocalPtr owning object doesn't leak.
TEST_F(ThreadLocalPtrTest, ObjectDestruction) {
    auto str = std::make_unique<std::string>("heap object");
    {
        // Explicit scope for ThreadLocalPtr object; check the pointer
        // is deleted once it ends.
        ThreadLocalPtr<std::string> tls;
        tls = str.get();
        EXPECT_EQ("heap object", *tls);
    }
    // Now scope has ended; ThreadLocalPtr undernearth should be freed.
    // Need Valgrind / ASan to check this.
}

// Test that destructing the ThreadLocalPtr owning object doesn't leak when
// created on multiple threads.
TEST_F(ThreadLocalPtrTest, ObjectDestructionThreaded) {
    const auto startingMem = getBytesAllocated();
    {
        ThreadLocalPtr<std::string> tls;
        // Create and immediately delete a thread.
        std::thread t1(
                [this](ThreadLocalPtr<std::string>& tls) {
                    size_t pre = getBytesAllocated();
                    tls = new std::string("thread A");
                    size_t post = getBytesAllocated();
                    std::cerr << "MEM T1:" << getBytesAllocated() << "\n";
                    EXPECT_GT(post, pre);
                },
                std::ref(tls));

        // After threads deleted; memory should have been freed (even though
        // 'tls'
        // object is still here).
        t1.join();
    }
    EXPECT_EQ(startingMem, getBytesAllocated());
}

#if 0
TEST(ThreadLocalTest, Assignment) {
    ThreadLocal<std::string> tls;
    tls = std::string{"main thread"};
    EXPECT_EQ("main_thread", std::string(tls));
}
#endif

// Create a thread-local object in two threads; check they are different objects
TEST(ThreadLocalTest, EachThreadDifferent) {
    ThreadLocal<std::string> tls_str;
}
