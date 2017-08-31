#include "threadlocal.h"

#include "memory_tracker.h"

#include <gtest/gtest.h>
#include <platform/make_unique.h>
#include "daemon/alloc_hooks.h"

#include <thread>

class ThreadLocalPtrTest : public ::testing::Test {
protected:

    size_t getBytesAllocated() {
        allocator_stats stats = {0};
        AllocHooks::get_allocator_stats(&stats);
        return stats.allocated_size;
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
    const auto startMem = getBytesAllocated();
    {
        ThreadLocalPtr<std::string> tls;

        // Create and immediately delete a thread.
        // There's some caching of allocations going on in libc / libstdc++;
        // so we create & destroy 10 instances, then check that memory
        // is within a small margin of error (i.e. hasn't grown by 10x object
        // size).
        for (size_t ii = 0; ii < 10; ++ii) {
            std::thread t1(
                    [](size_t ii, ThreadLocalPtr<std::string>& tls) {
                        // Allocate a large, string. If we are freeing items
                        // correctly then this should be freed when the thread
                        // exits.
                        tls = new std::string(10000, char(ii));
                    },
                    ii,
                    std::ref(tls));
            t1.join();
        }

        // After threads deleted; memory should have been freed (even though
        // 'tls' object is still here). Allow a "fudge" factor of 1K due to
        // aforementioned caching.
        const size_t fudgeFactor = 1024;
        const auto endMem = getBytesAllocated();
        const auto memDifference =
                std::abs(ssize_t(startMem) - ssize_t(endMem));
        EXPECT_LT(memDifference, fudgeFactor);
    }
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
