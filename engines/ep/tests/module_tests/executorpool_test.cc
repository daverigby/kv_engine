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

/*
 * Unit tests for the ExecutorPool class
 */

#include "executorpool_test.h"
#include "../mock/mock_add_stat_fn.h"
#include "folly_executorpool.h"
#include "lambda_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include <folly/portability/GMock.h>
#include <folly/synchronization/Baton.h>
#include <nlohmann/json.hpp>

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace testing;

/**
 * Creates a LambdaTask which increments the given ThreadGate when run
 */
ExTask makeTask(Taskable& taskable,
                ThreadGate& tg,
                TaskId taskId,
                double initialSleepTime = 0) {
    return std::make_shared<LambdaTask>(
            taskable, taskId, initialSleepTime, true, [&](LambdaTask&) -> bool {
                tg.threadUp();
                return false;
            });
}

::std::ostream& operator<<(::std::ostream& os,
                           const ThreadCountsParams& expected) {
    return os << expected.in_reader_writer << "_CPU" << expected.maxThreads
              << "_W" << expected.writer << "_R" << expected.reader << "_A"
              << expected.auxIO << "_N" << expected.nonIO;
}

template <typename T>
void ExecutorPoolTest<T>::makePool(int maxThreads,
                                   int numReaders,
                                   int numWriters,
                                   int numAuxIO,
                                   int numNonIO) {
    pool = std::make_unique<T>(maxThreads,
                               ThreadPoolConfig::ThreadCount(numReaders),
                               ThreadPoolConfig::ThreadCount(numWriters),
                               numAuxIO,
                               numNonIO);
}

using ExecutorPoolTypes = ::testing::Types<TestExecutorPool, FollyExecutorPool>;
TYPED_TEST_SUITE(ExecutorPoolTest, ExecutorPoolTypes);

/**
 * Tests basic registration / unregistration of Taskables.
 */
TYPED_TEST(ExecutorPoolTest, register_taskable_test) {
    this->makePool(10);

    MockTaskable taskable;
    MockTaskable taskable2;

    // Only CB3ExecutorPool starts / stops threads on first / last
    // taskable.
    const auto expectedInitialWorkers =
            (typeid(TypeParam) == typeid(TestExecutorPool)) ? 0 : 8;

    ASSERT_EQ(expectedInitialWorkers, this->pool->getNumWorkersStat());
    ASSERT_EQ(0, this->pool->getNumTaskables());

    this->pool->registerTaskable(taskable);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(1, this->pool->getNumTaskables());

    this->pool->registerTaskable(taskable2);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(2, this->pool->getNumTaskables());

    this->pool->unregisterTaskable(taskable2, false);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(1, this->pool->getNumTaskables());

    this->pool->unregisterTaskable(taskable, false);

    ASSERT_EQ(expectedInitialWorkers, this->pool->getNumWorkersStat());
    ASSERT_EQ(0, this->pool->getNumTaskables());
}

/// Test that forcefully unregistering a tasktable cancels all outstanding
/// tasks.
TYPED_TEST(ExecutorPoolTest, UnregisterForce) {
    this->makePool(1);
    MockTaskable taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which should never run, as when taskable is unregistered
    // it is done forcefully.
    const auto sleepTime = 60.0 * 60.0; // 1 hour
    std::atomic<bool> taskRan = false;
    ExTask task{new LambdaTask(
            taskable, TaskId::ItemPager, sleepTime, true, [&](LambdaTask&) {
                taskRan = true;
                return false;
            })};
    this->pool->schedule(task);

    // Test: Unregister the taskable forcefully - task should be cancelled and
    // not run.
    this->pool->unregisterTaskable(taskable, true);

    EXPECT_FALSE(taskRan);
}

/// Test that after unregisterTaskable returns no more tasks of that Taskable
/// run.
TYPED_TEST(ExecutorPoolTest, UnregisterTaskablesCancelsTasks) {
    this->makePool(1);
    // Taskable deliberately put on heap to highlight any use-after-free issues
    // if any Tasks access it after unregister.
    auto taskable = std::make_unique<MockTaskable>();
    this->pool->registerTaskable(*taskable);

    // Create a task which runs constantly until cancelled.
    folly::Baton taskStart;

    auto taskFn = [&taskStart, taskablePtr = taskable.get()](LambdaTask& task) {
        // Post that this task has started.
        taskStart.post();

        // Sleep here to increase chance it is still running on CPU pool when
        // its taskable is unregistered.
        std::this_thread::sleep_for(std::chrono::milliseconds{1});

        // Access it's taskable - if this runs after unregisterTaskable() than
        // that would be invalid.
        taskablePtr->logRunTime(task.getTaskId(), {});
        return false;
    };
    auto task = std::make_shared<LambdaTask>(
            *taskable, TaskId::ItemPager, 0, false, taskFn);
    this->pool->schedule(task);

    // Wait for task to be running.
    taskStart.wait();

    // Test: Unregister the taskable, then delete the taskable. Taskable should
    // not be accessed.
    this->pool->unregisterTaskable(*taskable, false);
    taskable.reset();
}

/**
 * Test that after unregisterTaskable returns all tasks it owns (no references
 * retained by others) are deleted.
 * Regression test for MB-42049.
 */
TYPED_TEST(ExecutorPoolTest, UnregisterTaskablesDeletesTask) {
    // Test Task which set the given flag to true deleted.
    struct OnDeleteTask : public LambdaTask {
        OnDeleteTask(Taskable& t, bool& deleted)
            : LambdaTask(t,
                         TaskId::ItemPager,
                         /*sleeptime*/ 0,
                         /*completeBeforeShutdown*/ false,
                         [](LambdaTask&) {
                             std::this_thread::yield();
                             return true;
                         }),
              deleted(deleted) {
        }
        ~OnDeleteTask() override {
            std::this_thread::yield();
            deleted = true;
        }
        bool& deleted;
    };

    this->makePool(1);

    // Not interested in any calls to logQTime when task runs - just ignore them
    // using NiceMock.
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create an OnDeleteTask, and transfer ownership to ExecutorPool.
    // This should be destructed before unregisterTaskable returns.
    bool deleted = false;
    this->pool->schedule(std::make_shared<OnDeleteTask>(taskable, deleted));

    // Test: Unregister the taskable. 'deleted' should be true as soon as
    // unregisterTaskable finishes.
    this->pool->unregisterTaskable(taskable, false);
    EXPECT_TRUE(deleted)
            << "Task should be deleted when unregisterTaskable returns";
}

/// Test that tasks are run immediately when they are woken.
TYPED_TEST(ExecutorPoolTest, Wake) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a lambda task which when run increments ThreadGate, but whose
    // initial sleep time is 1 hour and hence will not run until then.
    ThreadGate tg{1};
    auto sleepTime = 60.0 * 60.0; // 1 hour.
    const auto taskId = this->pool->schedule(
            makeTask(taskable, tg, TaskId::ItemPager, sleepTime));

    // Test: Wake the task, and expect to run "immediately" (give a generous
    // deadline of 10s.
    EXPECT_TRUE(this->pool->wake(taskId));
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for task to wake";

    this->pool->unregisterTaskable(taskable, true);
}

/// Test that calling wake() on a Task multiple times while it is executing
// only wakes it once.
TYPED_TEST(ExecutorPoolTest, WakeMultiple) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which increments runCount everytime it runs, then re-wakes
    // itself twice on first run.
    // We expect the two wake-ups to be coaleacsed into one execution, _after_
    // this task run completes - i.e. the task shouldn't run again concurrently
    // in a different thread.
    SyncObject cv;
    int runCount = 0;
    auto task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, false, [&](LambdaTask& task) {
                // Initially put the task to sleep "forever".
                task.snooze(INT_MAX);
                {
                    std::unique_lock<std::mutex> guard(cv);
                    runCount++;
                    if (runCount == 1) {
                        // Attempt to wake multiple times. This should only
                        // run the task once (a second time), wake while
                        // already queued to run should have no effect.
                        this->pool->wake(task.getId());
                        this->pool->wake(task.getId());
                        this->pool->wake(task.getId());
                    }
                    cv.notify_one();
                }

                return true;
            });
    this->pool->schedule(task);

    // Test - wait for the task to run (hopefully only) two times.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount >= 2; });
    }

    // Unregister the taskable. This should run any pending tasks to run
    // to completion (note the above task is created with
    // completeBeforeShutdown==true).
    this->pool->unregisterTaskable(taskable, false);
    // Check the runCount again - no more instances should have occurred
    // while shutting down.
    {
        std::unique_lock<std::mutex> guard(cv);
        EXPECT_EQ(2, runCount)
                << "Task should only have run twice after wakeup";
    }
}

// Wake negative test - attempt to wake a task which hasn't been scheduled.
TYPED_TEST(ExecutorPoolTest, WakeWithoutSchedule) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Doesn't matter what the task is, we shouldn't run it.
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                return false;
            });

    EXPECT_FALSE(this->pool->wake(task->getId()));

    this->pool->unregisterTaskable(taskable, false);
}

/// Test that snooze correctly updates the waketime of a task.
TYPED_TEST(ExecutorPoolTest, Snooze) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which when run increments ThreadGate, but whose
    // initial sleep time is 1 hour and hence will not run until then.
    ThreadGate tg{1};
    auto initialSleepTime = 60.0 * 60.0; // 1 hour.
    const auto taskId = this->pool->schedule(
            makeTask(taskable, tg, TaskId::ItemPager, initialSleepTime));

    // Test: Wake the task, and expect to run "immediately" (give a generous
    // deadline of 10s.
    // Test: re-snooze the task to run in a fraction of a second.
    EXPECT_TRUE(this->pool->snooze(taskId, std::numeric_limits<double>::min()));
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete())
            << "Timeout waiting for task to run after snooze";

    // Cleanup.
    this->pool->unregisterTaskable(taskable, false);
}

// Test that waking a task supercedes any previous schedule time, and the task
// runs just once.
TYPED_TEST(ExecutorPoolTest, SnoozeThenWake) {
    using namespace std::chrono;

    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which increments runCount everytime it runs, initally
    // sleeping for 10ms.
    // It also blocks on a threadGate - this is because if the task finishes
    // it'll either be re-scheduled to run (return true), or
    // be cancelled (return false) - either way this wouldn't allow us to
    // test what happens to the original scheduled time.
    duration<double> sleepTime{0.01};
    // Using a thread gate with count 2 - we only expect one thread to checkin
    // to the gate, the second count will be used to unblock the task.
    ThreadGate tg{2};
    SyncObject cv;
    int runCount = 0;
    auto task = std::make_shared<LambdaTask>(
            taskable,
            TaskId::ItemPager,
            sleepTime.count(),
            true,
            [&](LambdaTask& t) {
                {
                    std::unique_lock<std::mutex> guard(cv);
                    runCount++;
                    cv.notify_one();
                }
                tg.threadUp();
                return false;
            });
    this->pool->schedule(task);

    // Test: Wake the task, expecting it to run immediately, but not a second
    // time (the previous schedule time should have been superceded).
    EXPECT_TRUE(this->pool->wake(task->getId()));
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 1; });
    }

    // Wait for a further 2x 'sleepTime' s to check that the original scheduled
    // wake time was correcty cancelled, and the task isn't run more than once.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait_for(guard,
                    duration_cast<nanoseconds>(2 * sleepTime),
                    [&runCount] { return runCount > 1; });
    }
    EXPECT_EQ(1, runCount);

    // Cleanup - increment thread gate to unblock task.
    tg.threadUp();
    EXPECT_EQ(2, tg.getCount());

    this->pool->unregisterTaskable(taskable, false);
}

// Snooze negative test - attempt to wake a task which hasn't been scheduled.
TYPED_TEST(ExecutorPoolTest, SnoozeWithoutSchedule) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Doesn't matter what the task is, we shouldn't run it.
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                return false;
            });

    EXPECT_FALSE(this->pool->snooze(task->getId(), 1.0));

    this->pool->unregisterTaskable(taskable, false);
}

/* This test creates an ExecutorPool, and attempts to verify that calls to
 * setNumWriters are able to dynamically create more workers than were present
 * at initialisation. A ThreadGate is used to confirm that two tasks
 * of type WRITER_TASK_IDX can run concurrently
 *
 */
TYPED_TEST(ExecutorPoolTest, increase_workers) {
    if (folly::kIsSanitizeThread) {
        // MB-41309: Skip this test as TSan reports a data-race deep inside
        // folly::SaturatingSemaphore. Expected to be addressed when folly
        // is upgraded to v2020.06.29.00 or greater.
        GTEST_SKIP();
    }
    const size_t numReaders = 1;
    const size_t numWriters = 1;
    const size_t numAuxIO = 1;
    const size_t numNonIO = 1;

    const size_t originalWorkers =
            numReaders + numWriters + numAuxIO + numNonIO;

    // This will allow us to check that numWriters + 1 writer tasks can run
    // concurrently after setNumWriters has been called.
    ThreadGate tg{numWriters + 1};

    this->makePool(5, numReaders, numWriters, numAuxIO, numNonIO);

    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    std::vector<ExTask> tasks;

    for (size_t i = 0; i < numWriters + 1; ++i) {
        // Use any Writer thread task (StatSnap) for the TaskId.
        ExTask task = makeTask(taskable, tg, TaskId::StatSnap);
        this->pool->schedule(task);
        tasks.push_back(task);
    }

    EXPECT_EQ(numWriters, this->pool->getNumWriters());
    ASSERT_EQ(originalWorkers, this->pool->getNumWorkersStat());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(numWriters + 1));

    EXPECT_EQ(numWriters + 1, this->pool->getNumWriters());
    ASSERT_EQ(originalWorkers + 1, this->pool->getNumWorkersStat());

    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to run";

    this->pool->unregisterTaskable(taskable, false);
}

// Verifies the priority of the different thread types. On Windows and Linux
// the Writer threads should be low priority.
TYPED_TEST(ExecutorPoolTest, ThreadPriorities) {
    // Create test pool and register a (mock) taskable to start all threads.
    this->makePool(10);

    const size_t totalNumThreads = 8;

    // Given we have to wait for all threads to be running (called
    // ::run()) before their priority will be set, use a ThreadGate
    // with a simple Task which calls threadUp() to ensure all threads
    // have started before checking priorities.
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);
    ThreadGate tg{totalNumThreads};

    // Task which when run records the priority of the thread it is executing
    // on, then increments threadGate.
    struct CheckPriorityTask : public GlobalTask {
        CheckPriorityTask(Taskable& taskable,
                          TaskId taskId,
                          ThreadGate& threadGate)
            : GlobalTask(taskable, taskId), threadGate(threadGate) {
        }

        std::string getDescription() override {
            return "CheckPriorityTask - "s + GlobalTask::getTaskName(taskId);
        }

        std::chrono::microseconds maxExpectedDuration() override {
            return {};
        }

    protected:
        bool run() override {
            threadPriority = getpriority(PRIO_PROCESS, 0);
            threadGate.threadUp();
            return false;
        }

    public:
        ThreadGate& threadGate;
        /// Observed priority of the thread this task was run on.
        int threadPriority{-1};
    };

    // Need 2 tasks of each type, so both threads of each type are
    // started.
    auto makePriorityTask = [&taskable, &tg](TaskId taskId) {
        return std::make_shared<CheckPriorityTask>(taskable, taskId, tg);
    };
    std::vector<std::shared_ptr<CheckPriorityTask>> tasks;
    // Reader
    tasks.push_back(makePriorityTask(TaskId::MultiBGFetcherTask));
    tasks.push_back(makePriorityTask(TaskId::MultiBGFetcherTask));
    // Writer
    tasks.push_back(makePriorityTask(TaskId::FlusherTask));
    tasks.push_back(makePriorityTask(TaskId::FlusherTask));
    // AuxIO
    tasks.push_back(makePriorityTask(TaskId::AccessScanner));
    tasks.push_back(makePriorityTask(TaskId::AccessScanner));
    // NonIO
    tasks.push_back(makePriorityTask(TaskId::ItemPager));
    tasks.push_back(makePriorityTask(TaskId::ItemPager));

    for (auto& task : tasks) {
        this->pool->schedule(task);
    }
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to start";

    // Windows (via folly portability) uses 20 for default (normal) priority.
    const int defaultPriority = folly::kIsWindows ? 20 : 0;

    // We only set Writer threads to a non-default level on Linux.
    const int expectedWriterPriority = folly::kIsLinux ? 19 : defaultPriority;

    for (const auto& task : tasks) {
        const auto type = GlobalTask::getTaskType(task->getTaskId());
        switch (type) {
        case WRITER_TASK_IDX:
            EXPECT_EQ(expectedWriterPriority, task->threadPriority)
                    << "for task: " << task->getDescription();
            break;
        case READER_TASK_IDX:
        case AUXIO_TASK_IDX:
        case NONIO_TASK_IDX:
            EXPECT_EQ(defaultPriority, task->threadPriority)
                    << "for task: " << task->getDescription();
            break;
        default:
            FAIL() << "Unexpected task type: " << type;
        }
    }

    this->pool->unregisterTaskable(taskable, false);
}

// Test that Task queue stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, TaskQStats) {
    if (typeid(TypeParam) == typeid(FollyExecutorPool)) {
        // Not yet implemented for FollyExecutorPool.
        GTEST_SKIP();
    }
    this->makePool(1);
    // Create and register both high and low priority buckets so stats for
    // both priorities are reported.
    MockTaskable lowPriBucket{LOW_BUCKET_PRIORITY};
    MockTaskable highPriBucket;
    this->pool->registerTaskable(lowPriBucket);
    this->pool->registerTaskable(highPriBucket);

    // Create some tasks with long sleep times to check for non-zero InQueue
    // (futureQueue) sizes.
    auto scheduleTask = [&](Taskable& bucket, TaskId id) {
        this->pool->schedule(ExTask(new LambdaTask(
                bucket, id, 600.0, true, [&](LambdaTask&) { return false; })));
    };

    // Reader.
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    // Writer
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    // AuxIO
    scheduleTask(highPriBucket, TaskId::AccessScanner);
    // NonIO - left empty.

    MockAddStat mockAddStat;

    using ::testing::_;
    const void* cookie = this;
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:InQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:InQsize", "2", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:InQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:InQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:OutQsize", "0", cookie));

    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Writer:InQsize", "3", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Writer:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Reader:InQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Reader:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_AuxIO:InQsize", "1", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_AuxIO:OutQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_NonIO:InQsize", "0", cookie));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_NonIO:OutQsize", "0", cookie));

    this->pool->doTaskQStat(lowPriBucket, this, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(lowPriBucket, true);
    this->pool->unregisterTaskable(highPriBucket, true);
}

// Test that worker stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, WorkerStats) {
    if (typeid(TypeParam) == typeid(FollyExecutorPool)) {
        // Not yet implemented for FollyExecutorPool.
        GTEST_SKIP();
    }

    this->makePool(1, 1, 1, 1, 1);
    // Create two buckets so they have different names.
    NiceMock<MockTaskable> bucket0;
    bucket0.setName("bucket0");
    NiceMock<MockTaskable> bucket1;
    bucket1.setName("bucket1");
    this->pool->registerTaskable(bucket0);
    this->pool->registerTaskable(bucket1);

    // Create a task which just runs forever (until task is cancelled) so
    // we have a running task to show stats for.
    ThreadGate tg{1};
    this->pool->schedule(ExTask(new LambdaTask(
            bucket0, TaskId::ItemPager, 0, false, [&tg](LambdaTask& me) {
                tg.threadUp();
                while (!me.isdead()) {
                    std::this_thread::yield();
                }
                return false;
            })));
    tg.waitFor(std::chrono::seconds(10));
    ASSERT_TRUE(tg.isComplete());

    MockAddStat mockAddStat;

    using namespace testing;
    const void* cookie = this;
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:state", _, cookie));
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:task", _, cookie));
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:cur_time", _, cookie));

    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:state", _, cookie));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:task", _, cookie));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:cur_time", _, cookie));

    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:state", _, cookie));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:task", _, cookie));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:cur_time", _, cookie));

    // NonIO pool has the above ItemPager task running on it.
    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:bucket", "bucket0", cookie));
    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:state", "running", cookie));
    EXPECT_CALL(mockAddStat,
                callback("NonIO_worker_0:task", "Lambda Task", cookie));
    // The 'runtime' stat is only reported if the thread is in state
    // EXECUTOR_RUNNING. Nominally it will only be in that state when actually
    // running a task, however it also briefly exists in that state when
    // first started, before it determines if there is a Task to run.
    // As such, allow 'runtime' to be reported, but it is optional for all but
    // the NonIO pool, where we *know* we have the above test task running.
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:runtime", _, cookie))
            .Times(AtMost(1));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:runtime", _, cookie))
            .Times(AtMost(1));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:runtime", _, cookie))
            .Times(AtMost(1));

    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:runtime", _, cookie));
    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:cur_time", _, cookie));

    this->pool->doWorkerStat(bucket0, this, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(bucket0, true);
    this->pool->unregisterTaskable(bucket1, true);
}

// Test that task stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, TaskStats) {
    this->makePool(1, 1, 1, 1, 1);
    // Create two buckets so they have different names.
    NiceMock<MockTaskable> bucket0;
    bucket0.setName("bucket0");
    NiceMock<MockTaskable> bucket1;
    bucket1.setName("bucket1");
    this->pool->registerTaskable(bucket0);
    this->pool->registerTaskable(bucket1);

    // Create two tasks which just run once, then sleep forever, so we have
    // some task stats to report.
    ThreadGate tg{2};
    auto sleepyFn = [&tg](LambdaTask& me) {
        tg.threadUp();
        me.snooze(INT_MAX);
        return true;
    };
    this->pool->schedule(std::make_shared<LambdaTask>(
            bucket0, TaskId::ItemPager, 0, false, sleepyFn));
    this->pool->schedule(std::make_shared<LambdaTask>(
            bucket1, TaskId::FlusherTask, 0, false, sleepyFn));
    tg.waitFor(std::chrono::seconds(10));
    ASSERT_TRUE(tg.isComplete());

    MockAddStat mockAddStat;

    using ::testing::_;
    const void* cookie = this;

    // "ep_tasks:tasks" is returned as a JSON object.
    // To check it's value, use a function which decodes the JSON and checks
    // various expected fields.
    auto tasksChecker = [](std::string s) -> std::string {
        auto tasks = nlohmann::json::parse(s);
        if (!tasks.is_array()) {
            return "Not an array:" + tasks.dump();
        }
        for (auto t : tasks) {
            if (!t.is_object()) {
                return "Task not an object: " + t.dump();
            }
            for (auto field : {"tid"s,
                               "state"s,
                               "name"s,
                               "this"s,
                               "bucket"s,
                               "description"s,
                               "priority"s,
                               "waketime_ns"s,
                               "total_runtime_ns"s,
                               "last_starttime_ns"s,
                               "previous_runtime_ns"s,
                               "num_runs"s,
                               "type"s}) {
                if (t.count(field) != 1) {
                    return "Missing field '" + field + "':" + t.dump();
                }
            }
        }
        // Return empty string to indicate success
        return "";
    };

    EXPECT_CALL(mockAddStat,
                callback("ep_tasks:tasks",
                         testing::ResultOf(tasksChecker, ""),
                         cookie));
    EXPECT_CALL(mockAddStat, callback("ep_tasks:cur_time", _, cookie));
    EXPECT_CALL(mockAddStat, callback("ep_tasks:uptime_s", _, cookie));

    this->pool->doTasksStat(bucket0, this, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(bucket0, true);
    this->pool->unregisterTaskable(bucket1, true);
}

// Test that scheduler stats (long long tasks have been waiting to run for)
// are reported correctly.
TYPED_TEST(ExecutorPoolTest, SchedulerStats) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Set expectation on logQTime
    using ::testing::_;
    EXPECT_CALL(taskable, logQTime(TaskId::ItemPager, _));

    // Create a task to run, which we can check scheduler times for.
    ThreadGate tg{1};
    this->pool->schedule(makeTask(taskable, tg, TaskId::ItemPager, 0));

    // Wait for the task to run (and notify threadGate).
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for task to wake";

    this->pool->unregisterTaskable(taskable, true);
}

TYPED_TEST_SUITE(ExecutorPoolDynamicWorkerTest, ExecutorPoolTypes);

TYPED_TEST(ExecutorPoolDynamicWorkerTest, decrease_workers) {
    ASSERT_EQ(2, this->pool->getNumWriters());
    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));
    EXPECT_EQ(1, this->pool->getNumWriters());
}

TYPED_TEST(ExecutorPoolDynamicWorkerTest, setDefault) {
    ASSERT_EQ(2, this->pool->getNumWriters());
    ASSERT_EQ(2, this->pool->getNumReaders());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(4, this->pool->getNumWriters())
            << "num_writers should be 4 with ThreadCount::Default";

    this->pool->setNumReaders(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(16, this->pool->getNumReaders())
            << "num_writers should be capped at 16 with ThreadCount::Default";
}

TYPED_TEST(ExecutorPoolDynamicWorkerTest, setDiskIOOptimized) {
    ASSERT_EQ(2, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(this->MaxThreads, this->pool->getNumWriters());

    this->pool->setNumReaders(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(this->MaxThreads, this->pool->getNumReaders());
}

TEST_P(ExecutorPoolTestWithParam, max_threads_test_parameterized) {
    ThreadCountsParams expected = GetParam();

    NiceMock<MockTaskable> taskable;

    TestExecutorPool pool(expected.maxThreads, // MaxThreads
                          expected.in_reader_writer,
                          expected.in_reader_writer,
                          0, // MaxNumAuxio
                          0 // MaxNumNonio
    );

    pool.registerTaskable(taskable);

    EXPECT_EQ(expected.reader, pool.getNumReaders())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.writer, pool.getNumWriters())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.auxIO, pool.getNumAuxIO())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.nonIO, pool.getNumNonIO())
            << "When maxThreads=" << expected.maxThreads;

    pool.unregisterTaskable(taskable, false);
    pool.shutdown();
}

std::vector<ThreadCountsParams> threadCountValues = {
        {ThreadPoolConfig::ThreadCount::Default, 1, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 2, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 4, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 8, 8, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 10, 10, 4, 1, 3},
        {ThreadPoolConfig::ThreadCount::Default, 14, 14, 4, 2, 4},
        {ThreadPoolConfig::ThreadCount::Default, 20, 16, 4, 2, 6},
        {ThreadPoolConfig::ThreadCount::Default, 24, 16, 4, 3, 7},
        {ThreadPoolConfig::ThreadCount::Default, 32, 16, 4, 4, 8},
        {ThreadPoolConfig::ThreadCount::Default, 48, 16, 4, 5, 8},
        {ThreadPoolConfig::ThreadCount::Default, 64, 16, 4, 7, 8},
        {ThreadPoolConfig::ThreadCount::Default, 128, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 1, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 2, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 4, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 8, 8, 8, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 10, 10, 10, 1, 3},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 14, 14, 14, 2, 4},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 20, 20, 20, 2, 6},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 24, 24, 24, 3, 7},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 32, 32, 32, 4, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 48, 48, 48, 5, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 64, 64, 64, 7, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 128, 64, 64, 8, 8}};

INSTANTIATE_TEST_SUITE_P(ThreadCountTest,
                         ExecutorPoolTestWithParam,
                         ::testing::ValuesIn(threadCountValues),
                         ::testing::PrintToStringParamName());

TYPED_TEST(ExecutorPoolDynamicWorkerTest, new_worker_naming_test) {
    EXPECT_EQ(2, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));

    EXPECT_EQ(1, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(2));

    EXPECT_EQ(2, this->pool->getNumWriters());
}

/* Make sure that a task that has run once and been cancelled can be
 * rescheduled and will run again properly.
 */
TYPED_TEST(ExecutorPoolDynamicWorkerTest, reschedule_dead_task) {
    // Must have a single NonIO thread to ensure serialization of `task` and
    // `sentinelTask`.
    this->pool->setNumNonIO(1);

    SyncObject cv;
    size_t runCount{0};
    ExTask task = std::make_shared<LambdaTask>(
            this->taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                std::unique_lock<std::mutex> guard(cv);
                ++runCount;
                cv.notify_one();
                return false;
            });

    ASSERT_EQ(TASK_RUNNING, task->getState())
            << "Initial task state should be RUNNING";

    this->pool->schedule(task);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 1; });
    }

    // To know when `task` has actually finished running and executor thread
    // has set it to dead (and not just got as far notifying the condvar in
    // it's run() method), insert and schedule a "sentinal" task. Once that
    // task has started running we know our main task must have completed
    // all execution.
    bool sentinelExecuted = false;
    ExTask sentinelTask = std::make_shared<LambdaTask>(
            this->taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                std::unique_lock<std::mutex> guard(cv);
                sentinelExecuted = true;
                cv.notify_one();
                return false;
            });
    this->pool->schedule(sentinelTask);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&sentinelExecuted] { return sentinelExecuted; });
    }

    EXPECT_EQ(TASK_DEAD, task->getState())
            << "Task has completed and been cleaned up, state should be DEAD";

    // Schedule main task again.
    this->pool->schedule(task);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 2; });
    }

    // As above, use sentinal task to ensure task has indeed finished execution.
    {
        std::unique_lock<std::mutex> guard(cv);
        sentinelExecuted = false;
    }
    this->pool->schedule(sentinelTask);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&sentinelExecuted] { return sentinelExecuted; });
    }

    EXPECT_EQ(TASK_DEAD, task->getState())
            << "Task has completed and been cleaned up, state should be DEAD";

    EXPECT_EQ(2, runCount);
}

/* Testing to ensure that repeatedly scheduling a task does not result in
 * multiple entries in the taskQueue - this could cause a deadlock in
 * _unregisterTaskable when the taskLocator is empty but duplicate tasks remain
 * in the queue.
 */
TEST_F(SingleThreadedExecutorPoolTest, ignore_duplicate_schedule) {
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 10, true, [&](LambdaTask&) {
                return false;
            });

    size_t taskId = task->getId();

    ASSERT_EQ(taskId, this->pool->schedule(task));
    ASSERT_EQ(taskId, this->pool->schedule(task));

    std::map<size_t, TaskQpair> taskLocator =
            dynamic_cast<SingleThreadedExecutorPool*>(ExecutorPool::get())
                    ->getTaskLocator();

    TaskQueue* queue = taskLocator.find(taskId)->second.second;

    EXPECT_EQ(1, queue->getFutureQueueSize())
            << "Task should only appear once in the taskQueue";

    this->pool->cancel(taskId, true);
}

template <>
ExecutorPoolEpEngineTest<TestExecutorPool>::ExecutorPoolEpEngineTest() {
    config = "executor_pool_backend=cb3";
}

template <>
ExecutorPoolEpEngineTest<FollyExecutorPool>::ExecutorPoolEpEngineTest() {
    config = "executor_pool_backend=folly";
}

template <typename T>
void ExecutorPoolEpEngineTest<T>::SetUp() {
    engine = SynchronousEPEngine::build(config);
    ExecutorPoolTest<T>::SetUp();
}

template <typename T>
void ExecutorPoolEpEngineTest<T>::TearDown() {
    engine.reset();
    ExecutorPool::shutdown();
}

TYPED_TEST_SUITE(ExecutorPoolEpEngineTest, ExecutorPoolTypes);

class ScheduleOnDestruct {
public:
    ScheduleOnDestruct(ExecutorPool& pool,
                       Taskable& taskable,
                       SyncObject& cv,
                       bool& stopTaskHasRun)
        : pool(pool),
          taskable(taskable),
          cv(cv),
          stopTaskHasRun(stopTaskHasRun) {
    }
    ~ScheduleOnDestruct();

    ExecutorPool& pool;
    Taskable& taskable;
    SyncObject& cv;
    bool& stopTaskHasRun;
};

class ScheduleOnDestructTask : public GlobalTask {
public:
    ScheduleOnDestructTask(EventuallyPersistentEngine& bucket,
                           Taskable& taskable,
                           ExecutorPool& pool,
                           SyncObject& cv,
                           bool& stopTaskHasRun)
        : GlobalTask(&bucket, TaskId::ItemPager, 10, true),
          scheduleOnDestruct(pool, taskable, cv, stopTaskHasRun) {
    }

    bool run() override {
        return false;
    }

    std::string getDescription() override {
        return "ScheduleOnDestructTask";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(60);
    }

    ScheduleOnDestruct scheduleOnDestruct;
};

class StopTask : public GlobalTask {
public:
    StopTask(Taskable& taskable, SyncObject& cv, bool& taskHasRun)
        : GlobalTask(taskable, TaskId::ItemPager, 10, false),
          taskHasRun(taskHasRun),
          cv(cv) {
    }

    bool run() override {
        std::lock_guard<std::mutex> guard(cv);
        taskHasRun = true;
        cv.notify_one();
        return false;
    }

    std::string getDescription() override {
        return "StopTask";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(60);
    }

    // Flag and CV to notify main thread that this task has run.
    bool& taskHasRun;
    SyncObject& cv;
};

ScheduleOnDestruct::~ScheduleOnDestruct() {
    // We don't want to have the StopTask's heap allocation accounted to
    // the bucket we are testing with. This is because we cannot easily
    // determine when StopTask itself is cleaned up by the underlying executor
    // thread, and if it isn't destroyed by the time we check memory at the
    // end of the test then the memory comparison will fail.
    // We don't otherwise care when this Task actually is cleaned up, so
    // simply ignore it's memory usage when allocating, and associate it
    // with mockTaskable so when it's later freed the deallocation is
    // also ignored.
    BucketAllocationGuard guard(nullptr);

    ExTask task = std::make_shared<StopTask>(taskable, cv, stopTaskHasRun);
    // Schedule a Task from the destructor. This is done to validate we
    // do not deadlock when scheduling when called via cancel().

    // MB-40517: This deadlocked
    pool.schedule(task);
    EXPECT_TRUE(pool.wake(task->getId()));
}

/*
 * At least one object in KV-engine (VBucket) requires to schedule a new
 * task from its destructor - and that destructor can be called from a task's
 * destructor. Thus it's a requirement of ExecutorPool/KV that a task
 * destructing within the scope of ExecutorPool::cancel() can safely call
 * ExecutorPool::schedule(). MB-40517 broke this as a double lock (deadlock)
 * occurred.
 *
 * This test simulates the requirements with the following:
 * 1) schedule the "ScheduleOnDestructTask", a task which will schedule another
 *    task when it destructs.
 * 2) Use the real ExecutorPool and run the task
 * 3) ~ScheduleOnDestructTask is called from ExecutorThread::run
 * 4) From the destructor, schedule a new task, StopTask. When this task runs it
 *    calls into ExeuctorThread::stop so the run-loop terminates and the test
 *    can finish.
 * Without the MB-40517 fix, step 4 would deadlock because the ExecutorPool
 * lock is already held by the calling thread.
 */
TYPED_TEST(ExecutorPoolEpEngineTest, cancel_can_schedule) {
    // Note: Need to use global ExecutorPool (and not this->pool as typical for
    // ExecutorPoolTest) as we need an EPEngine object for testing memory
    // tracking, and EpEngine will create theglobal ExecutorPool if it doesn't
    // already exist. We don't want two different pools in action...

    auto* pool = ExecutorPool::get();
    // Config: Require only 1 NonIO thread as we want ScheduleOnDestructTask and
    // StopTask to be serialised with respect to each other.
    pool->setNumNonIO(1);

    // Create and register a MockTaskable to use for the StopTask, as we don't
    // want to account it's memory to the bucket under test. See further
    // comments in ~ScheduleOnDestruct().
    NiceMock<MockTaskable> mockTaskable;
    pool->registerTaskable(mockTaskable);

    // Condition variable and flag to allow this main thread to wait on the
    // final StopTask to be run.
    SyncObject cv;
    bool stopTaskHasRun = false;
    auto memUsedA = this->engine->getEpStats().getPreciseTotalMemoryUsed();
    ExTask task = std::make_shared<ScheduleOnDestructTask>(
            *this->engine, mockTaskable, *pool, cv, stopTaskHasRun);

    auto id = task->getId();
    ASSERT_EQ(id, pool->schedule(task));
    task.reset(); // only the pool has a reference

    // Wake our task and call run. The task is a 'one-shot' task which will
    // then be cancelled, which as part of cancel calls schedule (for StopTask).
    pool->wake(id);

    // Wait on StopTask to run.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&stopTaskHasRun] { return stopTaskHasRun; });
    }

    EXPECT_EQ(memUsedA, this->engine->getEpStats().getPreciseTotalMemoryUsed());

    // Cleanup - want to ensure global ExecutorPool is destroyed at the end of
    // this test, which requires engine is destructed first.
    this->engine.reset();
    pool->unregisterTaskable(mockTaskable, true);
    pool->shutdown();
}

// Test that per-bucket memory tracking is handled correctly when
// tasks are running on background threads.
TYPED_TEST(ExecutorPoolEpEngineTest, MemoryTracking_Run) {
    if (folly::kIsSanitizeAddress || folly::kIsSanitizeThread) {
        // ASan/TSan perform their own interposing of global new / delete, which
        // means KV-Engine's memory tracking (which this test relies on)
        // doesn't work.
        GTEST_SKIP();
    }
    // Setup - create a second engine, so there are two buckets which
    // memory should be accounted to.
    auto secondEngine = SynchronousEPEngine::build(this->config +
                                                   ";couch_bucket=\"engine2\"");
    EventuallyPersistentEngine* engine1 = this->engine.get();
    EventuallyPersistentEngine* engine2 = secondEngine.get();

    // Task which:
    // - does nothing on first execution
    // - allocates memory on second execution
    // - deallocates memory on third execution.
    // Note it initially sleeps forever, must be explicitly woken to run.
    struct MemoryAllocTask : public GlobalTask {
        MemoryAllocTask(EventuallyPersistentEngine* engine)
            : GlobalTask(engine, TaskId::ItemPager, INT_MAX, false) {
        }

        std::string getDescription() override {
            return "MemoryAllocTask";
        }
        std::chrono::microseconds maxExpectedDuration() override {
            return std::chrono::microseconds(1);
        }

    protected:
        bool run() override {
            // Sleep until we are explicitly told to wake.
            this->snooze(INT_MAX);

            std::lock_guard<std::mutex> guard(cv);
            runCount++;
            if (runCount == 1) {
                // do nothing.
            } else if (runCount == 2) {
                payload.resize(4096);
            } else if (runCount == 3) {
                payload.resize(0);
                payload.shrink_to_fit();
            }
            cv.notify_one();
            return true;
        }

    public:
        // Used to guard updates to runCount / notfiy waiting test program.
        SyncObject cv;
        int runCount{0};
        // Some test data which is later expanded / shrunk when task runs.
        std::vector<char> payload;
    };

    // Establish initial memory accounting of the bucket, including the
    // initial payload vector memory.
    auto getMemUsed = [](EventuallyPersistentEngine* engine) {
        return engine->getEpStats().getPreciseTotalMemoryUsed();
    };
    const auto memoryInitial1 = getMemUsed(engine1);
    const auto memoryInitial2 = getMemUsed(engine2);

    // Setup - create an instance of MemoryAllocTask for each bucket.
    auto task1 = [&] {
        BucketAllocationGuard guard(engine1);
        return std::make_shared<MemoryAllocTask>(engine1);
    }();

    auto task2 = [&] {
        BucketAllocationGuard guard(engine2);
        return std::make_shared<MemoryAllocTask>(engine2);
    }();

    // Sanity check - memory should have increased by at least
    // sizeof(MemoryAllocTask) for each engine.
    const auto memoryPostTaskCreate1 = getMemUsed(engine1);
    ASSERT_GE(memoryPostTaskCreate1 - memoryInitial1, sizeof(MemoryAllocTask))
            << "engine1 mem_used has not grown by at least sizeof(task) after "
               "creating it.";

    const auto memoryPostTaskCreate2 = getMemUsed(engine2);
    ASSERT_GE(memoryPostTaskCreate2 - memoryInitial2, sizeof(MemoryAllocTask))
            << "engine2 mem_used has not grown by at least sizeof(task) after "
               "creating it.";

    // Test 1: Schedule both tasks - memory usage shouldn't have changed - any
    // internal memory used by ExecutorPool to track task shouldn't be accounted
    // to bucket.
    ExecutorPool::get()->schedule(task1);
    ExecutorPool::get()->schedule(task2);
    const auto memoryPostTaskSchedule1 = getMemUsed(engine1);
    ASSERT_EQ(memoryPostTaskCreate1, memoryPostTaskSchedule1)
            << "engine1 mem_used has unexpectedly changed after calling "
               "ExecutorPool::schedule()";
    const auto memoryPostTaskSchedule2 = getMemUsed(engine2);
    ASSERT_EQ(memoryPostTaskCreate2, memoryPostTaskSchedule2)
            << "engine2 mem_used has unexpectedly changed after calling "
               "ExecutorPool::schedule()";

    // Test 2 - wake each buckets' MemoryAllocTask, then check mem_used has
    // not changed on first execution.
    ExecutorPool::get()->wake(task1->getId());
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 1; });
    }
    ExecutorPool::get()->wake(task2->getId());
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 1; });
    }
    const auto memoryPostTaskFirstRun1 = getMemUsed(engine1);
    EXPECT_EQ(memoryPostTaskFirstRun1, memoryPostTaskCreate1)
            << "engine1 mem_used has unexpectedly changed after running task "
               "once";
    const auto memoryPostTaskFirstRun2 = getMemUsed(engine2);
    EXPECT_GE(memoryPostTaskFirstRun2, memoryPostTaskCreate2)
            << "engine2 mem_used has unexpectedly changed after running task "
               "once";

    // Test 3 - wake each buckets' MemoryAllocTask, then check mem_used has
    // changed after second execution.
    ExecutorPool::get()->wake(task1->getId());
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 2; });
    }
    ExecutorPool::get()->wake(task2->getId());
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 2; });
    }
    const auto memoryPostTaskSecondRun1 = getMemUsed(engine1);
    EXPECT_GE(memoryPostTaskSecondRun1,
              memoryPostTaskCreate1 + task1->payload.size())
            << "engine1 mem_used has not grown by payload.size() after "
               "running task twice.";
    const auto memoryPostTaskSecondRun2 = getMemUsed(engine2);
    EXPECT_GE(memoryPostTaskSecondRun2,
              memoryPostTaskCreate2 + task2->payload.size())
            << "engine2 mem_used has not grown by payload.size() after "
               "running task twice.";

    // Test 4 - wake each task again, this time the memory usage should drop
    // back down.
    ExecutorPool::get()->wake(task1->getId());
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 3; });
    }
    ExecutorPool::get()->wake(task2->getId());
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 3; });
    }
    const auto memoryPostTaskThirdRun1 = getMemUsed(engine1);
    EXPECT_EQ(memoryPostTaskThirdRun1, memoryPostTaskCreate1)
            << "engine1 mem_used has not returned back to previous value after "
               "running task a third time.";
    const auto memoryPostTaskThirdRun2 = getMemUsed(engine2);
    EXPECT_EQ(memoryPostTaskThirdRun2, memoryPostTaskCreate2)
            << "engine2 mem_used has not returned back to previous value after "
               "running task a third time.";
}
