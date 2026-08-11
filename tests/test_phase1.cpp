#include "ThreadPool.hpp"
#include "test_utils.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// Test 1: single-threaded submission (main thread only), 100k jobs.
// Checks that the pool's worker side (workerLoop, task dequeue, execution)
// is correct when there is exactly one producer.
void test_basic_correctness() {
    ThreadPool pool(4);
    std::atomic<int> counter{0};
    const int N = 100000;

    std::vector<std::future<void>> futures;
    futures.reserve(N);
    for (int i = 0; i < N; ++i) {
        futures.push_back(pool.submit([&counter] {
            counter.fetch_add(1, std::memory_order_relaxed);
        }));
    }
    for (auto& f : futures) f.get();

    REQUIRE_EQ(counter.load(), N);
    std::cout << "test_basic_correctness passed (count=" << counter.load() << ")\n";
}

// Test 2: multiple producer threads calling submit() concurrently.
// Checks that submit()'s locking is correct under concurrent producers,
// not just concurrent consumers (which Test 1 already covers).
void test_concurrent_submission() {
    std::atomic<int> counter{0};
    const int producers = 8;
    const int jobsPerProducer = 5000;

    {
        ThreadPool pool(4);
        std::vector<std::thread> producerThreads;
        for (int p = 0; p < producers; ++p) {
            producerThreads.emplace_back([&pool, &counter] {
                for (int i = 0; i < jobsPerProducer; ++i) {
                    pool.submit([&counter] {
                        counter.fetch_add(1, std::memory_order_relaxed);
                    });
                }
            });
        }
        for (auto& t : producerThreads) t.join();
        // pool destructs here (end of scope) — destructor joins workers.
    }

    REQUIRE_EQ(counter.load(), producers * jobsPerProducer);
    std::cout << "test_concurrent_submission passed (count=" << counter.load() << ")\n";
}

// Test 3: an exception thrown inside a task must not kill the worker thread;
// it must be captured by packaged_task and rethrown at future::get().
void test_exception_propagation() {
    ThreadPool pool(2);

    auto f = pool.submit([]() -> int {
        throw std::runtime_error("task failed on purpose");
        return 0; // unreachable, but keeps the lambda's return type explicit
    });

    bool caught = false;
    try {
        f.get();
    } catch (const std::runtime_error& e) {
        caught = true;
        REQUIRE(std::string(e.what()) == "task failed on purpose");
    }

    REQUIRE(caught);
    std::cout << "test_exception_propagation passed\n";
}

// Test 4: destroying the pool with work still queued must terminate, not hang.
void test_clean_shutdown() {
    auto start = std::chrono::steady_clock::now();

    {
        ThreadPool pool(4);
        // Queue 1000 jobs that each take a tiny bit of time, then immediately
        // let the pool go out of scope while most are still waiting.
        for (int i = 0; i < 1000; ++i) {
            pool.submit([] {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            });
        }
        // pool destructs HERE — while jobs are still pending.
    }

    auto elapsed = std::chrono::steady_clock::now() - start;
    // If it hung, this line would never be reached at all.
    REQUIRE(elapsed < std::chrono::seconds(5));
    std::cout << "test_clean_shutdown passed\n";
}

// Test 5: higher-priority tasks run first.
// NOTE: uses 1 worker thread, which is the only configuration where global
// priority ordering is actually guaranteed. With N threads there are N
// independent queues, so this test does not prove global ordering.
void test_priority_ordering() {
    ThreadPool pool(1);
    std::vector<Priority> executionOrder;
    std::mutex orderMutex;

    // Block the only worker with a task that waits on a signal, so nothing
    // can start running while we queue the real tasks.
    std::promise<void> releaseSignal;
    std::shared_future<void> releaseFuture(releaseSignal.get_future());

    pool.submit(Priority::Normal, [releaseFuture] {
        releaseFuture.wait();
    });

    auto record = [&](Priority p) {
        std::lock_guard<std::mutex> lock(orderMutex);
        executionOrder.push_back(p);
    };

    // Queue Low, then High, then Normal — deliberately out of priority order.
    auto fLow = pool.submit(Priority::Low, [&record] { record(Priority::Low); });
    auto fHigh = pool.submit(Priority::High, [&record] { record(Priority::High); });
    auto fNormal = pool.submit(Priority::Normal, [&record] { record(Priority::Normal); });

    releaseSignal.set_value(); // let the worker proceed to the real queue

    fLow.get();
    fHigh.get();
    fNormal.get();

    REQUIRE_EQ(executionOrder.size(), 3u);
    REQUIRE(executionOrder[0] == Priority::High);
    REQUIRE(executionOrder[1] == Priority::Normal);
    REQUIRE(executionOrder[2] == Priority::Low);

    std::cout << "test_priority_ordering passed (High -> Normal -> Low confirmed)\n";
}

// Test 6: a job with a declared dependency must not run until that dep finishes.
// WEAK: only passes reliably because A sleeps 50ms. See notes below.
void test_job_dependencies() {
    ThreadPool pool(4);
    std::atomic<bool> jobACompleted{false};
    std::atomic<bool> orderWasCorrect{false};

    auto [idA, futureA] = pool.submit(Priority::Normal, {},
        [&jobACompleted] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            jobACompleted.store(true);
        });

    auto [idB, futureB] = pool.submit(Priority::Normal, std::vector<JobID>{idA},
        [&jobACompleted, &orderWasCorrect] {
            // If this runs before A finishes, jobACompleted is still false.
            orderWasCorrect.store(jobACompleted.load());
        });

    futureA.get();
    futureB.get();

    REQUIRE(orderWasCorrect.load());
    std::cout << "test_job_dependencies passed (B correctly waited for A)\n";
}

// Test 7: WEAK — see notes below. Tasks are round-robined across queues at
// submit time, so multiple threads run work even with stealing disabled.
void test_work_stealing() {
    ThreadPool pool(4);
    std::mutex threadIdMutex;
    std::set<std::thread::id> threadsUsed;

    const int N = 200;
    std::vector<std::future<void>> futures;
    futures.reserve(N);
    for (int i = 0; i < N; ++i) {
        futures.push_back(pool.submit([&threadIdMutex, &threadsUsed] {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            std::lock_guard<std::mutex> lock(threadIdMutex);
            threadsUsed.insert(std::this_thread::get_id());
        }));
    }
    for (auto& f : futures) f.get();

    REQUIRE(threadsUsed.size() > 1);
    std::cout << "test_work_stealing passed (" << threadsUsed.size()
              << " distinct threads participated)\n";
}

int main() {
    test_basic_correctness();
    test_concurrent_submission();
    test_exception_propagation();
    test_clean_shutdown();
    test_priority_ordering();
    test_job_dependencies();
    test_work_stealing();
    std::cout << "All tests passed.\n";
    return 0;
}