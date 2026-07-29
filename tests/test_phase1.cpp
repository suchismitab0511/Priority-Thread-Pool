#include "ThreadPool.hpp"

#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <stdexcept>

// Test 1: single-threaded submission (main thread only), 100k jobs.
// Checks that the pool's worker side (workerLoop, condition_ signaling,
// task execution) is correct.
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

    assert(counter.load() == N);
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
        // pool destructs here (end of scope) — destructor drains remaining
        // tasks and joins workers, so ALL jobs are guaranteed done by the
        // time we exit this block.
    }

    assert(counter.load() == producers * jobsPerProducer);
    std::cout << "test_concurrent_submission passed (count=" << counter.load() << ")\n";
}

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
        assert(std::string(e.what()) == "task failed on purpose");
    }

    assert(caught);
    std::cout << "test_exception_propagation passed\n";
}

#include <chrono>

void test_clean_shutdown() {
    auto start = std::chrono::steady_clock::now();

    {
        ThreadPool pool(4);
        // Queue 1000 jobs that each take a tiny bit of time,
        // then immediately let the pool go out of scope while
        // most of them are still waiting.
        for (int i = 0; i < 1000; ++i) {
            pool.submit([] {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            });
        }
        // pool destructs HERE — while jobs are still pending.
    }

    auto elapsed = std::chrono::steady_clock::now() - start;
    // If it hung, this line would never even be reached.
    // The 5-second bound is generous — this should finish in well under 1 second.
    assert(elapsed < std::chrono::seconds(5));
    std::cout << "test_clean_shutdown passed\n";
}

int main() {
    test_basic_correctness();
    test_concurrent_submission();
    test_exception_propagation();
    test_clean_shutdown();
    std::cout << "Phase 1, Step 8 tests passed.\n";
    return 0;
}