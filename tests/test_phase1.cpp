#include "ThreadPool.hpp"

#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

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

int main() {
    test_basic_correctness();
    test_concurrent_submission();
    std::cout << "Phase 1, Step 6 tests passed.\n";
    return 0;
}