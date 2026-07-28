#include "ThreadPool.hpp"

#include <atomic>
#include <cassert>
#include <iostream>
#include <vector>

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

int main() {
    test_basic_correctness();
    std::cout << "Phase 1, Step 5 test passed.\n";
    return 0;
}