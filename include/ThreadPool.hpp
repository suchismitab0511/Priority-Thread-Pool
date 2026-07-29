#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

using JobID = uint64_t;

enum class Priority : int {
    Low = 0,
    Normal = 1,
    High = 2
};

struct PrioritizedTask {
    Priority priority;
    std::function<void()> task;

    // Used by std::priority_queue to decide ordering.
    // std::priority_queue is a MAX-heap: operator< here means
    // "this is LOWER priority than other" so that higher Priority
    // values naturally end up at the top.
    bool operator<(const PrioritizedTask& other) const {
        return priority < other.priority;
    }
};

class ThreadPool {
public:
    explicit ThreadPool(size_t numThreads) {
        if (numThreads == 0) {
            throw std::invalid_argument("ThreadPool requires at least 1 thread");
        }
        workers_.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            stopping_ = true;
        }
        condition_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template <typename F, typename... Args>
    auto submit(Priority priority, F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<ReturnType> result = task->get_future();

        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (stopping_) {
                throw std::runtime_error("submit() called on a ThreadPool that is shutting down");
            }
            tasks_.push(PrioritizedTask{priority, [task]() { (*task)(); }});
        }
        condition_.notify_one();
        return result;
    }

    // Overload: no priority given -> defaults to Normal.
    // Keeps old-style calls (from Phase 1 tests) compiling unchanged.
    template <typename F, typename... Args>
    auto submit(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
        return submit(Priority::Normal, std::forward<F>(f), std::forward<Args>(args)...);
    }

private:
    void workerLoop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queueMutex_);
                condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });

                if (tasks_.empty()) {
                    return;
                }

                task = std::move(tasks_.top().task);
                tasks_.pop();
            }
            task();
        }
    }

    // Tracks a job that's waiting on dependencies — not yet in the
    // runnable priority queue. Kept separate from PrioritizedTask so the
    // priority queue only ever holds jobs that are already runnable.
    struct PendingJob {
        Priority priority;
        std::function<void()> task;
        int remainingDependencies;
    };

    std::vector<std::thread> workers_;
    std::priority_queue<PrioritizedTask> tasks_;
    std::mutex queueMutex_;
    std::condition_variable condition_;
    bool stopping_ = false;

    // Phase 4: dependency tracking state.
    JobID nextJobId_ = 0;

    // Jobs still waiting on at least one dependency to finish.
    std::unordered_map<JobID, PendingJob> pendingJobs_;

    // Reverse index: for each job, who depends on it? When jobId finishes,
    // we look here to find jobs whose remainingDependencies count needs
    // decrementing.
    std::unordered_map<JobID, std::vector<JobID>> dependents_;
};