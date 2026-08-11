#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
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
    JobID id;

    bool operator<(const PrioritizedTask& other) const {
        return priority < other.priority;
    }
};
// Returned by the dependency-aware submit() overload. A named struct instead
// of std::pair so callers write job.id / job.future instead of .first / .second.
template <typename T>
struct Job {
    JobID id;
    std::future<T> future;
};

class ThreadPool {
public:
    explicit ThreadPool(size_t numThreads) {
        if (numThreads == 0) {
            throw std::invalid_argument("ThreadPool requires at least 1 thread");
        }

        workerQueues_.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workerQueues_.push_back(std::make_unique<WorkerQueue>());
        }

        workers_.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workers_.emplace_back([this, i] { workerLoop(i); });
        }
    }

    ~ThreadPool() {
        stopping_.store(true);
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Overload 1: priority, no dependencies. Returns {JobID, future} so the
    // job can be referenced as a dependency by a later submit() call.
    template <typename F, typename... Args>
    auto submit(Priority priority, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<ReturnType> result = task->get_future();

        if (stopping_.load()) {
            throw std::runtime_error("submit() called on a ThreadPool that is shutting down");
        }

        JobID id;
        {
            std::lock_guard<std::mutex> lock(idMutex_);
            id = nextJobId_++;
        }

        size_t index = nextQueueIndex_.fetch_add(1) % workerQueues_.size();
        {
            std::lock_guard<std::mutex> lock(workerQueues_[index]->mutex);
            workerQueues_[index]->queue.push(PrioritizedTask{priority, [task]() { (*task)(); }, id});
        }

        return result;
    }

    // Overload 2: no priority given -> defaults to Normal. Also returns
    // {JobID, future} since it forwards to Overload 1.
    template <typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit(Priority::Normal, std::forward<F>(f), std::forward<Args>(args)...);
    }

    // Overload 3: priority + dependencies -> returns {JobID, future}.
    template <typename F, typename... Args>
    auto submit(Priority priority, const std::vector<JobID>& dependsOn, F&& f, Args&&... args)
        -> Job<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<ReturnType> result = task->get_future();

        if (stopping_.load()) {
            throw std::runtime_error("submit() called on a ThreadPool that is shutting down");
        }

        std::lock_guard<std::mutex> lock(idMutex_);
        JobID id = nextJobId_++;
        std::function<void()> wrappedTask = [task]() { (*task)(); };

        if (dependsOn.empty()) {
            size_t index = nextQueueIndex_.fetch_add(1) % workerQueues_.size();
            std::lock_guard<std::mutex> qlock(workerQueues_[index]->mutex);
            workerQueues_[index]->queue.push(PrioritizedTask{priority, std::move(wrappedTask), id});
        } else {
            pendingJobs_[id] = PendingJob{priority, std::move(wrappedTask),
                                          static_cast<int>(dependsOn.size())};
            for (JobID dep : dependsOn) {
                dependents_[dep].push_back(id);
            }
        }

        return {id, std::move(result)};
    }

private:
    // One of these per worker thread. Each has its own queue + own lock,
    // so a thread accessing its own queue doesn't contend with other
    // threads accessing theirs — contention only happens during stealing.
    struct WorkerQueue {
        std::priority_queue<PrioritizedTask> queue;
        std::mutex mutex;
    };

    // Tracks a job that's waiting on dependencies — not yet runnable.
    struct PendingJob {
        Priority priority;
        std::function<void()> task;
        int remainingDependencies;
    };

    void onJobCompleted(JobID completedId) {
        std::lock_guard<std::mutex> lock(idMutex_);

        auto it = dependents_.find(completedId);
        if (it == dependents_.end()) {
            return;
        }

        for (JobID waitingId : it->second) {
            auto pendingIt = pendingJobs_.find(waitingId);
            if (pendingIt == pendingJobs_.end()) continue;

            pendingIt->second.remainingDependencies--;
            if (pendingIt->second.remainingDependencies == 0) {
                size_t index = nextQueueIndex_.fetch_add(1) % workerQueues_.size();
                {
                    std::lock_guard<std::mutex> qlock(workerQueues_[index]->mutex);
                    workerQueues_[index]->queue.push(PrioritizedTask{
                        pendingIt->second.priority,
                        std::move(pendingIt->second.task),
                        waitingId});
                }
                pendingJobs_.erase(pendingIt);
            }
        }
        dependents_.erase(it);
    }

    void workerLoop(size_t myIndex) {
        for (;;) {
            std::function<void()> task;
            JobID completedId;
            bool gotTask = false;

            {
                std::unique_lock<std::mutex> lock(workerQueues_[myIndex]->mutex);
                if (!workerQueues_[myIndex]->queue.empty()) {
                    completedId = workerQueues_[myIndex]->queue.top().id;
                    task = std::move(workerQueues_[myIndex]->queue.top().task);
                    workerQueues_[myIndex]->queue.pop();
                    gotTask = true;
                }
            }

            if (!gotTask) {
                for (size_t offset = 1; offset < workerQueues_.size(); ++offset) {
                    size_t victim = (myIndex + offset) % workerQueues_.size();
                    std::unique_lock<std::mutex> lock(workerQueues_[victim]->mutex);
                    if (!workerQueues_[victim]->queue.empty()) {
                        completedId = workerQueues_[victim]->queue.top().id;
                        task = std::move(workerQueues_[victim]->queue.top().task);
                        workerQueues_[victim]->queue.pop();
                        gotTask = true;
                        break;
                    }
                }
            }

            if (!gotTask) {
                if (stopping_.load()) {
                    return;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(500));
                continue;
            }

            task();
            onJobCompleted(completedId);
        }
    }

    std::vector<std::thread> workers_;
    std::vector<std::unique_ptr<WorkerQueue>> workerQueues_;
    std::atomic<size_t> nextQueueIndex_{0};
    std::atomic<bool> stopping_{false};

    // Dependency tracking state (idMutex_ protects all three).
    std::mutex idMutex_;
    JobID nextJobId_ = 0;
    std::unordered_map<JobID, PendingJob> pendingJobs_;
    std::unordered_map<JobID, std::vector<JobID>> dependents_;
};