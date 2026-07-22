#include "share_object/phf_lookup_thread_pool.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <utility>

namespace embtable {

PhfLookupThreadPool::PhfLookupThreadPool(size_t threadCount) {
    threadCount = std::max<size_t>(1, threadCount);
    workers_.reserve(threadCount);
    try {
        for (size_t i = 0; i < threadCount; ++i) {
            workers_.emplace_back([this]() { WorkerLoop(); });
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        throw;
    }
}

PhfLookupThreadPool::~PhfLookupThreadPool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

std::future<void> PhfLookupThreadPool::Submit(std::function<void()> task) {
    auto packagedTask =
        std::make_shared<std::packaged_task<void()>>(std::move(task));
    auto future = packagedTask->get_future();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            throw std::runtime_error("PHF lookup thread pool is stopping");
        }
        tasks_.emplace([packagedTask]() { (*packagedTask)(); });
    }
    cv_.notify_one();
    return future;
}

void PhfLookupThreadPool::WorkerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) return;
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

}  // namespace embtable
