#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace embtable {

// Fixed-size worker pool shared by all ShareMaps in one ShareMapStore.
// Lookup callers submit independent key ranges and wait for their completion;
// worker threads live until the owning ShareMapStore is destroyed.
class PhfLookupThreadPool {
   public:
    explicit PhfLookupThreadPool(size_t threadCount);
    ~PhfLookupThreadPool();

    PhfLookupThreadPool(const PhfLookupThreadPool&) = delete;
    PhfLookupThreadPool& operator=(const PhfLookupThreadPool&) = delete;

    std::future<void> Submit(std::function<void()> task);
    size_t Size() const { return workers_.size(); }

   private:
    void WorkerLoop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
};

}  // namespace embtable
