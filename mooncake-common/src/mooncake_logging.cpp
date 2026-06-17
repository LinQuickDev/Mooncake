#include "mooncake_logging.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace mooncake::logging {
namespace {

thread_local uint64_t current_trace_id = 0;

uint64_t GetPidForTrace() {
#ifdef _WIN32
    return static_cast<uint64_t>(_getpid());
#else
    return static_cast<uint64_t>(getpid());
#endif
}

uint64_t SteadyClockNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

double ParseHiFreqLogSampleRate() {
    const char* value = std::getenv("MC_HIFREQ_LOG_SAMPLE_RATE");
    if (value == nullptr || *value == '\0') return 0.1;
    errno = 0;
    char* end = nullptr;
    double rate = std::strtod(value, &end);
    if (end == value || errno != 0) return 0.1;  // non-numeric / overflow
    if (rate < 0.0) return 0.0;
    if (rate > 1.0) return 1.0;
    return rate;
}

struct LogEntry {
    const char* file;
    int line;
    google::LogSeverity severity;
    uint64_t trace_id;
    std::string message;
};

class AsyncLogQueue {
   public:
    static AsyncLogQueue& Instance() {
        static AsyncLogQueue* queue = new AsyncLogQueue();
        return *queue;
    }

    void Enqueue(LogEntry entry) {
        EnsureStarted();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_not_full_.wait(lock, [this] {
                return stopped_ || queue_.size() < kMaxQueueSize;
            });
            if (stopped_) {
                WriteSync(entry);
                return;
            }
            queue_.push(std::move(entry));
        }
        queue_not_empty_.notify_one();
    }

    void Flush() {
        EnsureStarted();
        std::unique_lock<std::mutex> lock(mutex_);
        queue_empty_.wait(
            lock, [this] { return queue_.empty() && active_writes_ == 0; });
        google::FlushLogFiles(google::INFO);
    }

   private:
    static constexpr size_t kMaxQueueSize = 8192;

    AsyncLogQueue() = default;

    void EnsureStarted() {
        std::call_once(start_once_, [this] {
            worker_ = std::thread([this] { WorkerLoop(); });
            std::atexit([] { AsyncLogQueue::Instance().Stop(); });
        });
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();
        if (worker_.joinable()) worker_.join();
        google::FlushLogFiles(google::INFO);
    }

    void WorkerLoop() {
        while (true) {
            LogEntry entry;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                queue_not_empty_.wait(
                    lock, [this] { return stopped_ || !queue_.empty(); });
                if (queue_.empty()) {
                    queue_empty_.notify_all();
                    if (stopped_) return;
                    continue;
                }
                entry = std::move(queue_.front());
                queue_.pop();
                ++active_writes_;
            }
            queue_not_full_.notify_one();
            WriteSync(entry);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --active_writes_;
                if (queue_.empty() && active_writes_ == 0) {
                    queue_empty_.notify_all();
                }
            }
        }
    }

    static void WriteSync(const LogEntry& entry) {
        google::LogMessage log_message(entry.file, entry.line, entry.severity);
        auto& stream = log_message.stream();
        if (entry.trace_id != 0) {
            stream << "trace_id[" << entry.trace_id << "] ";
        } else {
            stream << "trace_id[none] ";
        }
        stream << entry.message;
        if (entry.severity == google::FATAL)
            google::FlushLogFiles(google::INFO);
    }

    std::once_flag start_once_;
    std::thread worker_;
    std::mutex mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    std::condition_variable queue_empty_;
    std::queue<LogEntry> queue_;
    size_t active_writes_ = 0;
    bool stopped_ = false;
};

}  // namespace

uint64_t NewTraceId() {
    static const uint64_t process_seed =
        (GetPidForTrace() << 48) ^ (SteadyClockNs() & 0x0000FFFFFFFF0000ULL);
    static std::atomic<uint64_t> counter{1};
    return process_seed ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CurrentTraceId() { return current_trace_id; }

double HiFreqLogSampleRate() {
    static const double rate = ParseHiFreqLogSampleRate();
    return rate;
}

bool ShouldSampleHiFreqLog() {
    const double rate = HiFreqLogSampleRate();
    if (rate >= 1.0) return true;
    if (rate <= 0.0) return false;
    thread_local std::mt19937 rng(static_cast<uint32_t>(
        SteadyClockNs() ^ reinterpret_cast<uintptr_t>(&rng)));
    thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) < rate;
}

bool ShouldLog(google::LogSeverity severity) {
    // MC_LOG_ENABLE was removed: MC_LOG now behaves like plain glog LOG and is
    // gated only by glog's own severity threshold (still async + trace_id).
    if (severity == google::FATAL) return true;
    return severity >= FLAGS_minloglevel;
}

bool ShouldVLog(int level) { return VLOG_IS_ON(level); }

void ApplyMooncakeLogEnableToGlog() {
    // No-op retained for call-site compatibility (master/real_client main).
    // MC_LOG_ENABLE was removed; nothing to apply.
}

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(current_trace_id) {
    current_trace_id = trace_id;
}

ScopedTraceId::~ScopedTraceId() { current_trace_id = previous_trace_id_; }

AsyncLogMessage::AsyncLogMessage(const char* file, int line,
                                 google::LogSeverity severity, bool enabled)
    : file_(file),
      line_(line),
      severity_(severity),
      enabled_(enabled),
      trace_id_(CurrentTraceId()) {}

AsyncLogMessage::~AsyncLogMessage() {
    if (!enabled_) return;
    if (severity_ == google::FATAL) {
        google::LogMessage log_message(file_, line_, severity_);
        auto& output = log_message.stream();
        if (trace_id_ != 0) {
            output << "trace_id[" << trace_id_ << "] ";
        } else {
            output << "trace_id[none] ";
        }
        output << stream_.str();
        return;
    }
    AsyncLogQueue::Instance().Enqueue(
        LogEntry{file_, line_, severity_, trace_id_, stream_.str()});
}

std::ostream& AsyncLogMessage::stream() { return stream_; }

void FlushAsyncLogs() { AsyncLogQueue::Instance().Flush(); }

}  // namespace mooncake::logging
