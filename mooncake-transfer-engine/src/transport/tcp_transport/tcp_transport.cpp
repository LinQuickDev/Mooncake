// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/tcp_transport/tcp_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>
#include <asio/ip/v6_only.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <random>

#include <asio/steady_timer.hpp>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

#include "cuda_alike.h"

namespace mooncake {
using tcpsocket = asio::ip::tcp::socket;
static size_t getChunkSize() {
    static const size_t val = [] {
        const char* env = std::getenv("MC_TCP_SLICE_SIZE");
        if (env) {
            size_t v = std::stoull(env);
            if (v > 0) return v;
        }
        return size_t(65536);  // 64KB default
    }();
    return val;
}

struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
    uint8_t version;
    uint16_t flags;
    uint32_t magic;
    uint64_t request_id;
};

struct SessionAck {
    uint32_t magic;
    uint16_t version;
    uint16_t status;
    uint64_t request_id;
    uint64_t transferred_bytes;
};

static_assert(sizeof(SessionHeader) == 32,
              "TCP session header must have a stable wire size");
static_assert(sizeof(SessionAck) == 24,
              "TCP session ack must have a stable wire size");

constexpr uint32_t kTcpSessionHeaderMagic = 0x4d435448;  // "MCTH"
constexpr uint32_t kTcpSessionAckMagic = 0x4d434143;     // "MCAC"
constexpr uint16_t kTcpSessionProtocolVersion = 1;
static std::atomic<uint64_t> g_tcp_request_id{1};

static uint64_t nextTcpRequestId() {
    uint64_t id = g_tcp_request_id.fetch_add(1, std::memory_order_relaxed);
    return id == 0 ? g_tcp_request_id.fetch_add(1, std::memory_order_relaxed)
                   : id;
}

static std::chrono::milliseconds getTcpAckTimeout() {
    static const auto timeout = [] {
        const char* env = std::getenv("MC_TCP_ACK_TIMEOUT_MS");
        if (env) {
            try {
                auto value = std::stoull(env);
                if (value > 0) return std::chrono::milliseconds(value);
            } catch (const std::exception&) {
                LOG(WARNING) << "Invalid MC_TCP_ACK_TIMEOUT_MS: " << env;
            }
        }
        return std::chrono::milliseconds(5000);
    }();
    return timeout;
}

static bool tcpAckTraceEnabled() {
    static const bool enabled = std::getenv("MC_TCP_ACK_TRACE") != nullptr;
    return enabled;
}

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
static bool isCudaMemory(void* addr) {
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return false;
    return attributes.type == cudaMemoryTypeDevice;
}

// Returns the CUDA device ordinal if addr is device memory, or -1 otherwise.
// Callers must call cudaSetDevice before any cudaMemcpy to avoid implicit
// GPU 0 context creation.
static int getCudaDeviceId(void* addr) {
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return -1;
    if (attributes.type == cudaMemoryTypeDevice) return attributes.device;
    return -1;
}

#ifdef USE_MACA
static cudaError_t copyTcpCudaMemory(void* dst, const void* src, size_t size) {
    cudaStream_t stream;
    cudaError_t status =
        cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
    if (status != cudaSuccess) return status;

    status = cudaMemcpyAsync(dst, src, size, cudaMemcpyDefault, stream);
    if (status == cudaSuccess) {
        status = cudaStreamSynchronize(stream);
    }

    cudaError_t destroy_status = cudaStreamDestroy(stream);
    return status == cudaSuccess ? destroy_status : status;
}
#endif
#endif

// Forward declaration
class TcpTransport;

using ValidateAddrFn = std::function<bool(uint64_t, uint64_t)>;

// Server-side session: handles one transfer request on a persistent connection
struct ServerSession : public std::enable_shared_from_this<ServerSession> {
    explicit ServerSession(std::shared_ptr<tcpsocket> socket,
                           ValidateAddrFn validate_addr)
        : socket_(std::move(socket)),
          validate_addr_(std::move(validate_addr)) {}

    std::shared_ptr<tcpsocket> socket_;
    ValidateAddrFn validate_addr_;
    SessionHeader header_;
    SessionAck ack_{};
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    std::function<void(TransferStatusEnum)> on_finalize_;
    std::mutex session_mutex_;

    void start() {
        session_mutex_.lock();
        total_transferred_bytes_ = 0;
        readHeader();
    }

   private:
    void readHeader() {
        auto self(shared_from_this());
        asio::async_read(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    if (ec.value() != asio::error::eof) {
                        LOG(WARNING)
                            << "ServerSession::readHeader failed. Error: "
                            << ec.message() << " (value: " << ec.value() << ")"
                            << ", bytes read: " << len;
                    }
                    session_mutex_.unlock();
                    return;
                }

                if (le32toh(header_.magic) != kTcpSessionHeaderMagic ||
                    header_.version != kTcpSessionProtocolVersion) {
                    LOG(ERROR)
                        << "ServerSession: invalid TCP session header"
                        << ", magic=" << le32toh(header_.magic)
                        << ", version=" << static_cast<int>(header_.version);
                    asio::error_code close_ec;
                    socket_->close(close_ec);
                    session_mutex_.unlock();
                    return;
                }

                if (tcpAckTraceEnabled()) {
                    LOG(INFO)
                        << "ServerSession received TCP transfer header"
                        << ", request_id=" << le64toh(header_.request_id)
                        << ", opcode=" << static_cast<int>(header_.opcode)
                        << ", size=" << le64toh(header_.size)
                        << ", target=" << static_cast<void*>(local_buffer_);
                }

                local_buffer_ = (char*)(le64toh(header_.addr));
                uint64_t size = le64toh(header_.size);
                if (tcpAckTraceEnabled()) {
                    LOG(INFO)
                        << "ServerSession decoded TCP transfer header"
                        << ", request_id=" << le64toh(header_.request_id)
                        << ", target=" << static_cast<void*>(local_buffer_)
                        << ", size=" << size;
                }
                if (validate_addr_ &&
                    !validate_addr_((uint64_t)local_buffer_, size)) {
                    LOG(ERROR) << "ServerSession: remote-supplied address 0x"
                               << std::hex << (uint64_t)local_buffer_
                               << std::dec << " with size " << size
                               << " is not within any registered buffer";
                    if (header_.opcode == (uint8_t)TransferRequest::WRITE) {
                        sendAck(TransferStatusEnum::FAILED, 0);
                    } else {
                        session_mutex_.unlock();
                    }
                    return;
                }
                if (tcpAckTraceEnabled()) {
                    LOG(INFO) << "ServerSession accepted TCP transfer target"
                              << ", request_id=" << le64toh(header_.request_id);
                }
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    readBody();
                else if (header_.opcode == (uint8_t)TransferRequest::READ)
                    writeBody();
                else {
                    LOG(ERROR) << "ServerSession: unsupported transfer opcode: "
                               << static_cast<int>(header_.opcode);
                    session_mutex_.unlock();
                }
            });
    }

    void sendAck(TransferStatusEnum status, uint64_t transferred_bytes) {
        auto self(shared_from_this());
        ack_.magic = htole32(kTcpSessionAckMagic);
        ack_.version = htole16(kTcpSessionProtocolVersion);
        ack_.status = htole16(static_cast<uint16_t>(status));
        ack_.request_id = header_.request_id;
        ack_.transferred_bytes = htole64(transferred_bytes);

        asio::async_write(
            *socket_, asio::buffer(&ack_, sizeof(ack_)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionAck)) {
                    LOG(ERROR) << "ServerSession::sendAck failed. Error: "
                               << ec.message() << " (value: " << ec.value()
                               << "), bytes written: " << len;
                    asio::error_code close_ec;
                    socket_->close(close_ec);
                    session_mutex_.unlock();
                    return;
                }

                // The next request is read only after the ACK has been sent,
                // keeping the persistent connection strictly ordered.
                session_mutex_.unlock();
                start();
            });
    }

    void writeBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            session_mutex_.unlock();
            // READ transfers keep the original stream semantics: the client
            // observes completion after receiving the requested bytes.
            start();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
            cudaSetDevice(cuda_device);
#ifdef USE_MACA
            cudaError_t cuda_status = copyTcpCudaMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size);
#else
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
#endif
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ServerSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                session_mutex_.unlock();
                delete[] dram_buffer;
                return;  // Connection will be closed
            }
        }
#endif

        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
                    delete[] dram_buffer;
                }
#endif
                if (ec) {
                    LOG(ERROR)
                        << "ServerSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    session_mutex_.unlock();
                    return;  // Connection will be closed
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (tcpAckTraceEnabled()) {
                LOG(INFO) << "ServerSession received complete TCP WRITE"
                          << ", request_id=" << le64toh(header_.request_id)
                          << ", bytes=" << total_transferred_bytes_;
            }
            sendAck(TransferStatusEnum::COMPLETED, total_transferred_bytes_);
            return;
        }

        if (tcpAckTraceEnabled()) {
            LOG(INFO) << "ServerSession posting TCP WRITE body read"
                      << ", request_id=" << le64toh(header_.request_id)
                      << ", bytes=" << buffer_size;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

        if (tcpAckTraceEnabled()) {
            LOG(INFO) << "ServerSession starting TCP WRITE body read"
                      << ", request_id=" << le64toh(header_.request_id)
                      << ", bytes=" << buffer_size;
        }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
        }
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    // If client closed connection (EOF), this is normal - don't
                    // log
                    if (ec.value() != asio::error::eof) {
                        LOG(WARNING)
                            << "ServerSession::readBody failed. "
                            << "Attempt to read data "
                            << static_cast<void*>(addr) << " using buffer "
                            << static_cast<void*>(dram_buffer)
                            << ". Error: " << ec.message()
                            << " (value: " << ec.value() << ")";
                    }
                    session_mutex_.unlock();
                    if (cuda_device >= 0) delete[] dram_buffer;
                    return;  // Connection will be closed
                }
                if (tcpAckTraceEnabled()) {
                    LOG(INFO) << "ServerSession received TCP WRITE chunk"
                              << ", request_id=" << le64toh(header_.request_id)
                              << ", bytes=" << transferred_bytes;
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
                    cudaSetDevice(cuda_device);
#ifdef USE_MACA
                    cudaError_t cuda_status =
                        copyTcpCudaMemory(addr + total_transferred_bytes_,
                                          dram_buffer, transferred_bytes);
#else
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
#endif
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ServerSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        delete[] dram_buffer;
                        session_mutex_.unlock();
                        return;  // Connection will be closed
                    }
                    delete[] dram_buffer;
                }
#endif
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
    }
};

// Client-side session: initiates one transfer request
struct ClientSession : public std::enable_shared_from_this<ClientSession> {
    explicit ClientSession(std::shared_ptr<tcpsocket> socket,
                           std::function<void(bool)> on_complete = nullptr)
        : socket_(std::move(socket)),
          ack_timer_(socket_->get_executor()),
          on_complete_(std::move(on_complete)) {}

    std::shared_ptr<tcpsocket> socket_;
    SessionHeader header_;
    SessionAck ack_{};
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    std::function<void(TransferStatusEnum)> on_finalize_;
    asio::steady_timer ack_timer_;
    std::function<void(bool)> on_complete_;  // Callback when transfer completes
    std::mutex session_mutex_;
    bool completion_started_ = false;

    void initiate(void* buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        session_mutex_.lock();
        header_ = {};
        local_buffer_ = (char*)buffer;
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode;
        header_.version = kTcpSessionProtocolVersion;
        header_.flags = htole16(0);
        header_.magic = htole32(kTcpSessionHeaderMagic);
        header_.request_id = htole64(nextTcpRequestId());
        total_transferred_bytes_ = 0;
        if (tcpAckTraceEnabled()) {
            LOG(INFO) << "ClientSession sending TCP transfer header"
                      << ", request_id=" << le64toh(header_.request_id)
                      << ", opcode=" << static_cast<int>(header_.opcode)
                      << ", size=" << size << ", target=0x" << std::hex
                      << dest_addr << std::dec;
        }
        writeHeader();
    }

   private:
    void finish(TransferStatusEnum status, bool reusable) {
        if (completion_started_) return;
        completion_started_ = true;
        asio::error_code timer_ec;
        ack_timer_.cancel(timer_ec);

        auto self(shared_from_this());
        asio::post(socket_->get_executor(),
                   [this, self, status, reusable,
                    on_finalize = std::move(on_finalize_),
                    on_complete = std::move(on_complete_)]() {
                       if (on_finalize) on_finalize(status);
                       session_mutex_.unlock();
                       if (on_complete) on_complete(reusable);
                   });
    }

    void startAckTimer() {
        auto self(shared_from_this());
        ack_timer_.expires_after(getTcpAckTimeout());
        ack_timer_.async_wait([this, self](const asio::error_code& ec) {
            if (ec == asio::error::operation_aborted) return;
            if (ec) {
                LOG(ERROR) << "ClientSession ACK timer failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED, false);
                return;
            }
            LOG(ERROR) << "ClientSession timed out waiting for remote write ACK"
                       << ", request_id=" << le64toh(header_.request_id);
            finish(TransferStatusEnum::TIMEOUT, false);
        });
    }

    void readAck() {
        auto self(shared_from_this());
        startAckTimer();
        asio::async_read(
            *socket_, asio::buffer(&ack_, sizeof(ack_)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                // The ACK read is cancelled when the timeout or another
                // failure path has already finalized this session.
                if (completion_started_) return;
                if (ec || len != sizeof(SessionAck)) {
                    LOG(ERROR) << "ClientSession::readAck failed. Error: "
                               << ec.message() << " (value: " << ec.value()
                               << "), bytes read: " << len;
                    finish(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (tcpAckTraceEnabled()) {
                    LOG(INFO) << "ClientSession received TCP write ACK"
                              << ", request_id=" << le64toh(ack_.request_id)
                              << ", bytes=" << le64toh(ack_.transferred_bytes);
                }

                const auto magic = le32toh(ack_.magic);
                const auto version = le16toh(ack_.version);
                const auto status = le16toh(ack_.status);
                const auto request_id = le64toh(ack_.request_id);
                const auto transferred_bytes = le64toh(ack_.transferred_bytes);
                const auto expected_request_id = le64toh(header_.request_id);

                if (magic != kTcpSessionAckMagic ||
                    version != kTcpSessionProtocolVersion ||
                    request_id != expected_request_id ||
                    status !=
                        static_cast<uint16_t>(TransferStatusEnum::COMPLETED) ||
                    transferred_bytes != le64toh(header_.size)) {
                    LOG(ERROR)
                        << "ClientSession::readAck received invalid ACK"
                        << ", magic=" << magic << ", version=" << version
                        << ", status=" << status
                        << ", request_id=" << request_id
                        << ", expected_request_id=" << expected_request_id
                        << ", transferred_bytes=" << transferred_bytes
                        << ", expected_bytes=" << le64toh(header_.size);
                    finish(TransferStatusEnum::FAILED, false);
                    return;
                }

                finish(TransferStatusEnum::COMPLETED, true);
            });
    }

    void writeHeader() {
        auto self(shared_from_this());
        asio::async_write(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    LOG(ERROR)
                        << "ClientSession::writeHeader failed. Error: "
                        << ec.message() << " (value: " << ec.value() << ")"
                        << ", bytes written: " << len;
                    finish(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    writeBody();
                else
                    readBody();
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            finish(TransferStatusEnum::COMPLETED, true);
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
        }
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::readBody failed. "
                        << "Attempt to read data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    // Post entire cleanup to ensure it runs after callback
                    // returns
                    asio::post(socket_->get_executor(),
                               [this, self, dram_buffer, cuda_device,
                                on_finalize = std::move(on_finalize_),
                                on_complete = std::move(on_complete_)]() {
                                   if (on_finalize)
                                       on_finalize(TransferStatusEnum::FAILED);
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                                   if (cuda_device >= 0) delete[] dram_buffer;
#endif
                                   session_mutex_.unlock();
                                   if (on_complete) on_complete(false);
                               });
                    return;
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
                    cudaSetDevice(cuda_device);
#ifdef USE_MACA
                    cudaError_t cuda_status =
                        copyTcpCudaMemory(addr + total_transferred_bytes_,
                                          dram_buffer, transferred_bytes);
#else
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
#endif
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ClientSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        // Post entire cleanup to ensure it runs after callback
                        // returns
                        asio::post(
                            socket_->get_executor(),
                            [this, self, dram_buffer,
                             on_finalize = std::move(on_finalize_),
                             on_complete = std::move(on_complete_)]() {
                                if (on_finalize)
                                    on_finalize(TransferStatusEnum::FAILED);
                                delete[] dram_buffer;
                                session_mutex_.unlock();
                                if (on_complete) on_complete(false);
                            });
                        return;
                    }
                    delete[] dram_buffer;
                }
#endif
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
    }

    void writeBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (tcpAckTraceEnabled()) {
            LOG(INFO) << "ClientSession preparing TCP WRITE chunk"
                      << ", request_id=" << le64toh(header_.request_id)
                      << ", offset=" << total_transferred_bytes_
                      << ", bytes=" << buffer_size;
        }
        if (buffer_size == 0) {
            // Local socket completion is not remote completion. Wait for the
            // receiver's ACK before publishing Slice::SUCCESS.
            readAck();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
            cudaSetDevice(cuda_device);
#ifdef USE_MACA
            cudaError_t cuda_status = copyTcpCudaMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size);
#else
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
#endif
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ClientSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                // Post entire cleanup to ensure it runs after callback returns
                asio::post(socket_->get_executor(),
                           [this, self, dram_buffer,
                            on_finalize = std::move(on_finalize_),
                            on_complete = std::move(on_complete_)]() {
                               if (on_finalize)
                                   on_finalize(TransferStatusEnum::FAILED);
                               delete[] dram_buffer;
                               session_mutex_.unlock();
                               if (on_complete) on_complete(false);
                           });
                return;
            }
        }
#endif

        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (cuda_device >= 0) {
                    delete[] dram_buffer;
                }
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    // Post entire cleanup to ensure it runs after callback
                    // returns
                    asio::post(
                        socket_->get_executor(),
                        [this, self, on_finalize = std::move(on_finalize_),
                         on_complete = std::move(on_complete_)]() {
                            if (on_finalize)
                                on_finalize(TransferStatusEnum::FAILED);
                            session_mutex_.unlock();
                            if (on_complete) on_complete(false);
                        });
                    return;
                }
                if (tcpAckTraceEnabled()) {
                    LOG(INFO) << "ClientSession completed TCP WRITE chunk"
                              << ", request_id=" << le64toh(header_.request_id)
                              << ", bytes=" << transferred_bytes;
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }
};

struct TcpContext {
    TcpContext(short port, ValidateAddrFn validate_addr)
        : acceptor(io_context), validate_addr_(std::move(validate_addr)) {
        std::error_code ec;
        asio::ip::tcp::endpoint endpoint(asio::ip::tcp::v6(), port);

        acceptor.open(endpoint.protocol(), ec);
        if (!ec) {
            acceptor.set_option(asio::ip::v6_only(false), ec);
            if (!ec) {
                acceptor.set_option(
                    asio::ip::tcp::acceptor::reuse_address(true));
                acceptor.bind(endpoint, ec);
                if (!ec) {
                    acceptor.listen();
                    return;
                }
            }
            acceptor.close();
        }
        LOG(ERROR) << "Failed to set up IPv6 dual-stack listener: "
                   << ec.message() << " (error code: " << ec.value() << ")";
        asio::ip::tcp::endpoint endpoint_v4(asio::ip::tcp::v4(), port);
        acceptor.open(endpoint_v4.protocol());
        acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor.bind(endpoint_v4);
        acceptor.listen();
    }

    void doAccept() {
        acceptor.async_accept([this](asio::error_code ec, tcpsocket socket) {
            if (!ec) {
                asio::error_code nodelay_ec;
                socket.set_option(asio::ip::tcp::no_delay(true), nodelay_ec);
                auto socket_ptr =
                    std::make_shared<tcpsocket>(std::move(socket));
                auto session =
                    std::make_shared<ServerSession>(socket_ptr, validate_addr_);
                session->start();
            }
            doAccept();
        });
    }

    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    ValidateAddrFn validate_addr_;
};

TcpTransport::TcpTransport() : context_(nullptr), running_(false) {
    if (getenv("MC_TCP_ENABLE_CONNECTION_POOL") != nullptr) {
        std::string val(getenv("MC_TCP_ENABLE_CONNECTION_POOL"));
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) -> char { return std::tolower(c); });
        if (val == "0" || val == "false" || val == "no") {
            enable_connection_pool_ = false;
        } else {
            enable_connection_pool_ = true;
        }
    }
}

TcpTransport::~TcpTransport() {
    if (running_) {
        running_ = false;
        context_->io_context.stop();
        thread_.join();
    }

    // Clear connection pool BEFORE deleting context
    // because sockets in the pool reference io_context
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        connection_pool_.clear();
    }

    if (context_) {
        delete context_;
        context_ = nullptr;
    }

    metadata_->removeSegmentDesc(local_server_name_);
}

int TcpTransport::startHandshakeDaemon() {
    return metadata_->startHandshakeDaemon(nullptr,
                                           metadata_->localRpcMeta().rpc_port,
                                           metadata_->localRpcMeta().sockfd);
}

int TcpTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;
    int sockfd = -1;
    int tcp_port = findAvailableTcpPort(sockfd);
    if (tcp_port == 0) {
        LOG(ERROR) << "TcpTransport: unable to find available tcp port for "
                      "data transmission";
        return -1;
    }

    int ret = allocateLocalSegmentID(tcp_port);
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot allocate local segment";
        return -1;
    }

    ret = startHandshakeDaemon();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot start handshake daemon";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    close(sockfd);  // the above function has opened a socket
    LOG(INFO) << "TcpTransport: listen on port " << tcp_port;
    context_ = new TcpContext(tcp_port, [this](uint64_t addr, uint64_t size) {
        return validateAddress(addr, size);
    });
    running_ = true;
    thread_ = std::thread(&TcpTransport::worker, this);
    return 0;
}

int TcpTransport::allocateLocalSegmentID(int tcp_data_port) {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "tcp";
#else
    desc->protocol = "tcp";
#endif
    desc->tcp_data_port = tcp_data_port;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int TcpTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    buffer_desc.protocol = "tcp";
#endif
    return metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
}

int TcpTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int TcpTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int TcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto& addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status TcpTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "TcpTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status TcpTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "TcpTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "TcpTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        startTransfer(slice);
    }

    return Status::OK();
}

Status TcpTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        startTransfer(slice);
    }
    return Status::OK();
}

void TcpTransport::worker() {
    while (running_) {
        try {
            context_->doAccept();
            context_->io_context.run();
        } catch (std::exception& e) {
            LOG(ERROR) << "TcpTransport::worker encountered an exception "
                          "during doAccept/run: "
                       << e.what();
            context_->io_context.restart();
        }
    }
}

std::shared_ptr<asio::ip::tcp::socket> TcpTransport::getConnection(
    const std::string& host, uint16_t port) {
    // If connection pool is disabled, always create a new connection
    if (!enable_connection_pool_) {
        try {
            asio::ip::tcp::resolver resolver(context_->io_context);
            auto endpoint_iterator =
                resolver.resolve(host, std::to_string(port));
            auto socket_ptr =
                std::make_shared<asio::ip::tcp::socket>(context_->io_context);
            asio::connect(*socket_ptr, endpoint_iterator);
            socket_ptr->set_option(asio::ip::tcp::no_delay(true));
            return socket_ptr;
        } catch (std::exception& e) {
            LOG(ERROR)
                << "TcpTransport::getConnection failed to create connection to "
                << host << ":" << port << ". Error: " << e.what();
            return nullptr;
        }
    }

    ConnectionKey key{host, port};

    // First phase: search for available connection while holding the lock
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        // Cleanup idle and dead connections
        cleanupIdleConnections();

        auto it = connection_pool_.find(key);
        if (it != connection_pool_.end()) {
            auto& queue = it->second;

            // Find an available connection
            for (auto queue_it = queue.begin(); queue_it != queue.end();) {
                auto& entry = *queue_it;
                if (!entry->in_use) {
                    // Check if connection is still alive
                    if (entry->socket->is_open()) {
                        entry->in_use = true;
                        entry->last_used = std::chrono::steady_clock::now();
                        return entry->socket;
                    } else {
                        // Remove dead connection immediately
                        queue_it = queue.erase(queue_it);
                        continue;
                    }
                }
                ++queue_it;
            }
        }
    }

    // No available connection, create a new one (pool grows dynamically)
    // Release lock before creating new connection to avoid blocking other
    // threads during slow DNS resolution and TCP handshake
    std::shared_ptr<asio::ip::tcp::socket> new_socket;
    try {
        asio::ip::tcp::resolver resolver(context_->io_context);
        auto endpoint_iterator = resolver.resolve(host, std::to_string(port));
        new_socket =
            std::make_shared<asio::ip::tcp::socket>(context_->io_context);
        asio::connect(*new_socket, endpoint_iterator);
        new_socket->set_option(asio::ip::tcp::no_delay(true));
    } catch (std::exception& e) {
        LOG(ERROR)
            << "TcpTransport::getConnection failed to create connection to "
            << host << ":" << port << ". Error: " << e.what();
        return nullptr;
    }

    // Re-acquire lock to add the new connection to the pool
    std::shared_ptr<PooledConnection> entry;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        // Re-check if another thread already added a connection while we were
        // creating this one
        auto& queue = connection_pool_[key];
        for (auto it = queue.begin(); it != queue.end(); ++it) {
            auto& existing_entry = *it;
            if (!existing_entry->in_use && existing_entry->socket->is_open()) {
                // Another thread added an available connection, use that
                // instead and close the one we just created
                if (new_socket && new_socket->is_open()) {
                    asio::error_code ec;
                    new_socket->close(ec);
                }
                existing_entry->in_use = true;
                existing_entry->last_used = std::chrono::steady_clock::now();
                return existing_entry->socket;
            }
        }

        // No other connection available, add the one we created to the pool
        entry = std::make_shared<PooledConnection>(new_socket, host, port);
        queue.push_back(entry);
    }

    return entry->socket;
}

void TcpTransport::returnConnection(
    const std::string& host, uint16_t port,
    std::shared_ptr<asio::ip::tcp::socket> socket) {
    ConnectionKey key{host, port};

    std::lock_guard<std::mutex> lock(pool_mutex_);

    auto it = connection_pool_.find(key);
    if (it != connection_pool_.end()) {
        for (auto entry_it = it->second.begin(); entry_it != it->second.end();
             ++entry_it) {
            if ((*entry_it)->socket == socket) {
                if (socket->is_open()) {
                    (*entry_it)->in_use = false;
                    (*entry_it)->last_used = std::chrono::steady_clock::now();
                } else {
                    // Connection is dead, remove from pool
                    it->second.erase(entry_it);
                }
                return;
            }
        }
    }

    // Connection not found in pool (might be temporary), close it
    if (socket && socket->is_open()) {
        asio::error_code ec;
        socket->close(ec);
    }
}

void TcpTransport::cleanupIdleConnections() {
    auto now = std::chrono::steady_clock::now();

    for (auto it = connection_pool_.begin(); it != connection_pool_.end();) {
        auto& queue = it->second;

        for (auto entry_it = queue.begin(); entry_it != queue.end();) {
            auto& entry = *entry_it;
            if (!entry->in_use) {
                auto idle_duration =
                    std::chrono::duration_cast<std::chrono::seconds>(
                        now - entry->last_used)
                        .count();
                if (idle_duration > kConnectionIdleTimeout.count()) {
                    if (entry->socket && entry->socket->is_open()) {
                        asio::error_code ec;
                        entry->socket->close(ec);
                    }
                    entry_it = queue.erase(entry_it);
                    continue;
                }
            }
            ++entry_it;
        }

        if (queue.empty()) {
            it = connection_pool_.erase(it);
        } else {
            ++it;
        }
    }
}

bool TcpTransport::validateAddress(uint64_t addr, uint64_t size) const {
    if (size == 0) return false;
    if (addr + size < addr) return false;

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc) return false;

    for (const auto& buffer : desc->buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}

void TcpTransport::startTransfer(Slice* slice) {
    auto desc = metadata_->getSegmentDescByID(slice->target_id);
    if (!desc) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get segment "
                      "description for target_id: "
                   << slice->target_id;
        slice->markFailed();
        return;
    }

    TransferMetadata::RpcMetaDesc meta_entry;
    if (metadata_->getRpcMetaEntry(desc->name, meta_entry)) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get RPC meta "
                      "entry for segment name: "
                   << desc->name;
        slice->markFailed();
        return;
    }

    // Get connection from pool
    auto socket =
        getConnection(meta_entry.ip_or_host_name, desc->tcp_data_port);
    if (!socket) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get connection to "
                   << meta_entry.ip_or_host_name << ":" << desc->tcp_data_port;
        slice->markFailed();
        return;
    }

    if (tcpAckTraceEnabled()) {
        LOG(INFO) << "TcpTransport::startTransfer target="
                  << meta_entry.ip_or_host_name << ":" << desc->tcp_data_port
                  << ", source=" << static_cast<void*>(slice->source_addr)
                  << ", target_address=0x" << std::hex << slice->tcp.dest_addr
                  << std::dec << ", size=" << slice->length
                  << ", opcode=" << static_cast<int>(slice->opcode);
    }

    try {
        auto session = std::make_shared<ClientSession>(socket);

        session->on_finalize_ = [slice](TransferStatusEnum status) {
            if (status == TransferStatusEnum::COMPLETED)
                slice->markSuccess();
            else
                slice->markFailed();
        };

        // Return connection to pool when transfer completes, or close if
        // disabled
        if (enable_connection_pool_) {
            session->on_complete_ = [this, host = meta_entry.ip_or_host_name,
                                     port = desc->tcp_data_port,
                                     socket](bool reusable) {
                if (!reusable && socket && socket->is_open()) {
                    asio::error_code ec;
                    socket->close(ec);
                }
                returnConnection(host, port, socket);
            };
        } else {
            session->on_complete_ = [socket](bool) {
                // Close connection immediately after transfer
                if (socket && socket->is_open()) {
                    asio::error_code ec;
                    socket->close(ec);
                }
            };
        }

        session->initiate(slice->source_addr, slice->tcp.dest_addr,
                          slice->length, slice->opcode);
    } catch (std::exception& e) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an exception. "
                      "Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id
                   << ". Exception: " << e.what();
        // On exception, always close the socket and remove from pool if present
        // Don't return it to the pool as it may be in an inconsistent state
        if (socket && socket->is_open()) {
            asio::error_code ec;
            socket->close(ec);
        }
        if (enable_connection_pool_) {
            // Remove the connection from pool if it was pooled
            ConnectionKey key{meta_entry.ip_or_host_name,
                              static_cast<uint16_t>(desc->tcp_data_port)};
            std::lock_guard<std::mutex> lock(pool_mutex_);
            auto it = connection_pool_.find(key);
            if (it != connection_pool_.end()) {
                auto& queue = it->second;
                for (auto queue_it = queue.begin(); queue_it != queue.end();
                     ++queue_it) {
                    if ((*queue_it)->socket == socket) {
                        queue.erase(queue_it);
                        break;
                    }
                }
                if (queue.empty()) {
                    connection_pool_.erase(it);
                }
            }
        }
        slice->markFailed();
    }
}

}  // namespace mooncake
