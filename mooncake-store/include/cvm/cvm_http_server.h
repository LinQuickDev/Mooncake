#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <ylt/coro_http/coro_http_server.hpp>

#include "types.h"

namespace mooncake {
namespace cvm {

// CVM 三模块之一：外部 HTTP 接口（cvmhttpserver）。
//
// 提供只读 HTTP API，按 etcd 中的视图快照路径读取快照并返回 JSON，供网页 /
// 外部客户端展示。视图快照路径由 CvmController 定期推送；未推送时使用基于
// cluster_namespace 构造的默认快照路径（见 cvm_keys.h）。
class CvmHttpServer {
   public:
    struct Config {
        std::string host = "0.0.0.0";
        uint16_t port = 0;
        std::string cluster_namespace;
    };

    explicit CvmHttpServer(Config config);
    ~CvmHttpServer();

    CvmHttpServer(const CvmHttpServer&) = delete;
    CvmHttpServer& operator=(const CvmHttpServer&) = delete;

    ErrorCode Start();
    void Stop();

    // 供 CvmController 推送最新视图快照路径（etcd key）。
    void SetKvViewSnapshotKey(const std::string& key);
    void SetSegmentViewSnapshotKey(const std::string& key);

    // 按当前路径读取快照并返回 JSON 字符串；路径为空或读取失败时返回空串。
    std::string GetKvViewJson() const;
    std::string GetSegmentViewJson() const;

   private:
    void InitRoutes();
    std::string ReadSnapshot(const std::string& key) const;

    Config config_;
    std::unique_ptr<coro_http::coro_http_server> server_;

    mutable std::mutex path_mutex_;
    std::string kv_view_snapshot_key_;
    std::string segment_view_snapshot_key_;

    std::atomic<bool> running_{false};
};

}  // namespace cvm
}  // namespace mooncake
