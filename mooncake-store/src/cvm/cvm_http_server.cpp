#include "cvm/cvm_http_server.h"

#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_keys.h"
#include "etcd_helper.h"

namespace mooncake {
namespace cvm {

CvmHttpServer::CvmHttpServer(Config config)
    : config_(std::move(config)),
      server_(std::make_unique<coro_http::coro_http_server>(4, config_.port)),
      kv_view_snapshot_key_(KvViewSnapshotKey(config_.cluster_namespace)),
      segment_view_snapshot_key_(
          SegmentViewSnapshotKey(config_.cluster_namespace)) {
    InitRoutes();
}

CvmHttpServer::~CvmHttpServer() { Stop(); }

void CvmHttpServer::InitRoutes() {
    using namespace coro_http;

    server_->set_http_handler<GET>(
        "/kv_view", [this](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            std::string json = GetKvViewJson();
            if (json.empty()) {
                resp.set_status_and_content(status_type::not_found,
                                            "kv view snapshot not found");
                return;
            }
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, json);
        });

    server_->set_http_handler<GET>(
        "/segment_view",
        [this](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            std::string json = GetSegmentViewJson();
            if (json.empty()) {
                resp.set_status_and_content(
                    status_type::not_found, "segment view snapshot not found");
                return;
            }
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, json);
        });

    server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

ErrorCode CvmHttpServer::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    // async_start() binds synchronously and returns a future that is already
    // resolved (hasResult()) when the bind failed. Mirrors
    // HttpMetadataServer::start().
    auto ec = server_->async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "CvmHttpServer failed to start on " << config_.host << ":"
                   << config_.port;
        return ErrorCode::RPC_FAIL;
    }
    running_.store(true);
    LOG(INFO) << "CvmHttpServer started on " << config_.host << ":"
              << config_.port;
    return ErrorCode::OK;
}

void CvmHttpServer::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    server_->stop();
    LOG(INFO) << "CvmHttpServer stopped";
}

void CvmHttpServer::SetKvViewSnapshotKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(path_mutex_);
    kv_view_snapshot_key_ = key;
}

void CvmHttpServer::SetSegmentViewSnapshotKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(path_mutex_);
    segment_view_snapshot_key_ = key;
}

std::string CvmHttpServer::GetKvViewJson() const {
    std::string key;
    {
        std::lock_guard<std::mutex> lock(path_mutex_);
        key = kv_view_snapshot_key_;
    }
    return ReadSnapshot(key);
}

std::string CvmHttpServer::GetSegmentViewJson() const {
    std::string key;
    {
        std::lock_guard<std::mutex> lock(path_mutex_);
        key = segment_view_snapshot_key_;
    }
    return ReadSnapshot(key);
}

std::string CvmHttpServer::ReadSnapshot(const std::string& key) const {
    if (key.empty()) {
        return "";
    }
    std::string value;
    EtcdRevisionId revision = 0;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer read snapshot failed: " << key
                     << " err=" << err;
        return "";
    }
    return value;
}

}  // namespace cvm
}  // namespace mooncake
