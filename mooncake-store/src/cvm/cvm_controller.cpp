#include "cvm/cvm_controller.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_http_server.h"
#include "cvm/cvm_keys.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "etcd_helper.h"

namespace mooncake {
namespace cvm {

namespace {

constexpr int kWatchEventBroken = 2;
constexpr int kWatchStopTimeoutMs = 1000;
constexpr int kKeepAliveReadyTimeoutMs = 1000;

}  // namespace

CvmController::CvmController(Config config) : config_(std::move(config)) {
    current_role_.store(config_.role);
}

CvmController::~CvmController() { Stop(); }

void CvmController::SetDelegate(CvmServiceDelegate* delegate) {
    delegate_ = delegate;
}

ErrorCode CvmController::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    LOG(INFO) << "CvmController::Start begin: cluster_namespace="
              << config_.cluster_namespace << ", master_id="
              << config_.master_id << ", address=" << config_.address
              << ", role=" << static_cast<int32_t>(config_.role)
              << ", registration_lease_ttl_sec="
              << config_.registration_lease_ttl_sec << ", http_host="
              << config_.http_host << ", http_port=" << config_.http_port
              << ", sync_interval_ms=" << config_.sync_interval.count();

    // NOTE: the etcd client is a process-global singleton (EtcdHelper); the
    // embedding master is responsible for connecting it before Start().
    ErrorCode err = EtcdHelper::GrantLease(config_.registration_lease_ttl_sec,
                                           lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start GrantLease failed: err=" << err
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", master_id=" << config_.master_id
                   << ", registration_lease_ttl_sec="
                   << config_.registration_lease_ttl_sec
                   << " (etcd client may not be connected yet)";
        return err;
    }
    LOG(INFO) << "CvmController::Start GrantLease ok: lease_id=" << lease_id_;

    MasterRegistration reg;
    reg.master_id = config_.master_id;
    reg.address = config_.address;
    reg.role = static_cast<int32_t>(config_.role);
    reg.registered_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    LOG(INFO) << "CvmController::Start registering master: master_id="
              << reg.master_id << ", address=" << reg.address << ", role="
              << reg.role << ", cluster_namespace=" << config_.cluster_namespace
              << ", lease_id=" << lease_id_;
    err = EtcdViewStore::RegisterMaster(config_.cluster_namespace, reg,
                                        lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start RegisterMaster failed: err=" << err
                   << ", master_id=" << reg.master_id << ", address="
                   << reg.address << ", role=" << reg.role
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", lease_id=" << lease_id_;
        (void)EtcdHelper::RevokeLease(lease_id_);
        return err;
    }
    LOG(INFO) << "CvmController::Start RegisterMaster ok: master_id="
              << reg.master_id;

    err = RefreshKvView();
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController initial RefreshKvView failed: " << err
                     << ", cluster_namespace=" << config_.cluster_namespace;
    } else {
        LOG(INFO) << "CvmController initial RefreshKvView ok";
    }

    watch_state_ = std::make_unique<WatchState>();
    masters_watch_state_ = std::make_unique<WatchState>();
    running_.store(true);

    keepalive_thread_ = std::thread([this]() { KeepaliveLoop(); });
    watch_thread_ = std::thread([this]() { WatchLoop(); });
    masters_watch_thread_ = std::thread([this]() { MastersWatchLoop(); });
    sync_thread_ = std::thread([this]() { SyncLoop(); });
    membership_thread_ = std::thread([this]() { MembershipLoop(); });
    LOG(INFO) << "CvmController::Start started "
                 "keepalive/watch/masters_watch/sync/membership threads";

    if (config_.http_port != 0) {
        CvmHttpServer::Config http_cfg;
        http_cfg.host = config_.http_host;
        http_cfg.port = config_.http_port;
        http_cfg.cluster_namespace = config_.cluster_namespace;
        LOG(INFO) << "CvmController::Start starting CvmHttpServer: host="
                  << http_cfg.host << ", port=" << http_cfg.port;
        http_server_ = std::make_unique<CvmHttpServer>(http_cfg);
        err = http_server_->Start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "CvmController start http server failed: " << err
                       << ", host=" << http_cfg.host << ", port="
                       << http_cfg.port;
            http_server_.reset();
        } else {
            LOG(INFO) << "CvmController CvmHttpServer started";
        }
    }

    LOG(INFO) << "CvmController::Start ok";
    return ErrorCode::OK;
}

void CvmController::Stop() {
    if (!running_.load() && !watch_thread_.joinable() &&
        !masters_watch_thread_.joinable() && !keepalive_thread_.joinable() &&
        !sync_thread_.joinable() && !membership_thread_.joinable()) {
        return;
    }

    running_.store(false);

    CancelWatchAndWait();

    if (watch_state_) {
        std::lock_guard<std::mutex> lock(watch_state_->mutex);
        watch_state_->cv.notify_all();
    }
    if (masters_watch_state_) {
        std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
        masters_watch_state_->cv.notify_all();
    }

    if (keepalive_thread_.joinable()) {
        (void)EtcdHelper::CancelKeepAlive(lease_id_);
        keepalive_thread_.join();
    }
    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }
    if (masters_watch_thread_.joinable()) {
        masters_watch_thread_.join();
    }
    if (sync_thread_.joinable()) {
        sync_cv_.notify_all();
        sync_thread_.join();
    }
    if (membership_thread_.joinable()) {
        membership_cv_.notify_all();
        membership_thread_.join();
    }

    if (http_server_) {
        http_server_->Stop();
        http_server_.reset();
    }

    (void)EtcdHelper::RevokeLease(lease_id_);
    watch_state_.reset();
    masters_watch_state_.reset();
}

bool CvmController::OwnsSlot(uint16_t slot) const {
    SharedMutexLocker locker(&view_mutex_, shared_lock);
    auto it = kv_view_.find(slot);
    if (it == kv_view_.end()) {
        return false;
    }
    return it->second.primary_master_id == config_.master_id &&
           it->second.state == static_cast<int32_t>(SlotState::kStable);
}

std::vector<SlotOwner> CvmController::GetKvView() const {
    SharedMutexLocker locker(&view_mutex_, shared_lock);
    std::vector<SlotOwner> out;
    out.reserve(kv_view_.size());
    for (const auto& entry : kv_view_) {
        out.push_back(entry.second);
    }
    return out;
}

ViewVersionId CvmController::GetKvViewVersion() const {
    SharedMutexLocker locker(&view_mutex_, shared_lock);
    return kv_view_version_;
}

ErrorCode CvmController::SyncOnce() {
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::BuildAndSaveKvViewSnapshot(
        config_.cluster_namespace, version);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController build kv view snapshot failed: " << err;
        return err;
    }

    err = EtcdViewStore::BuildAndSaveSegmentViewSnapshot(
        config_.cluster_namespace, version);
    if (err != ErrorCode::OK) {
        // segment 视图为预留，失败不阻断 kv 视图路径同步。
        LOG(WARNING) << "CvmController build segment view snapshot failed: "
                     << err;
    }

    PushViewPaths();
    return ErrorCode::OK;
}

void CvmController::PushViewPaths() {
    const std::string kv_key = KvViewSnapshotKey(config_.cluster_namespace);
    const std::string segment_key =
        SegmentViewSnapshotKey(config_.cluster_namespace);

    view_paths_["kv_view"] = kv_key;
    view_paths_["segment_view"] = segment_key;

    if (http_server_) {
        http_server_->SetKvViewSnapshotKey(kv_key);
        http_server_->SetSegmentViewSnapshotKey(segment_key);
    }
    LOG(INFO) << "CvmController pushed view paths: kv_view=" << kv_key
              << " segment_view=" << segment_key;
}

ErrorCode CvmController::RefreshKvView() {
    std::vector<SlotOwner> owners;
    ViewVersionId version = 0;
    ErrorCode err =
        EtcdViewStore::LoadAllSlotOwners(config_.cluster_namespace, owners,
                                         version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::unordered_map<uint16_t, SlotOwner> next;
    next.reserve(owners.size());
    for (auto& owner : owners) {
        next.emplace(owner.slot, std::move(owner));
    }

    {
        SharedMutexLocker locker(&view_mutex_);
        kv_view_ = std::move(next);
        kv_view_version_ = version;
    }
    return ErrorCode::OK;
}

void CvmController::CancelWatchAndWait() {
    if (watch_armed_.exchange(false)) {
        (void)EtcdViewStore::CancelWatchKvView(config_.cluster_namespace);
        (void)EtcdViewStore::WaitWatchKvViewStopped(config_.cluster_namespace,
                                                    kWatchStopTimeoutMs);
    }
    if (masters_watch_armed_.exchange(false)) {
        (void)EtcdViewStore::CancelWatchMasters(config_.cluster_namespace);
        (void)EtcdViewStore::WaitWatchMastersStopped(config_.cluster_namespace,
                                                     kWatchStopTimeoutMs);
    }
}

void CvmController::WatchLoop() {
    while (running_.load()) {
        ViewVersionId version_before = 0;
        {
            SharedMutexLocker locker(&view_mutex_, shared_lock);
            version_before = kv_view_version_;
        }

        ErrorCode err = RefreshKvView();
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController WatchLoop refresh failed: " << err;
        }

        ViewVersionId version_after = 0;
        {
            SharedMutexLocker locker(&view_mutex_, shared_lock);
            version_after = kv_view_version_;
        }
        // slot 所有权视图变化：通知 delegate，让 standby 即使角色不变也能
        // 重绑回放源（技术债 2 修复）。
        if (version_after != version_before && delegate_) {
            delegate_->OnKvViewChanged();
        }

        if (!running_.load()) {
            break;
        }

        CancelWatchAndWait();

        {
            std::lock_guard<std::mutex> lock(watch_state_->mutex);
            watch_state_->dirty = false;
            watch_state_->broken = false;
        }

        ViewVersionId start_version = 0;
        {
            SharedMutexLocker locker(&view_mutex_, shared_lock);
            start_version = kv_view_version_;
        }

        err = EtcdViewStore::WatchKvView(config_.cluster_namespace,
                                         start_version, watch_state_.get(),
                                         &CvmController::WatchCallback);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController arm watch failed: " << err;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        watch_armed_.store(true);

        std::unique_lock<std::mutex> lock(watch_state_->mutex);
        watch_state_->cv.wait(lock, [this] {
            return watch_state_->dirty || watch_state_->broken ||
                   !running_.load();
        });
    }
}

void CvmController::MastersWatchLoop() {
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
            masters_watch_state_->dirty = false;
            masters_watch_state_->broken = false;
        }

        // start_revision=0 → 从当前开始 watch，只关注未来的成员增删（lease
        // 过期删除 / 新节点注册）。
        ErrorCode err = EtcdViewStore::WatchMasters(
            config_.cluster_namespace, /*start_revision=*/0,
            masters_watch_state_.get(), &CvmController::WatchCallback);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController arm masters watch failed: " << err;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        masters_watch_armed_.store(true);

        std::unique_lock<std::mutex> lock(masters_watch_state_->mutex);
        masters_watch_state_->cv.wait(lock, [this] {
            return masters_watch_state_->dirty ||
                   masters_watch_state_->broken || !running_.load();
        });

        // 成员增删（lease 过期）→ 立即重算角色，并唤醒 sync 线程立即重算
        // slot 快照，使 failover 不再等待轮询周期。
        ReconcileRole();
        sync_cv_.notify_all();
    }
}

void CvmController::KeepaliveLoop() {
    (void)EtcdHelper::WaitKeepAliveReady(lease_id_, kKeepAliveReadyTimeoutMs);
    ErrorCode rc = EtcdHelper::KeepAlive(lease_id_);
    if (rc == ErrorCode::ETCD_OPERATION_ERROR) {
        LOG(WARNING) << "CvmController keepalive error: " << rc;
    }
}

void CvmController::SyncLoop() {
    while (running_.load()) {
        (void)SyncOnce();

        std::unique_lock<std::mutex> lock(sync_mutex_);
        sync_cv_.wait_for(lock, config_.sync_interval,
                          [this] { return !running_.load(); });
    }
}

bool CvmController::LoadRankedMembers(
    std::vector<MasterRegistration>& out) {
    std::vector<MasterRegistration> masters;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadAllMasters(config_.cluster_namespace,
                                                  masters, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController load masters failed: " << err
                     << ", master_id=" << config_.master_id;
        return false;
    }

    // 收集所有存活 master（含本机）作为候选，按「先到先得」排序。
    out.clear();
    out.reserve(masters.size());
    for (const auto& m : masters) {
        if (!m.master_id.empty()) {
            out.push_back(m);
        }
    }

    std::sort(out.begin(), out.end(),
              [](const MasterRegistration& a, const MasterRegistration& b) {
                  if (a.registered_at_ms != b.registered_at_ms) {
                      return a.registered_at_ms < b.registered_at_ms;
                  }
                  return a.master_id < b.master_id;
              });
    return true;
}

MasterRole CvmController::ComputeDesiredRole() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members)) {
        return current_role_.load();
    }

    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            return i < config_.submaster_count ? MasterRole::kPrimary
                                               : MasterRole::kStandby;
        }
    }

    // 本机不在成员集（未注册/异常）：保守保持当前角色。
    return current_role_.load();
}

std::string CvmController::GetPrimaryAddress() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return "";
    }
    // 排名第一的成员是集群中最早的 primary。若本机就是它，则无需（也不能）
    // 把自己当作回放源，返回空字符串让调用方保持纯 standby。
    if (members.front().master_id == config_.master_id) {
        return "";
    }
    return members.front().address;
}

std::vector<MasterRegistration> CvmController::GetPrimaryPeers() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return {};
    }
    std::vector<MasterRegistration> peers;
    peers.reserve(std::min<size_t>(members.size(), config_.submaster_count));
    for (size_t i = 0; i < members.size() && i < config_.submaster_count; ++i) {
        if (members[i].master_id == config_.master_id) {
            continue;  // 本机不作为自己的回放源。
        }
        peers.push_back(members[i]);
    }
    return peers;
}

std::vector<MasterRegistration> CvmController::GetBindingSources() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return {};
    }

    const size_t primary_count =
        std::min<size_t>(members.size(), config_.submaster_count);

    // 本机在「先到先得」排序列表中的位置。
    size_t my_index = members.size();
    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            my_index = i;
            break;
        }
    }
    // 未注册 / 本机已是 primary：不作为 standby 回放。
    if (my_index == members.size() || my_index < primary_count) {
        return {};
    }

    const size_t standby_count = members.size() - primary_count;
    if (standby_count == 0) {
        return {};
    }
    const size_t standby_rank = my_index - primary_count;

    // 本 standby 负责的 slot 区间 [start, end)，与其它 standby 均分 16384。
    const uint16_t start = static_cast<uint16_t>(standby_rank * kSlotCount /
                                                 standby_count);
    const uint16_t end = static_cast<uint16_t>((standby_rank + 1) * kSlotCount /
                                               standby_count);

    // 求出负责区间内每个 slot 的 primary owner（仅 kStable、非本机）。
    std::set<std::string> owner_ids;
    {
        SharedMutexLocker locker(&view_mutex_, shared_lock);
        for (uint16_t slot = start; slot < end; ++slot) {
            auto it = kv_view_.find(slot);
            if (it == kv_view_.end()) {
                continue;
            }
            if (it->second.state != static_cast<int32_t>(SlotState::kStable)) {
                continue;
            }
            const std::string& owner = it->second.primary_master_id;
            if (!owner.empty() && owner != config_.master_id) {
                owner_ids.insert(owner);
            }
        }
    }

    // 映射 owner master_id -> MasterRegistration（含 address）。
    std::vector<MasterRegistration> sources;
    sources.reserve(owner_ids.size());
    for (const auto& m : members) {
        if (owner_ids.count(m.master_id)) {
            sources.push_back(m);
        }
    }
    return sources;
}

void CvmController::ReconcileRole() {
    const MasterRole desired = ComputeDesiredRole();
    MasterRole current = current_role_.load();
    if (desired == current) {
        return;
    }
    // CAS：membership_thread_ 与 masters_watch_thread_ 并发调用时，仅一个
    // 线程执行角色迁移与通知，避免重复 OnRoleChanged。
    if (!current_role_.compare_exchange_strong(current, desired)) {
        return;
    }
    LOG(INFO) << "CvmController role decision: master_id="
              << config_.master_id << ", current="
              << static_cast<int32_t>(current)
              << ", desired=" << static_cast<int32_t>(desired)
              << ", submaster_count=" << config_.submaster_count;
    if (delegate_) {
        delegate_->OnRoleChanged(desired);
    }
}

void CvmController::MembershipLoop() {
    while (running_.load()) {
        ReconcileRole();

        std::unique_lock<std::mutex> lock(membership_mutex_);
        membership_cv_.wait_for(lock, config_.sync_interval,
                                [this] { return !running_.load(); });
    }
}

void CvmController::WatchCallback(void* ctx, const char* /*key*/,
                                  size_t /*key_size*/, const char* /*value*/,
                                  size_t /*value_size*/, int event_type,
                                  int64_t /*mod_revision*/) {
    auto* state = static_cast<WatchState*>(ctx);
    if (state == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->dirty = true;
        if (event_type == kWatchEventBroken) {
            state->broken = true;
        }
    }
    state->cv.notify_all();
}

}  // namespace cvm
}  // namespace mooncake
