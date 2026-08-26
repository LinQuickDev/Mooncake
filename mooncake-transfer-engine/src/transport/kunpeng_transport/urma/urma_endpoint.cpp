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

#include <glog/logging.h>
#include <cassert>
#include <cstddef>
#include "config.h"
#include "transport/kunpeng_transport/urma/urma_endpoint.h"

namespace mooncake {
static int isNullEid(urma_eid_t* eid) {
    for (int i = 0; i < URMA_EID_SIZE; ++i) {
        if (eid->raw[i] != 0) return 0;
    }
    return 1;
}

UrmaContext::UrmaContext(UbTransport& engine, std::string device_name,
                         int max_endpoints)
    : UbContext(engine, std::move(device_name), max_endpoints),
      next_jfce_index_(0),
      next_jfce_vector_index_(0),
      next_jfc_list_index_(0),
      next_jfr_list_index_(0) {}

UrmaContext::~UrmaContext() {
    auto thisString = toString();
    worker_pool_.reset();
    LOG(INFO) << "destroy worker pool done.";
    if (endpoint_store_) {
        endpoint_store_->destroy();
    }
    LOG(INFO) << "destroy endpoint store done.";
    if (urma_context_) deconstruct();
    LOG(WARNING) << "finished destroy context : " << thisString;
}

std::string UrmaContext::toString() {
    std::ostringstream ss;
    ss << "UrmaContext:{device_name : " << device_name_
       << " ,max_endpoints : " << max_endpoints_
       << " ,async_fd : " << getAsyncFd() << " }";
    return ss.str();
}

int UrmaContext::getAsyncFd() { return urma_context_->async_fd; }

int UrmaContext::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
    return worker_pool_->submitPostSend(slice_list);
}

int UrmaContext::construct(GlobalConfig& config) {
    size_t num_jfc_list = config.num_jfc_per_ctx;
    size_t num_jfces = config.num_jfce_per_ctx;
    int eid_index = config.eid_index;
    size_t max_jfc_e = config.max_jfc_e;
    // urma: Here, num_jfc_list and num_jfces use the same value,
    // meaning one JFC is bound to one JFCE.
    // max_jfc_e uses the default value DEFAULT_DEPTH.
    if (openDevice(device_name_, port_, eid_index)) {
        LOG(ERROR) << "Failed to open device : " << device_name_
                   << " with EID index : " << eid_index;
        return ERR_CONTEXT;
    }

    num_JFCE_ = num_jfces;
    jfce_ = new urma_jfce_t*[num_JFCE_];
    for (size_t i = 0; i < num_jfces; ++i) {
        jfce_[i] = urma_create_jfce(urma_context_);
        if (!jfce_[i]) {
            PLOG(ERROR) << "Failed to create jetty for completion events queue "
                           "on device"
                        << device_name_;
            return ERR_CONTEXT;
        }
    }
    LOG(INFO) << "create jfce done";
    if (joinNonblockingPollList(event_fd_, urma_context_->async_fd)) {
        LOG(ERROR) << "Failed to register context async fd to epoll";
        close(event_fd_);
        return ERR_CONTEXT;
    }

    LOG(INFO) << "join blocking list done";
    jfc_list_.resize(num_jfc_list);
    urma_jfc_cfg_t jfc_s_cfg[num_jfc_list] = {};
    for (size_t i = 0; i < num_jfc_list; ++i) {
        jfc_s_cfg[i].depth = max_jfc_e;
        jfc_s_cfg[i].jfce = NULL;
        jfc_s_cfg[i].user_ctx = (uint64_t)&jfc_list_[i].outstanding;
        auto jfc = urma_create_jfc(urma_context_, &jfc_s_cfg[i]);
        if (!jfc) {
            PLOG(ERROR) << "Failed to create jetty for completion queue(jfs)";
            close(event_fd_);
            return ERR_CONTEXT;
        }
        jfc_list_[i].native = jfc;
        LOG(INFO) << "create jfc(send) done, jfc id : " << jfc->jfc_id.id;
    }
    urma_jfc_cfg_t jfc_r_cfg = {};
    jfc_r_cfg.depth = max_jfc_e;
    jfc_r_cfg.jfce = NULL;
    jfc_r_cfg.user_ctx = 0;
    jfc_r_list_.resize(num_jfc_list);
    for (size_t i = 0; i < num_jfc_list; ++i) {
        auto jfc = urma_create_jfc(urma_context_, &jfc_r_cfg);
        if (!jfc) {
            PLOG(ERROR) << "Failed to create jetty for completion queue(jfr)";
            close(event_fd_);
            return ERR_CONTEXT;
        }
        jfc_r_list_[i] = jfc;
        LOG(INFO) << "create jfc(send) done, jfc id : " << jfc->jfc_id.id;
    }
    jfr_list_.resize(num_jfc_list);
    urma_jfr_cfg_t jfr_cfg[num_jfc_list] = {};
    /* one-side write/read, jfr no used */
    for (size_t i = 0; i < num_jfc_list; ++i) {
        jfr_cfg[i].depth = 2048;
        jfr_cfg[i].flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
        jfr_cfg[i].flag.bs.lock_free = 0;
        jfr_cfg[i].trans_mode = URMA_TM_RC;
        jfr_cfg[i].min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg[i].token_value = urma_token;
        jfr_cfg[i].id = 0;
        jfr_cfg[i].max_sge = 2;
        jfr_cfg[i].jfc = jfc_r_list_[i];
        auto jfr = urma_create_jfr(urma_context_, &jfr_cfg[i]);
        if (!jfr) {
            PLOG(ERROR) << "Failed to create jetty for receive queue";
            close(event_fd_);
            return ERR_CONTEXT;
        }
        jfr_list_[i].native = jfr;
    }
    LOG(INFO) << "create jfr done";
    LOG(INFO) << "URMA device: " << urma_context_->dev->name
              << ", EID: (EID_Index " << eid_index_ << ") " << eid();

    LOG(INFO) << "context_ == NULL ? "
              << (urma_context_ == nullptr ? "TRUE" : "FALSE");
    worker_pool_ = std::make_shared<UbWorkerPool>(*this, socketId());
    LOG(INFO) << "create workerpool done";
    return 0;
}

int UrmaContext::deconstruct() {
    for (auto& entry : seg_region_list_) {
        int ret = urma_unregister_seg(entry.first);
        if (ret) {
            PLOG(ERROR) << "Failed to unregister segment";
        }
    }
    seg_region_list_.clear();

    for (auto& seg : imported_seg_list_) {
        int ret = urma_unimport_seg(seg);
        if (ret) {
            PLOG(ERROR) << "Failed to unimport segment";
        }
    }
    imported_seg_list_.clear();

    for (auto& seg : remote_seg_list_) {
        free(seg);
    }
    remote_seg_list_.clear();

    import_tseg_map.clear();

    for (size_t i = 0; i < jfr_list_.size(); i++) {
        if (!jfr_list_[i].native) continue;

        int ret = urma_delete_jfr(jfr_list_[i].native);
        if (ret) {
            PLOG(ERROR) << "Failed to destroy jetty for receive queue";
        }
    }
    jfr_list_.clear();

    for (size_t i = 0; i < jfc_list_.size(); ++i) {
        if (!jfc_list_[i].native) continue;

        int ret = urma_delete_jfc(jfc_list_[i].native);
        if (ret) {
            PLOG(ERROR) << "Failed to destroy jetty for completion queue";
        }
    }
    jfc_list_.clear();

    for (size_t i = 0; i < jfc_r_list_.size(); i++) {
        if (!jfc_r_list_[i]) continue;

        int ret = urma_delete_jfc(jfc_r_list_[i]);
        if (ret) {
            PLOG(ERROR) << "Failed to destroy jetty for completion queue";
        }
    }
    jfc_r_list_.clear();

    if (event_fd_ >= 0) {
        if (close(event_fd_)) LOG(ERROR) << "Failed to close epoll fd";
        event_fd_ = -1;
    }

    if (jfce_) {
        for (size_t i = 0; i < num_JFCE_; ++i)
            if (jfce_[i])
                if (urma_delete_jfce(jfce_[i]))
                    LOG(ERROR)
                        << "Failed to destroy jetty for completion event queue";
        delete[] jfce_;
        jfce_ = nullptr;
    }

    if (urma_context_) {
        if (urma_delete_context(urma_context_))
            PLOG(ERROR) << "Failed to close device context";
        urma_context_ = nullptr;
    }

    urma_uninit();
    return 0;
}

urma_target_seg_t* UrmaContext::seg(uint64_t addr) {
    RWSpinlock::ReadGuard guard(seg_region_lock_);
    for (auto iter = seg_region_list_.begin(); iter != seg_region_list_.end();
         ++iter)
        if ((*iter).first->seg.ubva.va <= addr &&
            addr < (*iter).first->seg.ubva.va + (*iter).second)
            return (*iter).first;

    LOG(ERROR) << "Address " << addr << " seg not found for " << deviceName();
    return 0;
}

int UrmaContext::buildLocalBufferDesc(uint64_t addr,
                                      UbTransport::BufferDesc& buffer_desc) {
    auto str = serializeBinaryData(&seg(addr)->seg, sizeof(urma_seg_t));
    auto index = local_tseg_list().size() - 1;
    buffer_desc.tseg.push_back(str);
    buffer_desc.l_seg_index.push_back(index);
    return 0;
}

void* UrmaContext::localSegWithIndex(unsigned value) {
    return local_tseg_list_.at(value);
}

int UrmaContext::registerMemoryRegion(uint64_t va, size_t length) {
    if (length > (size_t)globalConfig().max_seg_size) {
        PLOG(WARNING) << "The buffer length exceeds device max_seg_size, "
                      << "shrink it to " << globalConfig().max_seg_size;
        length = (size_t)globalConfig().max_seg_size;
    }
    LOG(INFO) << "Register memory region " << va << " length " << length;
    urma_reg_seg_flag_t flag = {};
    flag.bs.token_policy = URMA_TOKEN_NONE;
    flag.bs.cacheable = URMA_NON_CACHEABLE;
    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    flag.bs.token_id_valid = 0;
    flag.bs.reserved = 0;

    urma_seg_cfg_t seg_cfg = {
        .va = va,
        .len = length,
        .token_id = NULL,
        .token_value = urma_token,
        .flag = flag,
        .user_ctx = (uintptr_t)NULL,
        .iova = 0,
    };
    urma_target_seg_t* seg = urma_register_seg(urma_context_, &seg_cfg);
    if (!seg) {
        PLOG(ERROR) << "Failed to register segment " << seg_cfg.va;
        return ERR_CONTEXT;
    }
    LOG(INFO) << "Local seg token id : " << seg->seg.token_id;
    local_tseg_list_.push_back(seg);

    RWSpinlock::WriteGuard guard(seg_region_lock_);
    seg_region_list_.emplace_back(seg, length);
    return 0;
}

int UrmaContext::unregisterMemoryRegion(uint64_t addr) {
    RWSpinlock::WriteGuard guard(seg_region_lock_);
    bool has_removed;
    do {
        has_removed = false;
        for (auto iter = seg_region_list_.begin();
             iter != seg_region_list_.end(); ++iter) {
            if ((*iter).first->seg.ubva.va <= addr &&
                addr < (*iter).first->seg.ubva.va + (*iter).second) {
                if (urma_unregister_seg((*iter).first)) {
                    LOG(ERROR) << "Failed to unregister memory "
                               << (*iter).first->seg.ubva.va;
                    return ERR_CONTEXT;
                }
                seg_region_list_.erase(iter);
                has_removed = true;
                break;
            }
        }
    } while (has_removed);
    return 0;
}

std::string UrmaContext::getEid() { return eid(); }

int UrmaContext::doProcessContextEvents() {
    urma_async_event_t event;
    if (urma_get_async_event(urma_context_, &event) < 0) return ERR_CONTEXT;
    LOG(WARNING) << "Worker: Received context async event " << event.event_type
                 << " for context " << device_name_;
    if (event.event_type == URMA_EVENT_JETTY_ERR ||
        event.event_type == URMA_EVENT_JETTY_LIMIT) {
        LOG(WARNING) << "JETTY ERR OR LIMIT" << event.event_type
                     << device_name_;
    } else if (event.event_type == URMA_EVENT_DEV_FATAL ||
               event.event_type == URMA_EVENT_JFC_ERR ||
               event.event_type == URMA_EVENT_PORT_DOWN ||
               event.event_type == URMA_EVENT_EID_CHANGE) {
        set_active(false);
        disconnectAllEndpoints();
        LOG(INFO) << "Worker: Context " << device_name_ << " is now inactive";
    } else if (event.event_type == URMA_EVENT_PORT_ACTIVE) {
        set_active(true);
        LOG(INFO) << "Worker: Context " << device_name_ << " is now active";
    }
    urma_ack_async_event(&event);
    return 0;
}

void* UrmaContext::retrieveRemoteSeg(const std::string& remoteSegmentStr) {
    auto ret = import_tseg_map.find(remoteSegmentStr);
    if (ret != import_tseg_map.end()) return ret->second;
    std::vector<unsigned char> output_buffer;
    deserializeBinaryData(remoteSegmentStr, output_buffer);
    urma_seg_t* handle;
    handle = (urma_seg_t*)malloc(sizeof(urma_seg_t));
    memcpy(handle, output_buffer.data(), sizeof(urma_seg_t));
    remote_seg_list_.push_back(handle);
    auto import_tseg =
        urma_import_seg(urma_context_, handle, &urma_token, 0, import_flag_);
    if (import_tseg == NULL) {
        LOG(ERROR) << "Import segment Failed With " << remoteSegmentStr;
        free(handle);
        return nullptr;
    }
    imported_seg_list_.push_back(import_tseg);
    import_tseg_map[remoteSegmentStr] = import_tseg;
    return import_tseg;
}

int UrmaContext::openDevice(const std::string& device_name, uint8_t port,
                            int& eid_index) {
    int num_devices = 0;
    urma_context_t* context = nullptr;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    urma_eid_info_t* eid_list;
    int ret;
    uint32_t eid_cnt;
    if (!devices) {
        LOG(ERROR) << "urma_get_device_list failed";
        return ERR_DEVICE_NOT_FOUND;
    }
    if (devices && num_devices <= 0) {
        LOG(ERROR) << "urma_get_device_list failed";
        urma_free_device_list(devices);
        return ERR_DEVICE_NOT_FOUND;
    }
    LOG(INFO) << "found " << num_devices << " devices.";
    for (int i = 0; i < num_devices; ++i) {
        if (device_name != devices[i]->name) continue;

        eid_list = urma_get_eid_list(devices[i], &eid_cnt);
        if (eid_list == NULL) {
            PLOG(ERROR) << "Failed to get eid list, device = " << device_name;
            urma_free_device_list(devices);
            return ERR_CONTEXT;
        }
        for (uint32_t j = 0; eid_list != NULL && j < eid_cnt; j++) {
            LOG(INFO) << "device_name : " << device_name
                      << " EID : " << eid(eid_list[j].eid);
        }
        if (eid_cnt > 0) {
            eid_index = eid_list[0].eid_index;
        }
        eid_ = eid_list[0].eid;
        urma_free_eid_list(eid_list);

        context = urma_create_context(devices[i], eid_index);
        if (!context) {
            LOG(ERROR) << "Urma_create_device context(" << device_name
                       << ")  failed failed";
            urma_free_device_list(devices);
            return ERR_CONTEXT;
        }

        ret = urma_query_device(devices[i], &dev_attr_);
        if (ret) {
            PLOG(ERROR) << "Failed to query dev attr( " << device_name << " ) ";
            if (urma_delete_context(context)) {
                PLOG(ERROR)
                    << "urma_delete_context(" << device_name << ") failed";
            }
            urma_free_device_list(devices);
            return ERR_CONTEXT;
        }
        for (int p = 0; p < MAX_PORT_CNT; p++) {
            auto port_attr = dev_attr_.port_attr[p];
            if (port_attr.state == URMA_PORT_ACTIVE ||
                port_attr.state == URMA_PORT_ACTIVE_DEFER) {
                port_ = p;
                break;
            }
        }
        if (dev_attr_.port_cnt != 0 &&
            dev_attr_.port_attr[port_].state != URMA_PORT_ACTIVE &&
            dev_attr_.port_attr[port_].state != URMA_PORT_ACTIVE_DEFER) {
            LOG(WARNING) << "Device " << device_name
                         << " not found active port";
            if (urma_delete_context(context)) {
                PLOG(ERROR)
                    << "urma_delete_context(" << device_name << ") failed";
            }
            urma_free_device_list(devices);
            return ERR_CONTEXT;
        }

        updateUrmaGlobalConfig(dev_attr_);

#ifndef CONFIG_SKIP_NULL_GID_CHECK
        if (isNullEid(&eid_)) {
            LOG(WARNING) << "GID is NULL, please check your EID index by "
                            "specifying MC_EID_INDEX";
            if (urma_delete_context(context)) {
                PLOG(ERROR)
                    << "urma_delete_context(" << device_name << ") failed";
            }
            urma_free_device_list(devices);
            return ERR_CONTEXT;
        }
#endif  // CONFIG_SKIP_NULL_GID_CHECK

        urma_context_ = context;
        eid_index_ = eid_index;
        if (dev_attr_.port_cnt != 0) {
            active_mtu_ = dev_attr_.port_attr[port_].active_mtu;
            active_speed_ = dev_attr_.port_attr[port_].active_speed;
        } else {
            active_mtu_ = URMA_MTU_4096;  // default mtu and speed
            active_speed_ = URMA_SP_100G;
        }

        urma_free_device_list(devices);
        return 0;
    }

    urma_free_device_list(devices);
    LOG(ERROR) << "No matched device found: " << device_name;
    return ERR_DEVICE_NOT_FOUND;
}

std::string UrmaContext::eid() const {
    std::string eid_str;
    char buf[16] = {0};
    const static size_t kEidLength = URMA_EID_SIZE;
    for (size_t i = 0; i < kEidLength; ++i) {
        sprintf(buf, "%02x", eid_.raw[i]);
        eid_str += i == 0 ? buf : std::string(":") + buf;
    }

    return eid_str;
}

std::string UrmaContext::eid(urma_eid_t eid) {
    std::string eid_str;
    char buf[16] = {0};
    const static size_t kEidLength = URMA_EID_SIZE;
    for (size_t i = 0; i < kEidLength; ++i) {
        sprintf(buf, "%02x", eid.raw[i]);
        eid_str += i == 0 ? buf : std::string(":") + buf;
    }

    return eid_str;
}

bool UrmaContext::transEidFromString(const std::string& eid_str,
                                     urma_eid_t& eid) {
    std::stringstream ss(eid_str);
    std::string byte_str;
    size_t index = 0;

    while (std::getline(ss, byte_str, ':') && index < URMA_EID_SIZE) {
        try {
            int byte_val = std::stoi(byte_str, nullptr, 16);
            eid.raw[index++] = static_cast<uint8_t>(byte_val);
        } catch (const std::exception&) {
            return false;
        }
    }

    return index == URMA_EID_SIZE;
}

void UrmaContext::registerJettyOwner(uint32_t jetty_id, UrmaEndpoint* endpoint,
                                     int slot) {
    RWSpinlock::WriteGuard guard(jetty_owner_lock_);
    jetty_owner_map_[jetty_id] = JettyOwner{endpoint, slot};
}

void UrmaContext::unregisterJettyOwner(uint32_t jetty_id) {
    RWSpinlock::WriteGuard guard(jetty_owner_lock_);
    jetty_owner_map_.erase(jetty_id);
}

bool UrmaContext::findJettyOwner(uint32_t jetty_id, UrmaEndpoint** endpoint,
                                 int* slot) {
    RWSpinlock::ReadGuard guard(jetty_owner_lock_);
    auto it = jetty_owner_map_.find(jetty_id);
    if (it == jetty_owner_map_.end()) return false;
    if (endpoint) *endpoint = it->second.endpoint;
    if (slot) *slot = it->second.slot;
    return true;
}

void UrmaContext::addDrainingEndpoint(UrmaEndpoint* endpoint) {
    RWSpinlock::WriteGuard guard(jetty_owner_lock_);
    draining_endpoints_.insert(endpoint);
}

void UrmaContext::removeDrainingEndpoint(UrmaEndpoint* endpoint) {
    RWSpinlock::WriteGuard guard(jetty_owner_lock_);
    draining_endpoints_.erase(endpoint);
}

void UrmaContext::checkJettyDrainTimeouts() {
    std::vector<UrmaEndpoint*> endpoints;
    {
        RWSpinlock::ReadGuard guard(jetty_owner_lock_);
        endpoints.assign(draining_endpoints_.begin(),
                         draining_endpoints_.end());
    }
    for (auto* endpoint : endpoints) {
        if (endpoint) endpoint->checkDrainTimeout();
    }
}

int UrmaContext::poll(int num_entries, Transport::Slice** failed_slices,
                      int& num_failed,
                      std::unordered_map<volatile int*, int>& jetty_depth_set,
                      int jfc_index) {
    num_failed = 0;
    checkJettyDrainTimeouts();
    urma_cr_t cr[num_entries];
    int nr_poll = urma_poll_jfc(jfc_list_[jfc_index].native, num_entries, cr);
    if (nr_poll < 0) {
        LOG(ERROR) << "Failed to poll JFC " << jfc_index << " of device "
                   << device_name_;
        return ERR_CONTEXT;
    }
    int wr_completions = 0;
    for (int i = 0; i < nr_poll; ++i) {
        // Fake fence CQE: user_ctx is invalid; match by local_id only.
        if (cr[i].status == URMA_CR_WR_FLUSH_ERR_DONE) {
            UrmaEndpoint* endpoint = nullptr;
            int slot = -1;
            if (findJettyOwner(cr[i].local_id, &endpoint, &slot) && endpoint) {
                endpoint->onFlushDone(slot);
            } else {
                LOG(WARNING) << "FLUSH_ERR_DONE for unknown jetty local_id="
                             << cr[i].local_id << " on " << device_name_;
            }
            continue;
        }

        auto slice = (Transport::Slice*)cr[i].user_ctx;
        if (!slice) {
            continue;
        }
        ++wr_completions;

        // All deref of `slice` (including the jetty_depth aggregation below)
        // MUST happen before markSuccess(): once that publishes completion,
        // the submitting thread may recycle the slice immediately.
        auto* depth = slice->ub.jetty_depth;
        auto it = jetty_depth_set.find(depth);
        if (it != jetty_depth_set.end())
            it->second++;
        else
            jetty_depth_set[depth] = 1;

        if (cr[i].status == URMA_CR_SUCCESS) {
            // Safe to publish here — we are done with this slice and do not
            // return it to the caller, so no one else will deref it.
            slice->markSuccess();
            continue;
        }

        if (cr[i].status == URMA_CR_ACK_TIMEOUT_ERR) {
            auto* endpoint = static_cast<UrmaEndpoint*>(slice->ub.endpoint);
            if (endpoint) {
                int slot = endpoint->findSlotByDepth(depth);
                if (slot < 0) {
                    UrmaEndpoint* mapped = nullptr;
                    int mapped_slot = -1;
                    if (findJettyOwner(cr[i].local_id, &mapped, &mapped_slot) &&
                        mapped == endpoint) {
                        slot = mapped_slot;
                    }
                }
                if (slot >= 0) endpoint->onJettyError(slot);
            }
        }

        if (cr[i].status != URMA_CR_WR_FLUSH_ERR ||
            show_work_request_flushed_error_)
            LOG(ERROR) << "Worker: Process failed for slice (opcode: "
                       << slice->opcode
                       << ", source_addr: " << slice->source_addr
                       << ", length: " << slice->length
                       << ", dest_addr: " << (void*)slice->ub.dest_addr
                       << ", local_nic: " << deviceName()
                       << ", peer_nic: " << slice->peer_nic_path
                       << ", dest_seg_tokenid: "
                       << static_cast<urma_target_seg_t*>(slice->ub.r_seg)
                              ->seg.token_id
                       << ", retry_cnt: " << slice->ub.retry_cnt
                       << "): " << cr[i].status << ", jfc idx : " << jfc_index
                       << ", comp_events_acked: "
                       << jfc_list_[jfc_index].native->comp_events_acked << " "
                       << jfc_list_[jfc_index].native->async_events_acked;

        // Failed: hand the slice back so the caller can decide retry vs
        // final markFailed(). Slice is NOT published, so the caller may
        // safely deref it.
        failed_slices[num_failed++] = slice;
    }
    // Exclude FLUSH_ERR_DONE from outstanding accounting (it was never posted).
    return wr_completions;
}

volatile int* UrmaContext::outstandingCount(int jfc_index) {
    return &jfc_list_[jfc_index].outstanding;
}

urma_jfc_t* UrmaContext::jfc() {
    int index = (next_jfc_list_index_++) % jfc_list_.size();
    return jfc_list_[index].native;
}

urma_jfr_t* UrmaContext::jfr() {
    int index = (next_jfr_list_index_++) % jfr_list_.size();
    return jfr_list_[index].native;
}

urma_jfce_t* UrmaContext::JFCE() {
    int index = (next_jfce_index_++) % num_JFCE_;
    return jfce_[index];
}

int UrmaContext::jfcCount() { return jfc_list_.size(); }

bool UrmaContext::uninit() {
    urma_uninit();
    return true;
}

bool UrmaContext::init() {
    urma_init_attr_t init_attr = {};
    auto ret = urma_init(&init_attr);
    if (ret != URMA_SUCCESS && ret != URMA_EEXIST) {
        LOG(ERROR) << "Failed to urma init, ret = " << ret;
        return false;
    }
    LOG(INFO) << "URMA module init success";
    return true;
}

// start define the UrmaEndpot method
int UrmaEndpoint::construct(GlobalConfig& config) {
    size_t num_jetty_list = config.num_jetty_per_ep;
    size_t max_wr_depth = config.max_wr;
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    jetty_list_.resize(num_jetty_list);
    jetty_state_.assign(num_jetty_list, ACTIVE);
    peer_jetty_id_.assign(num_jetty_list, 0);
    jetty_id_map_.clear();
    peer_eid_.clear();
    drain_start_ns_ = 0;
    draining_slot_ = -1;
    auto* jfc = context_->jfc();
    jfc_outstanding_ = (volatile int*)jfc->jfc_cfg.user_ctx;

    max_wr_depth_ = (int)max_wr_depth;  // work request
    wr_depth_list_ = new volatile int[num_jetty_list];
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }
    urma_jfs_cfg_t jfs_cfg = {
        .depth = 2048,            // DEFAULT_DEPTH (512)
        .trans_mode = URMA_TM_RC, /* Reliable connection */
        .priority = 15,           // URMA_MAX_PRIORITY 15
        .max_sge = 5,             // SGE_NUM_MAX 5
        .rnr_retry = 7,           // URMA_TYPICAL_RNR_RETRY    7
        .err_timeout = 17,        // URMA_TYPICAL_ERR_TIMEOUT   17
        .user_ctx = 0,
    };
    urma_jetty_flag_t jetty_flag = {};
    urma_jetty_cfg_t attr;
    memset(&attr, 0, sizeof(attr));
    jetty_flag.bs.share_jfr = 1;
    attr.flag = jetty_flag;
    attr.jfs_cfg = jfs_cfg;
    for (size_t i = 0; i < num_jetty_list; ++i) {
        wr_depth_list_[i] = 0;
        attr.jfs_cfg.jfc = jfc;
        // attr.shared.jfc = jfc;
        attr.shared.jfr = context_->jfr();
        jetty_list_[i] = urma_create_jetty(context_->urma_context_, &attr);
        if (!jetty_list_[i]) {
            PLOG(ERROR) << "Failed to create jetty";
            return ERR_ENDPOINT;
        }
        uint32_t jetty_id = jetty_list_[i]->jetty_id.id;
        jetty_id_map_[jetty_id] = static_cast<int>(i);
        context_->registerJettyOwner(jetty_id, this, static_cast<int>(i));
        LOG(INFO) << "Create jetty success, jetty id = " << jetty_id
                  << " ,jetty jfc id = "
                  << jetty_list_[i]->jetty_cfg.jfs_cfg.jfc->jfc_id.id << " : "
                  << jfc->jfc_id.id;
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int UrmaEndpoint::deconstruct() {
    int ret = 0;
    context_->removeDrainingEndpoint(this);
    for (size_t i = 0; i < jetty_list_.size(); ++i) {
        auto imported_it = imported_jetty_map_.find(jetty_list_[i]);
        auto imported_jetty = (imported_it != imported_jetty_map_.end())
                                  ? imported_it->second
                                  : nullptr;
        if (jetty_list_[i]) {
            context_->unregisterJettyOwner(jetty_list_[i]->jetty_id.id);
            ret = urma_unbind_jetty(jetty_list_[i]);
            if (ret) PLOG(ERROR) << "Failed to unbind jetty";
        }
        if (imported_jetty != nullptr) {
            ret = urma_unimport_jetty(imported_jetty);
            if (ret) PLOG(ERROR) << "Failed to unimport jetty";
        }
        if (jetty_list_[i]) {
            ret = urma_delete_jetty(jetty_list_[i]);
            if (ret) PLOG(ERROR) << "Failed to delete jetty";
        }
        // After destroying QP, the wr_depth_list_ won't change
        bool displayed = false;
        if (wr_depth_list_[i] != 0) {
            if (!displayed) {
                LOG(WARNING) << "Outstanding work requests found, CQ will not "
                                "be generated";
                displayed = true;
            }
            __sync_fetch_and_sub(jfc_outstanding_, wr_depth_list_[i]);
            wr_depth_list_[i] = 0;
        }
    }
    jetty_list_.clear();
    jetty_state_.clear();
    jetty_id_map_.clear();
    peer_jetty_id_.clear();
    peer_eid_.clear();
    draining_slot_ = -1;
    drain_start_ns_ = 0;
    delete[] wr_depth_list_;
    wr_depth_list_ = nullptr;
    imported_jetty_map_.clear();
    return 0;
}

void UrmaEndpoint::setPeerNicPath(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Previous connection will be discarded";
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

const std::string UrmaEndpoint::toString() const {
    auto status = status_.load(std::memory_order_relaxed);
    if (status == CONNECTED)
        return "EndPoint: local " + context_->nicPath() + ", peer " +
               peer_nic_path_;
    else
        return "EndPoint: local " + context_->nicPath() + " (unconnected)";
}

int UrmaEndpoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(INFO) << "Connection has been established";
        return 0;
    }

    if (context_->nicPath() == peer_nic_path_) {
        auto segment_desc =
            context_->engine().meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (segment_desc) {
            for (auto& nic : segment_desc->devices) {
                if (nic.name == context_->deviceName()) {
                    return doSetupConnection(nic.eid, JettyNum());
                }
            }
        }
        LOG(ERROR) << "Peer NIC " << context_->deviceName()
                   << " not found in localhost";
        return ERR_DEVICE_NOT_FOUND;
    }

    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_->nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.jetty_num = JettyNum();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_->engine().sendHandshake(peer_server_name, local_desc,
                                              peer_desc);
    if (rc) return rc;
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Reject the handshake request by peer "
                   << local_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.local_nic_path != peer_nic_path_ ||
        peer_desc.peer_nic_path != local_desc.local_nic_path) {
        LOG(ERROR) << "Invalid argument: received packet mismatch"
                   << ", local.local_nic_path: " << local_desc.local_nic_path
                   << ", local.peer_nic_path: " << local_desc.peer_nic_path
                   << ", peer.local_nic_path: " << peer_desc.local_nic_path
                   << ", peer.peer_nic_path: " << peer_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    auto segment_desc =
        context_->engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto& nic : segment_desc->devices) {
            if (nic.name == peer_nic_name) {
                return doSetupConnection(nic.eid, peer_desc.jetty_num);
            }
        }
    }
    LOG(ERROR) << "Peer NIC " << peer_nic_name << " not found in "
               << peer_server_name;
    return ERR_DEVICE_NOT_FOUND;
}

void UrmaEndpoint::disconnectUnlocked() {
    urma_jetty_attr_t attr;
    memset(&attr, 0, sizeof(attr));
    attr.mask = JETTY_STATE;
    attr.state = URMA_JETTY_STATE_RESET;

    context_->removeDrainingEndpoint(this);
    draining_slot_ = -1;
    drain_start_ns_ = 0;
    for (size_t i = 0; i < jetty_list_.size(); ++i) {
        int ret = urma_modify_jetty(jetty_list_[i], &attr);
        if (ret) PLOG(ERROR) << "Failed to modify jetty to RESET";
        auto imported_jetty = imported_jetty_map_[jetty_list_[i]];
        ret = urma_unbind_jetty(jetty_list_[i]);
        if (ret) PLOG(ERROR) << "Failed to unbind jetty";
        ret = urma_unimport_jetty(imported_jetty);
        if (ret) PLOG(ERROR) << "Failed to unimport jetty";
        // After resetting QP, the wr_depth_list_ won't change
        bool displayed = false;
        if (wr_depth_list_[i] != 0) {
            if (!displayed) {
                LOG(WARNING) << "Outstanding work requests found, JFC will not "
                                "be generated";
                displayed = true;
            }
            __sync_fetch_and_sub(jfc_outstanding_, wr_depth_list_[i]);
            wr_depth_list_[i] = 0;
        }
        jetty_state_[i] = ACTIVE;
    }
    imported_jetty_map_.clear();
    peer_jetty_id_.assign(jetty_list_.size(), 0);
    peer_eid_.clear();
    status_.store(UNCONNECTED, std::memory_order_release);
}

int UrmaEndpoint::setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                            HandShakeDesc& local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Re-establish connection: " << toString();
        disconnectUnlocked();
    }

    if (peer_desc.peer_nic_path != context_->nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg =
            "Invalid argument: peer nic path inconsistency, expect " +
            context_->nicPath() + " + " + peer_nic_path_ + ", while got " +
            peer_desc.peer_nic_path + " + " + peer_desc.local_nic_path;

        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path = context_->nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.jetty_num = JettyNum();

    auto segment_desc =
        context_->engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto& nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.eid, peer_desc.jetty_num,
                                         &local_desc.reply_msg);
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

bool UrmaEndpoint::hasOutstandingSlice() const {
    if (active_) return true;
    for (size_t i = 0; i < jetty_list_.size(); i++)
        if (wr_depth_list_[i] != 0) return true;
    return false;
}

int UrmaEndpoint::submitPostSend(
    std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!active_) return 0;
    int jetty_index = selectActiveJettyUnlocked();
    if (jetty_index < 0) return 0;
    int wr_count = std::min(max_wr_depth_ - wr_depth_list_[jetty_index],
                            (int)slice_list.size());
    wr_count =
        std::min(int(globalConfig().max_jfc_e) - *jfc_outstanding_, wr_count);
    if (wr_count <= 0) return 0;

    urma_jfs_wr_t wr_list[wr_count], *bad_wr = nullptr;
    urma_sge_t l_sge_list[wr_count];
    urma_sge_t r_sge_list[wr_count];
    memset(wr_list, 0, sizeof(urma_jfs_wr_t) * wr_count);
    for (int i = 0; i < wr_count; ++i) {
        auto slice = slice_list[i];
        auto& l_sge = l_sge_list[i];
        auto& r_sge = r_sge_list[i];
        l_sge.addr = (uint64_t)slice->source_addr;
        l_sge.len = slice->length;
        l_sge.tseg = static_cast<urma_target_seg_t*>(slice->ub.l_seg);
        r_sge.addr = slice->ub.dest_addr;
        r_sge.len = slice->length;
        r_sge.tseg = static_cast<urma_target_seg_t*>(slice->ub.r_seg);

        auto& wr = wr_list[i];
        wr.user_ctx = (uint64_t)slice;
        wr.opcode = slice->opcode == Transport::TransferRequest::READ
                        ? URMA_OPC_READ
                        : URMA_OPC_WRITE;
        wr.rw.src.sge =
            slice->opcode == Transport::TransferRequest::READ ? &r_sge : &l_sge;
        wr.rw.src.num_sge = 1;
        wr.rw.dst.sge =
            slice->opcode == Transport::TransferRequest::READ ? &l_sge : &r_sge;
        wr.rw.dst.num_sge = 1;
        wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
        wr.flag.bs.complete_enable = 1;
        wr.flag.bs.inline_flag = 0;
        // Check if the jetty is in the imported_jetty_map_
        auto it = imported_jetty_map_.find(jetty_list_[jetty_index]);
        if (it == imported_jetty_map_.end()) {
            LOG(ERROR) << "Jetty not imported for endpoint, tjetty is nullptr"
                       << jetty_index << ", local_nic=";
        }
        if (it != imported_jetty_map_.end()) {
            wr.tjetty = it->second;
        } else {
            // If not found, use a dummy value
            wr.tjetty = nullptr;
        }
        slice->ts = getCurrentTimeInNano();
        slice->status = Transport::Slice::POSTED;
        slice->ub.jetty_depth = &wr_depth_list_[jetty_index];
        // Set endpoint pointer for each slice before submitting
        slice->ub.endpoint = this;
    }
    __sync_fetch_and_add(&wr_depth_list_[jetty_index], wr_count);
    __sync_fetch_and_add(jfc_outstanding_, wr_count);
    int rc =
        urma_post_jetty_send_wr(jetty_list_[jetty_index], wr_list, &bad_wr);
    if (rc) {
        PLOG(ERROR) << "Failed to urma_post_jetty_send_wr";
        while (bad_wr) {
            int i = bad_wr - wr_list;
            LOG(ERROR) << "slice (" << i << ") post send failed.";
            failed_slice_list.push_back(slice_list[i]);
            __sync_fetch_and_sub(&wr_depth_list_[jetty_index], 1);
            __sync_fetch_and_sub(jfc_outstanding_, 1);
            bad_wr = bad_wr->next;
        }
    }
    slice_list.erase(slice_list.begin(), slice_list.begin() + wr_count);
    return 0;
}

std::vector<uint32_t> UrmaEndpoint::JettyNum() const {
    std::vector<uint32_t> ret;
    for (int jetty_index = 0; jetty_index < (int)jetty_list_.size();
         ++jetty_index)
        ret.push_back(jetty_list_[jetty_index]->jetty_id.id);
    return ret;
}

int UrmaEndpoint::doSetupConnection(const std::string& peer_eid,
                                    std::vector<uint32_t> peer_jetty_num_list,
                                    std::string* reply_msg) {
    if (jetty_list_.size() != peer_jetty_num_list.size()) {
        std::string message =
            "jetty count mismatch in peer and local endpoints, check "
            "MC_MAX_EP_PER_CTX";
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    peer_eid_ = peer_eid;
    for (int jetty_index = 0; jetty_index < (int)jetty_list_.size();
         ++jetty_index) {
        int ret = doSetupConnection(
            jetty_index, peer_eid, peer_jetty_num_list[jetty_index], reply_msg);
        if (ret) return ret;
    }

    status_.store(CONNECTED, std::memory_order_relaxed);
    return 0;
}

int UrmaEndpoint::doSetupConnection(int jetty_index,
                                    const std::string& peer_eid,
                                    uint32_t peer_jetty_num,
                                    std::string* reply_msg) {
    if (jetty_index < 0 || jetty_index >= (int)jetty_list_.size())
        return ERR_INVALID_ARGUMENT;
    auto& jetty = jetty_list_[jetty_index];
    urma_eid_t eid;
    bool trans_ret = context_->transEidFromString(peer_eid, eid);
    if (!trans_ret) {
        PLOG(ERROR) << "Invalid peer eid: " << peer_eid;
        return ERR_INVALID_ARGUMENT;
    }
    urma_rjetty_t rjetty = {};
    rjetty.jetty_id.id = peer_jetty_num;
    rjetty.jetty_id.eid = eid;
    rjetty.trans_mode = URMA_TM_RC;
    rjetty.type = URMA_JETTY;
    rjetty.tp_type = URMA_CTP;
    rjetty.flag.value = 0;
    LOG(INFO) << "Peer jetty id = " << peer_jetty_num;
    urma_target_jetty_t* imported_jetty =
        urma_import_jetty(context_->urma_context_, &rjetty, &urma_token);
    urma_status_t ret = urma_bind_jetty(jetty, imported_jetty);
    if (ret != URMA_SUCCESS && ret != URMA_EEXIST) {
        std::string message = "Failed to bind jetty";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        urma_unimport_jetty(imported_jetty);
        return ERR_ENDPOINT;
    }
    imported_jetty_map_[jetty] = imported_jetty;
    peer_jetty_id_[jetty_index] = peer_jetty_num;
    jetty_state_[jetty_index] = ACTIVE;
    LOG(INFO) << "Bind jetty success, local jetty id:" << jetty->jetty_id.id
              << ", remote jetty id:" << peer_jetty_num;

    return 0;
}

int UrmaEndpoint::findSlotByDepth(volatile int* depth) const {
    if (!depth || !wr_depth_list_) return -1;
    for (size_t i = 0; i < jetty_list_.size(); ++i) {
        if (&wr_depth_list_[i] == depth) return static_cast<int>(i);
    }
    return -1;
}

bool UrmaEndpoint::hasNonActiveJettyUnlocked() const {
    for (auto state : jetty_state_) {
        if (state != ACTIVE) return true;
    }
    return false;
}

int UrmaEndpoint::selectActiveJettyUnlocked() {
    if (jetty_list_.empty()) return -1;
    const int n = static_cast<int>(jetty_list_.size());
    int start = SimpleRandom::Get().next(n);
    for (int i = 0; i < n; ++i) {
        int idx = (start + i) % n;
        if (jetty_state_[idx] == ACTIVE) return idx;
    }
    return -1;
}

void UrmaEndpoint::onJettyError(int slot) {
    bool delete_ep = false;
    {
        RWSpinlock::WriteGuard guard(lock_);
        if (slot < 0 || slot >= static_cast<int>(jetty_list_.size())) return;
        if (jetty_state_[slot] == DRAINING ||
            jetty_state_[slot] == REBUILDING) {
            return;  // idempotent
        }
        // Serial rebuild: at most one non-ACTIVE jetty per endpoint.
        if (hasNonActiveJettyUnlocked()) {
            LOG(INFO) << "Skip jetty rebuild for slot " << slot
                      << ": another jetty is already draining/rebuilding on "
                      << toString();
            return;
        }
        if (!jetty_list_[slot]) return;

        urma_jetty_attr_t attr{};
        attr.mask = JETTY_STATE;
        attr.state = URMA_JETTY_STATE_ERROR;
        int ret = urma_modify_jetty(jetty_list_[slot], &attr);
        if (ret) {
            PLOG(ERROR) << "Failed to modify jetty to ERROR, slot=" << slot
                        << " jetty_id=" << jetty_list_[slot]->jetty_id.id;
            context_->removeDrainingEndpoint(this);
            draining_slot_ = -1;
            drain_start_ns_ = 0;
            delete_ep = true;
        } else {
            jetty_state_[slot] = DRAINING;
            draining_slot_ = slot;
            drain_start_ns_ = getCurrentTimeInNano();
            context_->addDrainingEndpoint(this);
            LOG(WARNING) << "Jetty ACK timeout: start drain slot=" << slot
                         << " jetty_id=" << jetty_list_[slot]->jetty_id.id
                         << " on " << toString();
        }
    }
    if (delete_ep) {
        LOG(ERROR) << "Jetty rebuild fallback to deleteEndpoint: "
                   << "modify_jetty(ERROR) failed on " << toString();
        context_->deleteEndpointByPtr(this);
    }
}

void UrmaEndpoint::onFlushDone(int slot) {
    bool delete_ep = false;
    {
        RWSpinlock::WriteGuard guard(lock_);
        if (slot < 0 || slot >= static_cast<int>(jetty_list_.size())) return;
        if (jetty_state_[slot] != DRAINING) return;
        jetty_state_[slot] = REBUILDING;
        LOG(INFO) << "Jetty flush-done: rebuild slot=" << slot << " on "
                  << toString();
        if (rebuildJettyUnlocked(slot)) {
            context_->removeDrainingEndpoint(this);
            draining_slot_ = -1;
            drain_start_ns_ = 0;
            delete_ep = true;
        }
    }
    if (delete_ep) {
        LOG(ERROR) << "Jetty rebuild fallback to deleteEndpoint: "
                   << "rebuildJetty failed on " << toString();
        context_->deleteEndpointByPtr(this);
    }
}

void UrmaEndpoint::checkDrainTimeout() {
    bool delete_ep = false;
    {
        RWSpinlock::WriteGuard guard(lock_);
        if (draining_slot_ < 0) return;
        int slot = draining_slot_;
        if (slot >= static_cast<int>(jetty_state_.size()) ||
            jetty_state_[slot] != DRAINING) {
            return;
        }
        uint64_t now = getCurrentTimeInNano();
        if (now - drain_start_ns_ < kJettyDrainTimeoutNs) return;
        LOG(ERROR) << "Jetty drain timed out after "
                   << ((now - drain_start_ns_) / 1000000ull)
                   << "ms, slot=" << slot << " on " << toString();
        context_->removeDrainingEndpoint(this);
        draining_slot_ = -1;
        drain_start_ns_ = 0;
        delete_ep = true;
    }
    if (delete_ep) {
        LOG(ERROR) << "Jetty rebuild fallback to deleteEndpoint: "
                   << "flush-done timeout on " << toString();
        context_->deleteEndpointByPtr(this);
    }
}

int UrmaEndpoint::rebuildJettyUnlocked(int slot) {
    auto* old_jetty = jetty_list_[slot];
    if (!old_jetty) return ERR_ENDPOINT;
    const uint32_t old_id = old_jetty->jetty_id.id;
    const uint32_t peer_id = peer_jetty_id_[slot];
    const uint64_t started_ns = drain_start_ns_;
    urma_jfc_t* reuse_jfc = old_jetty->jetty_cfg.jfs_cfg.jfc;
    urma_jfr_t* reuse_jfr = old_jetty->jetty_cfg.shared.jfr;

    // 1) Flush residual WRs (may overlap with already-polled CRs).
    urma_cr_t flush_crs[64];
    while (true) {
        int flushed = urma_flush_jetty(old_jetty, 64, flush_crs);
        if (flushed < 0) {
            PLOG(ERROR) << "urma_flush_jetty failed, slot=" << slot;
            return ERR_ENDPOINT;
        }
        if (flushed == 0) break;
        // Completions for these WRs should already have been (or will be)
        // accounted via poll; do not touch slice pointers from flush CRs.
    }

    // 2) Unbind / unimport old peer view.
    auto imported_it = imported_jetty_map_.find(old_jetty);
    urma_target_jetty_t* old_imported =
        (imported_it != imported_jetty_map_.end()) ? imported_it->second
                                                   : nullptr;
    int ret = urma_unbind_jetty(old_jetty);
    if (ret) PLOG(ERROR) << "Failed to unbind jetty before rebuild";
    if (old_imported) {
        ret = urma_unimport_jetty(old_imported);
        if (ret) PLOG(ERROR) << "Failed to unimport jetty before rebuild";
        imported_jetty_map_.erase(imported_it);
    }

    // 3) Delete old jetty and clear outstanding depth for this slot.
    context_->unregisterJettyOwner(old_id);
    jetty_id_map_.erase(old_id);
    ret = urma_delete_jetty(old_jetty);
    if (ret) {
        PLOG(ERROR) << "Failed to delete jetty during rebuild";
        jetty_list_[slot] = nullptr;
        return ERR_ENDPOINT;
    }
    jetty_list_[slot] = nullptr;
    if (wr_depth_list_[slot] != 0) {
        __sync_fetch_and_sub(jfc_outstanding_, wr_depth_list_[slot]);
        wr_depth_list_[slot] = 0;
    }

    // 4) Create replacement jetty with the same JFC/JFR config.
    urma_jfs_cfg_t jfs_cfg = {
        .depth = 2048,
        .trans_mode = URMA_TM_RC,
        .priority = 15,
        .max_sge = 5,
        .rnr_retry = 7,
        .err_timeout = 17,
        .user_ctx = 0,
    };
    urma_jetty_flag_t jetty_flag = {};
    jetty_flag.bs.share_jfr = 1;
    urma_jetty_cfg_t attr{};
    attr.flag = jetty_flag;
    attr.jfs_cfg = jfs_cfg;
    attr.jfs_cfg.jfc = reuse_jfc ? reuse_jfc : context_->jfc();
    attr.shared.jfr = reuse_jfr ? reuse_jfr : context_->jfr();
    urma_jetty_t* new_jetty = urma_create_jetty(context_->urma_context_, &attr);
    if (!new_jetty) {
        PLOG(ERROR) << "Failed to create jetty during rebuild";
        return ERR_ENDPOINT;
    }

    // 5) Re-import peer and bind locally (no peer protocol).
    if (peer_eid_.empty()) {
        LOG(ERROR) << "Missing peer eid during jetty rebuild";
        urma_delete_jetty(new_jetty);
        return ERR_ENDPOINT;
    }
    urma_eid_t eid;
    if (!context_->transEidFromString(peer_eid_, eid)) {
        LOG(ERROR) << "Invalid peer eid during jetty rebuild: " << peer_eid_;
        urma_delete_jetty(new_jetty);
        return ERR_ENDPOINT;
    }
    urma_rjetty_t rjetty = {};
    rjetty.jetty_id.id = peer_id;
    rjetty.jetty_id.eid = eid;
    rjetty.trans_mode = URMA_TM_RC;
    rjetty.type = URMA_JETTY;
    rjetty.tp_type = URMA_CTP;
    rjetty.flag.value = 0;
    urma_target_jetty_t* imported =
        urma_import_jetty(context_->urma_context_, &rjetty, &urma_token);
    if (!imported) {
        PLOG(ERROR) << "Failed to import peer jetty during rebuild";
        urma_delete_jetty(new_jetty);
        return ERR_ENDPOINT;
    }
    urma_status_t bind_ret = urma_bind_jetty(new_jetty, imported);
    if (bind_ret != URMA_SUCCESS && bind_ret != URMA_EEXIST) {
        PLOG(ERROR) << "Failed to bind rebuilt jetty";
        urma_unimport_jetty(imported);
        urma_delete_jetty(new_jetty);
        return ERR_ENDPOINT;
    }

    jetty_list_[slot] = new_jetty;
    imported_jetty_map_[new_jetty] = imported;
    const uint32_t new_id = new_jetty->jetty_id.id;
    jetty_id_map_[new_id] = slot;
    context_->registerJettyOwner(new_id, this, slot);
    jetty_state_[slot] = ACTIVE;
    draining_slot_ = -1;
    drain_start_ns_ = 0;
    context_->removeDrainingEndpoint(this);

    LOG(WARNING) << "Jetty rebuilt successfully slot=" << slot
                 << " old_id=" << old_id << " new_id=" << new_id
                 << " peer_id=" << peer_id << " elapsed_ms="
                 << ((getCurrentTimeInNano() - started_ns) / 1000000ull)
                 << " on " << toString();
    return 0;
}

std::shared_ptr<UbEndPoint> UrmaContext::makeEndpoint() {
    return std::make_shared<UrmaEndpoint>(this);
}
}  // namespace mooncake
