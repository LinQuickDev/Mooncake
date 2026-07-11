#include "embtable/emb_table_client/emb_table_client.h"

#include <glog/logging.h>

namespace embtable {

EmbTableClient::EmbTableClient(Options options)
    : options_(std::move(options)) {}

EmbTableClient::~EmbTableClient() = default;

Status EmbTableClient::Init() {
    if (options_.valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be > 0");
    }
    if (options_.tableName.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "tableName must be non-empty");
    }
    shareMapStore_ = std::make_shared<ShareMapStore>(options_.deployment);
    auto s = shareMapStore_->Init();
    if (!s.IsOk()) return s;
    // EmbTableMeta stores its metadata objects in Mooncake Store via a
    // RealClient. ShareMapStore already owns one; rather than expose it
    // publicly we let EmbTableMeta use a dedicated client pointing to the
    // same master (cheap; both share the underlying transfer engine).
    realClient_ = std::make_shared<mooncake::RealClient>();
    int ret = realClient_->setup_real(
        /*local_hostname=*/"", options_.deployment.metadataServer,
        /*global_segment_size=*/1024 * 1024 * 16,
        /*local_buffer_size=*/1024 * 1024 * 16,
        options_.deployment.protocol, options_.deployment.deviceNames,
        options_.deployment.masterAddress);
    if (ret != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "EmbTableClient RealClient setup_real failed");
    }
    embTable_ = std::make_shared<EmbTable>(options_.tableName,
                                           options_.numBuckets,
                                           options_.valueSize,
                                           shareMapStore_, realClient_);
    return embTable_->Init(options_.createNew);
}

Status EmbTableClient::Insert(const std::vector<uint64_t>& keys,
                              const std::vector<StringView>& values) {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->Insert(keys, values);
}

Status EmbTableClient::Find(const std::vector<uint64_t>& keys,
                            std::vector<StringView>& buffers) {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->Find(keys, buffers);
}

Status EmbTableClient::BuildIndex() {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->BuildIndex();
}

uint32_t EmbTableClient::NumBuckets() const {
    return embTable_ ? embTable_->NumBuckets() : 0;
}

uint64_t EmbTableClient::ValueSize() const {
    return embTable_ ? embTable_->ValueSize() : options_.valueSize;
}

}  // namespace embtable
