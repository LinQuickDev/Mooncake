#include "vchunk_metadata_store.h"

#include <utility>

#include <iomanip>
#include <sstream>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "etcd_helper.h"

namespace mooncake {
namespace {

std::string HexEncode(std::string_view value) {
    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (const unsigned char byte : value) {
        stream << std::setw(2) << static_cast<unsigned>(byte);
    }
    return stream.str();
}

tl::expected<std::vector<char>, ErrorCode> HexDecode(std::string_view value) {
    if (value.size() % 2 != 0) {
        return tl::unexpected(ErrorCode::INVALID_VERSION);
    }
    auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::vector<char> bytes;
    bytes.reserve(value.size() / 2);
    for (size_t i = 0; i < value.size(); i += 2) {
        const int high = digit(value[i]);
        const int low = digit(value[i + 1]);
        if (high < 0 || low < 0) {
            return tl::unexpected(ErrorCode::INVALID_VERSION);
        }
        bytes.push_back(static_cast<char>((high << 4) | low));
    }
    return bytes;
}

std::string MakeObjectIndexKey(std::string_view namespace_prefix,
                               const VChunkMetadataRecord& record) {
    return std::string(namespace_prefix) + "/objects/" +
           HexEncode(record.tenant_id) + "/" + HexEncode(record.key);
}

std::string MakePersistentRecordKey(std::string_view namespace_prefix,
                                    const VChunkMetadataRecord& record) {
    return std::string(namespace_prefix) + "/records/" +
           HexEncode(record.tenant_id) + "/" + record.vchunk_id;
}

std::string BytesToString(const std::vector<char>& bytes) {
    return {bytes.data(), bytes.size()};
}

}  // namespace

std::string MakeVChunkMetadataStoreKey(const VChunkMetadataRecord& record) {
    return std::string(kVChunkMetadataNamespace) + "/" + record.tenant_id +
           "/" + record.vchunk_id;
}

ErrorCode InMemoryVChunkMetadataStore::Put(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    records_[MakeVChunkMetadataStoreKey(record)] = record;
    return ErrorCode::OK;
}

ErrorCode InMemoryVChunkMetadataStore::Remove(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    records_.erase(MakeVChunkMetadataStoreKey(record));
    return ErrorCode::OK;
}

tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode>
InMemoryVChunkMetadataStore::List() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<VChunkMetadataRecord> result;
    result.reserve(records_.size());
    for (const auto& [_, record] : records_) {
        result.push_back(record);
    }
    return result;
}

EtcdVChunkMetadataStore::EtcdVChunkMetadataStore(std::string endpoints,
                                                 VChunkConfig config,
                                                 std::string cluster_id)
    : endpoints_(std::move(endpoints)),
      config_(std::move(config)),
      namespace_prefix_(std::string(kVChunkMetadataNamespace) + "/clusters/" +
                        HexEncode(cluster_id)) {
#ifdef STORE_USE_ETCD
    connection_error_ = EtcdHelper::ConnectToEtcdStoreClient(endpoints_);
#else
    connection_error_ = ErrorCode::ETCD_OPERATION_ERROR;
#endif
}

ErrorCode EtcdVChunkMetadataStore::Put(const VChunkMetadataRecord& record) {
    if (connection_error_ != ErrorCode::OK) return connection_error_;
    auto encoded = SerializeVChunkMetadata(record, config_);
    if (!encoded) return encoded.error();
    const auto metadata_key = MakePersistentRecordKey(namespace_prefix_, record);
    const auto object_key = MakeObjectIndexKey(namespace_prefix_, record);
    const auto value = HexEncode(BytesToString(*encoded));

    if (record.status == VChunkStatus::CREATING) {
        const auto error = EtcdHelper::TxnCompareAndPut(
            {{object_key, EtcdHelper::TxnCompareKind::kKeyNotExists, {}},
             {metadata_key, EtcdHelper::TxnCompareKind::kKeyNotExists, {}}},
            {{object_key, record.vchunk_id}, {metadata_key, value}});
        return error == ErrorCode::ETCD_TRANSACTION_FAIL
                   ? ErrorCode::OBJECT_ALREADY_EXISTS
                   : error;
    }

    std::string current;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::Get(metadata_key.data(), metadata_key.size(),
                                 current, revision);
    if (error != ErrorCode::OK) return error;
    return EtcdHelper::TxnCompareAndPut(
        {{object_key, EtcdHelper::TxnCompareKind::kValueEquals,
          record.vchunk_id},
         {metadata_key, EtcdHelper::TxnCompareKind::kValueEquals, current}},
        {{metadata_key, value}});
}

ErrorCode EtcdVChunkMetadataStore::Remove(
    const VChunkMetadataRecord& record) {
    if (connection_error_ != ErrorCode::OK) return connection_error_;
    const auto metadata_key = MakePersistentRecordKey(namespace_prefix_, record);
    const auto object_key = MakeObjectIndexKey(namespace_prefix_, record);
    std::string current;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::Get(metadata_key.data(), metadata_key.size(),
                                 current, revision);
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST) return ErrorCode::OK;
    if (error != ErrorCode::OK) return error;
    return EtcdHelper::TxnCompareAndPut(
        {{object_key, EtcdHelper::TxnCompareKind::kValueEquals,
          record.vchunk_id},
         {metadata_key, EtcdHelper::TxnCompareKind::kValueEquals, current}},
        {}, {metadata_key, object_key});
}

tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode>
EtcdVChunkMetadataStore::List() {
    if (connection_error_ != ErrorCode::OK) {
        return tl::unexpected(connection_error_);
    }
    const std::string begin = namespace_prefix_ + "/";
    const std::string end = namespace_prefix_ + "0";
    std::string json;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::GetRangeAsJson(begin.data(), begin.size(),
                                            end.data(), end.size(), 0, json,
                                            revision);
    if (error != ErrorCode::OK) return tl::unexpected(error);

    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<VChunkMetadataRecord> result;
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        const auto key = item["key"].asString();
        if (key.find(begin + "records/") != 0) continue;
        auto bytes = HexDecode(item["value"].asString());
        if (!bytes) return tl::unexpected(bytes.error());
        auto record = DeserializeVChunkMetadata(*bytes, config_);
        if (!record) return tl::unexpected(record.error());
        result.push_back(std::move(*record));
    }
    return result;
}

}  // namespace mooncake
