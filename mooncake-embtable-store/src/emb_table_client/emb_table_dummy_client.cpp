#include "embtable/emb_table_client/emb_table_dummy_client.h"

namespace embtable {

EmbTableDummyClient::EmbTableDummyClient(EmbTableClient::Options options)
    : client_(std::make_unique<EmbTableClient>(std::move(options))) {}

Status EmbTableDummyClient::Init() { return client_->Init(); }

Status EmbTableDummyClient::Insert(const std::vector<uint64_t>& keys,
                                   const std::vector<StringView>& values) {
    return client_->Insert(keys, values);
}

Status EmbTableDummyClient::Find(const std::vector<uint64_t>& keys,
                                 std::vector<StringView>& buffers) {
    return client_->Find(keys, buffers);
}

Status EmbTableDummyClient::BuildIndex() { return client_->BuildIndex(); }

}  // namespace embtable
