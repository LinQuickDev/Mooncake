#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "emb_table_client/emb_table_client.h"
#include "emb_types.h"

namespace embtable {

// EmbTableDummyClient is the user-side stub for disaggregated deployment
// (design doc 3.1). It forwards Insert/Find/BuildIndex to a co-located
// EmbTableClient via SHM/RPC. The first version is a thin placeholder that
// delegates to a local EmbTableClient; true remote forwarding is tracked as
// a follow-up.
class EmbTableDummyClient {
   public:
    explicit EmbTableDummyClient(EmbTableClient::Options options);

    Status Init();

    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    Status BuildIndex();

   private:
    std::unique_ptr<EmbTableClient> client_;
};

}  // namespace embtable
