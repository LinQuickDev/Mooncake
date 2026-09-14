// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#include "transfer_engine.h"
#include "transfer_engine_impl.h"

namespace mooncake {

Status TransferEngine::configureScheduling(
    const scheduling::SchedulerConfig& config) {
    if (use_tent_ || !impl_)
        return Status::NotImplemented(
            "Scheduling requires initialized legacy TE");
    return impl_->configureScheduling(config);
}

Status TransferEngine::submitScheduledTransfer(
    BatchID batch_id, const std::vector<ScheduledTransferRequest>& entries) {
    if (use_tent_) {
        std::vector<TransferRequest> requests;
        requests.reserve(entries.size());
        for (const auto& entry : entries) requests.push_back(entry.request);
        // TENT retains its existing QoS/admission path until its scheduling
        // adapter can consume the common hint contract.
        return submitTransfer(batch_id, requests);
    }
    if (!impl_)
        return Status::InvalidArgument("Transfer Engine is not initialized");
    return impl_->submitScheduledTransfer(batch_id, entries);
}

Status TransferEngine::cancelTransfer(BatchID batch_id, size_t task_id) {
    if (use_tent_ || !impl_)
        return Status::NotImplemented(
            "Scheduled cancellation requires legacy TE");
    return impl_->cancelTransfer(batch_id, task_id);
}

}  // namespace mooncake
