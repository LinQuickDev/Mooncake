// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Interface 8 (entity event reporting), minimal W2 shape.
//
// Design: docs/analyze/tent-ub-post-poll-threadpool-design.md §11 (interface 8)
// and §7.2 step 1/3b/4; W2 landing:
// docs/analyze/tent-ub-w2-completion-gate-design.md §7.3.
//
// W2 has exactly one event kind it can prove: a completion token that does not
// belong to any live record (unknown/late/duplicate-after-disposal). The
// signature is already the frozen one - (jfc/local key, entity type, local id,
// kind, evidence) - so W4 can fill in markers and async events without moving
// a single call site.
//
// The sink must never block the poller: W2 counts and rate-limits the log, W4
// replaces the body with a bounded, coalescing queue towards the pooling side.

#ifndef TENT_TRANSPORT_UB_ENTITY_EVENT_H_
#define TENT_TRANSPORT_UB_ENTITY_EVENT_H_

#include <atomic>
#include <cstdint>

#include <glog/logging.h>

namespace mooncake::tent::ub {

enum class EntityType : uint8_t {
    kUnknown = 0,
    kJfc = 1,
    kJetty = 2,
    kContext = 3,
    kDevice = 4,
};

enum class EntityEventKind : uint8_t {
    // A completion token that matched nothing in the business registry.
    kUnknownToken = 0,
    // Entity-level markers (W4): no flag bit, fabricated native status, no
    // valid user context.
    kMarkerFlushDone = 1,
    kMarkerSuspendDone = 2,
    // Asynchronous event (W4).
    kAsyncEvent = 3,
    // Token 0 or a marker/AE shape W2 cannot classify yet. The single call site
    // in dispatchCompletion keeps it counted so nothing is silently dropped.
    kUnclassified = 4,
};

// Everything the pooling side needs to attribute an event to a native entity
// and an error epoch. W2 fills it from the completion it could not place.
struct EntityEvidence {
    uint64_t token{0};
    uint32_t jfc_index{0};
    uint32_t jetty_id{0};
    int32_t native_status{0};
    uint32_t completed_bytes{0};
};

[[nodiscard]] inline const char* entityEventKindName(EntityEventKind kind) {
    switch (kind) {
        case EntityEventKind::kUnknownToken:
            return "unknown_token";
        case EntityEventKind::kMarkerFlushDone:
            return "marker_flush_done";
        case EntityEventKind::kMarkerSuspendDone:
            return "marker_suspend_done";
        case EntityEventKind::kAsyncEvent:
            return "async_event";
        case EntityEventKind::kUnclassified:
            return "unclassified";
    }
    return "unknown";
}

class EntityEventSink {
   public:
    virtual ~EntityEventSink() = default;

    // jfc/local key: `local_nic` plus `local_id` (the JFC index inside it).
    virtual void onEntityEvent(int local_nic, EntityType entity_type,
                               uint64_t local_id, EntityEventKind kind,
                               const EntityEvidence& evidence) = 0;
};

// W2 stub: counts every event and rate-limits a diagnostic line. No queue, no
// blocking, no allocation.
class CountingEntityEventSink final : public EntityEventSink {
   public:
    void onEntityEvent(int local_nic, EntityType entity_type, uint64_t local_id,
                       EntityEventKind kind,
                       const EntityEvidence& evidence) override {
        total_.fetch_add(1, std::memory_order_relaxed);
        if (kind == EntityEventKind::kUnknownToken) {
            unknown_token_.fetch_add(1, std::memory_order_relaxed);
        }
        LOG_EVERY_N(WARNING, 1000)
            << "UB entity event " << entityEventKindName(kind)
            << ": local_nic=" << local_nic
            << " entity=" << static_cast<int>(entity_type)
            << " local_id=" << local_id << " token=" << evidence.token
            << " jfc=" << evidence.jfc_index << " jetty=" << evidence.jetty_id
            << " status=" << evidence.native_status;
    }

    [[nodiscard]] uint64_t totalCount() const {
        return total_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t unknownTokenCount() const {
        return unknown_token_.load(std::memory_order_relaxed);
    }

   private:
    std::atomic<uint64_t> total_{0};
    std::atomic<uint64_t> unknown_token_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_ENTITY_EVENT_H_
