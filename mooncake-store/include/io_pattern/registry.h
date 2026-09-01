#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io_pattern/ops.h"

namespace mooncake::io_pattern {

template <typename Ops>
class OpsRegistry {
   public:
    using Factory = std::function<std::shared_ptr<Ops>()>;

    bool Register(std::string name, Factory factory) {
        if (name.empty() || !factory) {
            return false;
        }
        std::unique_lock lock(mutex_);
        return factories_.emplace(std::move(name), std::move(factory)).second;
    }

    std::shared_ptr<Ops> Create(std::string_view name) const {
        Factory factory;
        {
            std::shared_lock lock(mutex_);
            const auto it = factories_.find(std::string(name));
            if (it == factories_.end()) {
                return nullptr;
            }
            factory = it->second;
        }
        return factory();
    }

    std::vector<std::string> RegisteredNames() const {
        std::vector<std::string> names;
        {
            std::shared_lock lock(mutex_);
            names.reserve(factories_.size());
            for (const auto& entry : factories_) {
                names.push_back(entry.first);
            }
        }
        std::sort(names.begin(), names.end());
        return names;
    }

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, Factory> factories_;
};

struct PolicyOpsRegistries {
    OpsRegistry<EvictionOps> eviction;
    OpsRegistry<PrefetchOps> prefetch;
    OpsRegistry<AdmissionOps> admission;
};

}  // namespace mooncake::io_pattern
