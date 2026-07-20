#pragma once

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <system_error>
#include <unordered_map>
#include <ylt/easylog.hpp>

namespace mooncake {
class DefaultConfig {
   public:
    struct Node {
        YAML::Node yaml_node_;
        Json::Value json_value_;
    };

    enum ConfigType {
        YAML = 1,
        JSON = 2,
        UNKNOWN = 3,
    };

   public:
    void Load();
    /**
     * @brief GetInt32 retrieves an integer value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetInt32(const std::string& key, int32_t* val,
                  int32_t default_value = 0) const;

    /**
     * @brief GetInt32 retrieves an unsigned integer value from the
     * configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetUInt32(const std::string& key, uint32_t* val,
                   uint32_t default_value = 0) const;

    /**
     * @brief GetInt64 retrieves a 64-bit integer value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetInt64(const std::string& key, int64_t* val,
                  int64_t default_value = 0) const;

    /**
     * @brief GetInt64 retrieves a 64-bit unsigned integer value from the
     * configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetUInt64(const std::string& key, uint64_t* val,
                   uint64_t default_value = 0) const;

    /**
     * @brief GetDurationMs retrieves a duration value from the configuration
     * and converts it to milliseconds.
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value in milliseconds
     * @param default_value Default value to return if the key is not found
     * @note Duration strings may use ms, s, m, or h as suffixes. Bare numbers
     * are interpreted as milliseconds.
     */
    void GetDurationMs(const std::string& key, uint64_t* val,
                       uint64_t default_value = 0) const;

    /**
     * @brief GetDouble retrieves a double value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetDouble(const std::string& key, double* val,
                   double default_value = 0.0) const;

    /**
     * @brief GetFloat retrieves a float value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetFloat(const std::string& key, float* val,
                  float default_value = 0.0f) const;

    /**
     * @brief GetBool retrieves a boolean value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetBool(const std::string& key, bool* val,
                 bool default_value = false) const;

    /**
     * @brief GetString retrieves a string value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetString(const std::string& key, std::string* val,
                   const std::string& default_value = "") const;

    void SetPath(const std::string& path) { path_ = path; }

   private:
    void processNode(const YAML::Node& node, std::string key);

    void processNode(const Json::Value& node, std::string key);

    void loadFromYAML();

    void loadFromJSON();

    bool getValue(const std::string& key, Node* node) const {
        auto it = data_.find(key);
        if (it != data_.end()) {
            *node = it->second;
            return true;
        }
        return false;
    }

   private:
    std::string path_;
    ConfigType type_;
    std::unordered_map<std::string, Node> data_;
};

// Configure yalantinglibs logging with MC_YLT_LOG_LEVEL, MC_YLT_LOG_PATH,
// MC_YLT_LOG_MAX_FILE_SIZE (bytes), and MC_YLT_LOG_MAX_FILES.
inline void init_ylt_log_level() {
    static std::once_flag once;
    std::call_once(once, [] {
        easylog::Severity severity = easylog::Severity::WARN;
        const char* env_level = std::getenv("MC_YLT_LOG_LEVEL");
        if (env_level && *env_level) {
            std::string level_str(env_level);
            std::transform(level_str.begin(), level_str.end(),
                           level_str.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (level_str == "trace") {
                severity = easylog::Severity::TRACE;
            } else if (level_str == "debug") {
                severity = easylog::Severity::DEBUG;
            } else if (level_str == "info") {
                severity = easylog::Severity::INFO;
            } else if (level_str == "warn" || level_str == "warning") {
                severity = easylog::Severity::WARN;
            } else if (level_str == "error") {
                severity = easylog::Severity::ERROR;
            } else if (level_str == "critical") {
                severity = easylog::Severity::CRITICAL;
            }
        }

        std::string log_path = "logs/rpc.log";
        const char* env_log_path = std::getenv("MC_YLT_LOG_PATH");
        if (env_log_path && *env_log_path) {
            log_path = env_log_path;
        }

        auto get_size_env = [](const char* name, size_t default_value) {
            const char* value = std::getenv(name);
            if (!value || !*value) return default_value;

            uint64_t parsed = 0;
            const char* end = value + std::strlen(value);
            const auto result = std::from_chars(value, end, parsed);
            if (result.ec != std::errc() || result.ptr != end ||
                parsed > std::numeric_limits<size_t>::max()) {
                return default_value;
            }
            return static_cast<size_t>(parsed);
        };

        constexpr size_t kDefaultMaxFileSize = 1000ULL * 1024 * 1024;
        constexpr size_t kDefaultMaxFiles = 3;
        const size_t max_file_size = get_size_env(
            "MC_YLT_LOG_MAX_FILE_SIZE", kDefaultMaxFileSize);
        const size_t max_files =
            get_size_env("MC_YLT_LOG_MAX_FILES", kDefaultMaxFiles);

        easylog::init_log(severity, log_path, true, false, max_file_size,
                          max_files, false);
    });
}

}  // namespace mooncake
