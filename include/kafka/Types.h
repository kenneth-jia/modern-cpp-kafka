#pragma once

#include <kafka/Project.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>




namespace KAFKA_API {

template <typename T>
concept PlainMemoryObject =
    std::is_trivial<T>::value &&
    std::is_standard_layout<T>::value &&
    !std::is_pointer<T>::value &&
    !std::is_reference<T>::value;

class ConstBuffer
{
public:
    explicit ConstBuffer(const std::byte* data = nullptr, std::size_t size = 0) noexcept
        : _buffer(data, size) {}

    explicit ConstBuffer(std::span<const std::byte> buffer)
        : _buffer(buffer) {}

    explicit ConstBuffer(std::string_view sv)
        : _buffer(reinterpret_cast<const std::byte*>(sv.data()), sv.size()) {}      // NOLINT

    ConstBuffer(std::string&&) = delete;

    template <PlainMemoryObject T>
    explicit ConstBuffer(const T& t) noexcept
        : ConstBuffer(reinterpret_cast<const std::byte*>(&t), sizeof(T)) {}         // NOLINT

    template <PlainMemoryObject T>
    ConstBuffer(const T&&) = delete;

    constexpr const std::byte*  data() const noexcept { return _buffer.data(); }
    constexpr std::size_t       size() const noexcept { return _buffer.size(); }

    std::string toString() const
    {
        if (_buffer.empty()) return _buffer.data() ? "[empty]" : "[null]";

        std::ostringstream oss;

        auto printChar = [&oss](const unsigned char c) {
            if (std::isprint(c)) {
                oss << c;
            } else {
                oss << "[0x" << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(c) << "]";
            }
        };
        const auto* beg = reinterpret_cast<const char*>(_buffer.data());            // NOLINT
        std::for_each(beg, beg + _buffer.size(), printChar);

        return oss.str();
    }
private:
    std::span<const std::byte> _buffer;
};


/**
 * Infinite timeout.
 */
const inline std::chrono::milliseconds InfiniteTimeout = std::chrono::milliseconds::max();


/**
 * Topic name.
 */
using Topic     = std::string;

/**
 * Partition number.
 */
using Partition = std::int32_t;

/**
 * Record offset.
 */
using Offset    = std::int64_t;

/**
 * Record key.
 */
using Key       = ConstBuffer;
using KeySize   = std::size_t;

/**
 * Null Key.
 */
const inline Key NullKey = Key{};

/**
 * Record value.
 */
using Value     = ConstBuffer;
using ValueSize = std::size_t;

/**
 * Null Value.
 */
const inline Value NullValue = Value{};

/**
 * Topic set.
 */
using Topics                = std::set<Topic>;

/**
 * Topic Partition pair.
 */
using TopicPartition        = std::pair<Topic, Partition>;

/**
 * TopicPartition set.
 */
using TopicPartitions       = std::set<TopicPartition>;

/**
 * Topic/Partition/Offset tuple
 */
using TopicPartitionOffset  = std::tuple<Topic, Partition, Offset>;

/**
 * TopicPartition to Offset map.
 */
using TopicPartitionOffsets = std::map<TopicPartition, Offset>;


/**
 * Obtains explanatory string for Topics.
 */
inline std::string toString(const Topics& topics)
{
    std::string ret;
    std::ranges::for_each(topics,
                          [&ret](const auto& topic) {
                              ret.append(ret.empty() ? "" : ",").append(topic);
                          });
    return ret;
}

/**
 * Obtains explanatory string for TopicPartition.
 */
inline std::string toString(const TopicPartition& tp)
{
    return tp.first + std::string("-") + std::to_string(tp.second);
}

/**
 * Obtains explanatory string for TopicPartitions.
 */
inline std::string toString(const TopicPartitions& tps)
{
    std::string ret;
    std::ranges::for_each(tps,
                          [&ret](const auto& tp) {
                              ret.append((ret.empty() ? "" : ",") + tp.first + "-" + std::to_string(tp.second));
                          });
    return ret;
}

/**
 * Obtains explanatory string for TopicPartitionOffset.
 */
inline std::string toString(const TopicPartitionOffset& tpo)
{
    return std::get<0>(tpo) + "-" + std::to_string(std::get<1>(tpo)) + ":" + std::to_string(std::get<2>(tpo));
}

/**
 * Obtains explanatory string for TopicPartitionOffsets.
 */
inline std::string toString(const TopicPartitionOffsets& tpos)
{
    std::string ret;
    std::ranges::for_each(tpos,
                          [&ret](const auto& tp_o) {
                              const TopicPartition& tp = tp_o.first;
                              const Offset& o  = tp_o.second;
                              ret.append((ret.empty() ? "" : ",") + tp.first + "-" + std::to_string(tp.second) + ":" + std::to_string(o));
                          });
    return ret;
}

} // end of KAFKA_API

