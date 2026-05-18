#pragma once

#include <kafka/Project.h>

#include <kafka/Types.h>

#include <algorithm>
#include <string>
#include <vector>


namespace KAFKA_API {

/**
 * Message Header (with a key value pair)
 */
struct Header
{
    using Key   = std::string;
    using Value = ConstBuffer;

    Header() = default;
    Header(Key k, Value v): key(std::move(k)), value(v) {}

    /**
    * Obtains explanatory string.
    */
    std::string toString() const
    {
        return (key.empty() ? "[null]" : key) + ":" + value.toString();
    }

    Key   key;
    Value value;
};

/**
 * Message Headers.
 */
using Headers = std::vector<Header>;

/**
 * Null Headers.
 */
const inline Headers NullHeaders = Headers{};

/**
 * Obtains explanatory string for Headers.
 */
inline std::string toString(const Headers& headers)
{
    std::string ret;
    std::ranges::for_each(headers,
                          [&ret](const auto& header) {
                              ret.append(ret.empty() ? "" : ",").append(header.toString());
                          });
    return ret;
}

} // end of KAFKA_API

