#pragma once

#include <chrono>


using TimestampValue = std::uint64_t;

template<class DurationT = std::chrono::microseconds>
inline TimestampValue getCurrentTimestamp()
{
    using namespace std::chrono;
    return duration_cast<DurationT>(high_resolution_clock::now().time_since_epoch()).count();
}
