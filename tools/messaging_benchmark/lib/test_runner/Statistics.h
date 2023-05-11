#pragma once

#include "../../utility/Timestamp.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <list>
#include <mutex>
#include <sstream>

namespace test_runner_namespace {

class Statistics
{
public:
    static constexpr unsigned int MAX_LATENCY_US = 1000000;

    Statistics() { std::memset(&_latency, 0, sizeof(_latency)); }

    unsigned long msgTotalCount() const { return _totalNum; }

    void update(std::error_code ec, std::size_t msgSize, TimestampValue tsMsgSent, TimestampValue tsMsgAck)
    {
        const std::lock_guard<std::mutex> lock(_mtx);

        if (_totalNum == 0)
        {
            _startTimestamp = tsMsgSent;
        }
        _endTimestamp = tsMsgAck;

        ++_totalNum;
        _totalSize += msgSize;

        if (ec)
        {
            ++_errorNum;
            _errorSize += msgSize;
        }
        else
        {
            auto latencyUs = tsMsgAck - tsMsgSent;

            _latency.maxUs = std::max(_latency.maxUs, latencyUs);
            if (latencyUs <= MAX_LATENCY_US)
            {
                ++_latency.count[latencyUs];
            }
            else
            {
                _latency.overflowSumUs += latencyUs;
            }
        }
    }

    std::string output() const
    {
        const std::lock_guard<std::mutex> lock(_mtx);
   
        auto successNum  = _totalNum - _errorNum;
        auto successSize = _totalSize - _errorSize;

        unsigned int durationUs = _endTimestamp - _startTimestamp;
        durationUs = (durationUs == 0 ? 1 : durationUs);
    
        auto nMsgPerSecond = 1000000.0 * successNum / durationUs;
        auto nMBPerSecond  = 1.0 * successSize / durationUs;

        unsigned long long latencySum = _latency.overflowSumUs;
        for (unsigned int latency = 0; latency <= MAX_LATENCY_US; ++latency)
        {
            latencySum += latency * _latency.count[latency];
        }

        auto avgLatency = (successNum > 0 ? latencySum / successNum : 0);
  
        int msgIncrCount[MAX_LATENCY_US];
        for (std::size_t i = 0; i < MAX_LATENCY_US; ++i)
        {
            msgIncrCount[i] = _latency.count[i] + (i == 0 ? 0 : msgIncrCount[i - 1]);
        }

        auto lowerBoundFor50th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_US],
                                                  50.0 * successNum / 100);
        auto maxLatencyForFirst50th = std::distance(&msgIncrCount[0], lowerBoundFor50th);

        auto lowerBoundFor95th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_US],
                                                  95.0 * successNum / 100);
        auto maxLatencyForFirst95th = std::distance(&msgIncrCount[0], lowerBoundFor95th);

        auto lowerBoundFor99th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_US],
                                                  99.0 * successNum / 100);
        auto maxLatencyForFirst99th = std::distance(&msgIncrCount[0], lowerBoundFor99th);

        auto lowerBoundFor999th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_US],
                                                   999.0 * successNum / 1000);
        auto maxLatencyForFirst999th = std::distance(&msgIncrCount[0], lowerBoundFor999th);
 
        std::ostringstream oss;
        oss << _totalNum  << " messages sent, " << _errorNum << " messages failed, "
            << nMsgPerSecond << " messages/sec (" << nMBPerSecond << " MB/sec), "
            << avgLatency << " us avg latency, " << _latency.maxUs << " us max latency,";

        std::list<std::pair<unsigned int, std::string>> latencyPairsList =
        {
            { maxLatencyForFirst50th,  " us 50th,"  },
            { maxLatencyForFirst95th,  " us 95th,"  },
            { maxLatencyForFirst99th,  " us 99th,"  },
            { maxLatencyForFirst999th, " us 99.9th." }
        };

        for (const auto& latencyPair: latencyPairsList)
        {
            if (latencyPair.first > MAX_LATENCY_US)
            {
                oss << " >" << MAX_LATENCY_US;
            }
            else
            {
                oss << " " << latencyPair.first;
            }
            oss << latencyPair.second;
        }
        return oss.str();
    }

private:
    std::size_t _totalNum  = 0;
    std::size_t _totalSize = 0;
    std::size_t _errorNum  = 0;
    std::size_t _errorSize = 0;

    TimestampValue _startTimestamp = 0;
    TimestampValue _endTimestamp = 0;

    struct Latency
    {
        std::size_t count[MAX_LATENCY_US + 1];
        std::size_t overflowSumUs;
        std::size_t maxUs;
    } _latency;

    mutable std::mutex _mtx;
};

} // namespace test_runner_namespace
