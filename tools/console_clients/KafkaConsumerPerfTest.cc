#define NOMINMAX

#include "kafka/KafkaConsumer.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>


using namespace kafka;
using namespace kafka::clients;
using namespace kafka::clients::consumer;

namespace {

struct Arguments
{
    static void showHelpMessage() {
        std::cout << "Kafka Consumer Performance Test" << std::endl;
        std::cout << "    (with librdkafka v" << utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << std::endl;
        std::cout << "Options:" << std::endl;
        std::cout << "  -h, --help                                      Show the help message and exit" << std::endl;
        std::cout << "  --bootstrap-server <BOOTSTRAP-SERVER>           REQUIRED: The server(s) to connect to." << std::endl;
        std::cout << "  --topic <TOPIC>                                 REQUIRED: The topic to consume from." << std::endl;
        std::cout << "  --num-records <NUM-RECORDS>                     REQUIRED: The number of records to consume." << std::endl;        
        std::cout << "  --command-property <prop1=val1 prop2=val2 ...>  Kafka consumer related configuration properties" << std::endl;
        std::cout << "  --group <gid>                                   The group id to consume on." << std::endl;
        std::cout << "  --from-latest                                   If the consumer does not already have an established offset to consume from, start with the latest record.)" << std::endl;
        std::cout << R"(  --show-latency                                  Show the latency info (between the "message sent" and "message received")." << std::endl;
        std::cout << "  --reporting-interval <INTERVAL-MS>              Interval in milliseconds at which to print progress info. (default: 5000)" << std::endl;
    }

    std::string                         brokerList;
    std::string                         topic;
    std::size_t                         numRecords{};
    std::map<std::string, std::string>  props;
    std::string                         group;
    bool                                fromLatest{false};
    bool                                showLatency{false};
    std::size_t                         reportingIntervalMs = 5000;

    Arguments(int argc, char** argv) {
        for (int i = 1; i < argc; ++i) {
            const std::string arg(argv[i]);
            if (arg == "--help" || arg == "-h") {
                showHelpMessage();
                std::quick_exit(EXIT_SUCCESS);
            } else if (arg == "--bootstrap-server" && i + 1 < argc) {
                brokerList = argv[++i];
            } else if (arg == "--topic" && i + 1 < argc) {
                topic = argv[++i];
            } else if (arg == "--num-records" && i + 1 < argc) {
                numRecords = std::stoull(argv[++i]);
            } else if (arg == "--command-property") {
                while (i + 1 < argc) {
                    const std::string next(argv[i + 1]);
                    if (next.starts_with("--")) break;
                    if (!next.contains('=')) break;
                    ++i;
                    auto eq = next.find('=');
                    props[next.substr(0, eq)] = next.substr(eq + 1);
                }
            } else if (arg == "--group" && i + 1 < argc) {
                group = argv[++i];
            } else if (arg == "--from-latest") {
                fromLatest = true;
            } else if (arg == "--show-latency") {
                showLatency = true;
            } else if (arg == "--reporting-interval" && i + 1 < argc) {
                reportingIntervalMs = std::stoi(argv[++i]);
            } else {
                throw std::invalid_argument("Invalid parameter: " + arg + "!");
            }
        }

        if (brokerList.empty()) throw std::invalid_argument("Missing required --bootstrap-server");
        if (topic.empty())      throw std::invalid_argument("Missing required --topic");
        if (numRecords == 0)    throw std::invalid_argument("Missing required --num-records");
    }
};

inline std::uint64_t getCurrentTimestampMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch() ).count();
}

struct Stats
{
    static inline bool withLatencyInfo{false};

    static std::string header() {
        return withLatencyInfo ? "start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec" : "start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, avg.latency.ms, max.latency.ms";
    }

    struct PeriodStats
    {
        std::uint64_t   tsBeginMs{};
        std::uint64_t   tsEndMs{};

        std::size_t     msgReceived{};
        std::size_t     bytesReceived{};

        std::uint64_t   latencyMsSum{};
        std::uint64_t   latencyMsMax{};

        void update(const ConsumerRecord& record, std::uint64_t ts) {
          ++msgReceived;
          bytesReceived += record.value().size();

          const std::uint64_t latencyMs = ts - record.timestamp().msSinceEpoch;
          latencyMsSum += latencyMs;
          latencyMsMax = std::max(latencyMsMax, latencyMs);
        }

        static std::string toDateString(std::uint64_t ts) {
            using namespace std::chrono;
            const auto tp = system_clock::time_point(milliseconds(ts));
            return std::format("{0:%Y-%m-%d %H:%M:%S}.{1:03}",  floor<seconds>(tp), ts % 1000);
        }

        std::string toString() const {
            const double durationSeconds = static_cast<double>(tsEndMs - tsBeginMs) / 1000;
            auto ret = std::format("{}, {}, {:7.3f}, {:7.3f}, {:>10}, {:10.1f}",
                toDateString(tsBeginMs),
                toDateString(tsEndMs),
                static_cast<double>(bytesReceived)/1000000,
                tsEndMs > tsBeginMs ? static_cast<double>(bytesReceived)/1'000'000/durationSeconds : 0,
                msgReceived,
                tsEndMs > tsBeginMs ? static_cast<double>(msgReceived)/durationSeconds : 0);
            if (withLatencyInfo) ret += std::format(", {:>4}, {:>4}", msgReceived > 0 ? static_cast<int>(static_cast<double>(latencyMsSum)/static_cast<double>(msgReceived)) : 0, latencyMsMax);

            return ret;
        }
    };

    static std::shared_ptr<PeriodStats> rollOutNewPeriodStats() {
        if (_currentStats) {
            _currentStats->tsEndMs = getCurrentTimestampMs();

            const std::scoped_lock lock(_mtxStatsList);
            _statsList.push_back(_currentStats);
        }

        _currentStats = std::make_shared<PeriodStats>();
        _currentStats->tsBeginMs = getCurrentTimestampMs();
        return _currentStats;
    }

    static std::shared_ptr<PeriodStats> popStatsReadyForReport() {
        const std::scoped_lock lock(_mtxStatsList);
        if (!_statsList.empty()) {
            auto stats = _statsList.front();
            _statsList.pop_front();
            return stats;
        }

        return nullptr;
    }

    static void completePeriodStats() {
        if (_currentStats) {
            if (_currentStats->msgReceived > 0) {
                _currentStats->tsEndMs = getCurrentTimestampMs();

                const std::scoped_lock lock(_mtxStatsList);
                _statsList.push_back(_currentStats);
            }

            _currentStats.reset();
        }
    }

    static bool empty() {
        if (_currentStats) return false;

        const std::scoped_lock lock(_mtxStatsList);
        return _statsList.empty();
    }

private:
    inline static std::mutex _mtxStatsList;
    inline static std::list<std::shared_ptr<PeriodStats>> _statsList;
    inline static std::shared_ptr<PeriodStats> _currentStats;
};


void reportPeriodStats(const std::stop_token& stopToken) {
    while (!stopToken.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        while (auto stats = Stats::popStatsReadyForReport()) {
            std::cout << stats->toString() << std::endl;
        }
    }
}

} // end of namespace



int main(int argc, char** argv) {
    try {
        const Arguments args{argc, argv};

        Stats::withLatencyInfo = args.showLatency;

        ConsumerConfig props;
        props.put(Config::BOOTSTRAP_SERVERS, args.brokerList);
        if (!args.group.empty()) {
            props.put(ConsumerConfig::GROUP_ID, args.group);

        } else {
            std::ostringstream oss;
            oss << "consumer-perf-" << std::this_thread::get_id();
            props.put(ConsumerConfig::GROUP_ID, oss.str());
        }
        for (const auto& [key, value] : args.props) {
            props.put(key, value);
        }
        props.put(ConsumerConfig::AUTO_OFFSET_RESET, args.fromLatest ? "latest" : "earliest");
        props.put(Config::LOG_CB, NullLogger);

        KafkaConsumer consumer(props);

        std::jthread statsReporter(reportPeriodStats);

        consumer.subscribe(Topics{args.topic}, 
                           [](RebalanceEventType et, const TopicPartitions& /*tps*/) {
                                if (et == kafka::clients::consumer::RebalanceEventType::PartitionsAssigned) {
                                    std::cout << Stats::header() << std::endl;
                                }
                           });

        const std::uint64_t startTimestampMs  = getCurrentTimestampMs();

        std::uint64_t  periodStartTimestampMs  = startTimestampMs;

        auto currentStats = Stats::rollOutNewPeriodStats();

        std::size_t numReceived = 0;
        while (numReceived < args.numRecords) {
            const auto POLL_INTERVAL = std::chrono::milliseconds(0);

            auto records = consumer.poll(POLL_INTERVAL);
            if (records.empty()) continue;

            auto currentTs = getCurrentTimestampMs();
            if (currentTs - periodStartTimestampMs > args.reportingIntervalMs) {
                periodStartTimestampMs += args.reportingIntervalMs;
                currentStats = Stats::rollOutNewPeriodStats();
            }

            for (const auto& record: records) {
                const auto error = record.error();
                if (error) {
                    if (error.value() != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        std::cerr << "Met error: " << error.message() << std::endl;
                    }

                    continue;
                }

                currentStats->update(record, currentTs);
                ++numReceived;
                if (numReceived >= args.numRecords) break;
            }
        }

        Stats::completePeriodStats();

        while (!Stats::empty()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        statsReporter.request_stop();

    } catch (const KafkaException& e) {
        std::cerr << "Exception thrown by consumer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
