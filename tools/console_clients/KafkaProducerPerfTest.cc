#define NOMINMAX

#include "kafka/KafkaProducer.h"

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
using namespace kafka::clients::producer;

namespace {


struct Arguments
{
    static void showHelpMessage() {
        std::cout << "Kafka Producer Performance Test" << std::endl;
        std::cout << "    (with librdkafka v" << utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << std::endl;
        std::cout << "Options:" << std::endl;
        std::cout << "  -h, --help                                      Show the help message and exit" << std::endl;
        std::cout << "  --bootstrap-server <BOOTSTRAP-SERVER>           REQUIRED: The server(s) to connect to." << std::endl;
        std::cout << "  --topic <TOPIC>                                 REQUIRED: Produce records to this topic." << std::endl;
        std::cout << "  --num-records <NUM-RECORDS>                     REQUIRED: Number of records to produce." << std::endl;
        std::cout << "  --record-size <RECORD-SIZE>                     REQUIRED: Record size in bytes." << std::endl;   
        std::cout << "  --throughput <THROUGHPUT>                       Throttle maximum record throughput to *approximately* THROUGHPUT records/sec. (default: no throlling)" << std::endl;
        std::cout << "  --command-property <prop1=val1 prop2=val2 ...>  Kafka consumer related configuration properties" << std::endl;
        std::cout << "                                                  Kafka producer related configuration properties." << std::endl;
        std::cout << "  --warmup-records <WARMUP-RECORDS>               The number of records to treat as warmup. (default: 0)" << std::endl;
        std::cout << "  --reporting-interval <INTERVAL-MS>              Interval in milliseconds at which to print progress info. (default: 5000)" << std::endl;
    }

    std::string                         brokerList;
    std::string                         topic;
    std::size_t                         numRecords{};
    int                                 throughput          = -1;
    std::map<std::string, std::string>  props;
    std::size_t                         recordSize{};
    std::size_t                         warmupRecords       = 0;
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
            } else if (arg == "--throughput" && i + 1 < argc) {
                throughput = std::stoi(argv[++i]);
            } else if (arg == "--command-property") {
                while (i + 1 < argc) {
                    const std::string next(argv[i + 1]);
                    if (next.starts_with("--")) break;
                    if (!next.contains('=')) break;
                    ++i;
                    auto eq = next.find('=');
                    props[next.substr(0, eq)] = next.substr(eq + 1);
                }
            } else if (arg == "--warmup-records" && i + 1 < argc) {
                warmupRecords = std::stoull(argv[++i]);
            } else if (arg == "--reporting-interval" && i + 1 < argc) {
                reportingIntervalMs = std::stoi(argv[++i]);
            } else if (arg == "--record-size" && i + 1 < argc) {
                recordSize = std::stoull(argv[++i]);
            } else {
                throw std::invalid_argument("Invalid parameter: " + arg + "!");
            }
        }

        if (brokerList.empty()) throw std::invalid_argument("Missing required --bootstrap-server");
        if (topic.empty())      throw std::invalid_argument("Missing required --topic");
        if (numRecords == 0)    throw std::invalid_argument("Missing required --num-records");
        if (recordSize == 0)    throw std::invalid_argument("Missing required --record-size");

        if (warmupRecords > numRecords) {
            throw std::invalid_argument("error: The value for --warmup-records must be strictly fewer than the number of records in the test, --num-records.");
        }
    }
};

inline std::uint64_t getCurrentTimestampMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch() ).count();
}

struct Stats
{
    static constexpr std::uint64_t MAX_LATENCY_MS = 10000;

    struct PeriodStats
    {
        bool                                    warmup{};

        std::uint64_t                           tsBeginMs{};
        std::uint64_t                           tsEndMs{};

        std::atomic<std::size_t>                msgSent;
        std::atomic<std::size_t>                msgSucceed;
        std::atomic<std::size_t>                msgFailed;

        std::size_t                             msgSizeSum{};

        std::uint64_t                           latencyMsSum{};
        std::uint64_t                           latencyMsMax{};
        std::array<std::size_t, MAX_LATENCY_MS> latencyPerMsCount{};

        void increaseMsgSentCount() { ++msgSent; }

        void update(const RecordMetadata& metadata) {
          ++msgSucceed;

          msgSizeSum += metadata.valueSize();

          const std::uint64_t latencyMs = getCurrentTimestampMs() - metadata.timestamp().msSinceEpoch;
          latencyMsSum += latencyMs;
          latencyMsMax = std::max(latencyMsMax, latencyMs);
          ++latencyPerMsCount.at(std::min(latencyMs, MAX_LATENCY_MS - 1));
        }

        void update(const Error& /*unused*/) { ++msgFailed; }

        std::string toString() const {
            const auto durationSeconds = static_cast<double>(tsEndMs - tsBeginMs) / 1000.0;

            std::optional<int> latencyMsP50;
            std::optional<int> latencyMsP95;
            std::optional<int> latencyMsP99;
            std::optional<int> latencyMsP999;

            if (msgSucceed.load() > 0) {
                const auto indexP50  = static_cast<std::size_t>(static_cast<double>(msgSucceed.load() - 1) * 0.50);
                const auto indexP95  = static_cast<std::size_t>(static_cast<double>(msgSucceed.load() - 1) * 0.95);
                const auto indexP99  = static_cast<std::size_t>(static_cast<double>(msgSucceed.load() - 1) * 0.99);
                const auto indexP999 = static_cast<std::size_t>(static_cast<double>(msgSucceed.load() - 1) * 0.999);

                std::size_t index = 0;
                for (std::size_t i = 0; i < latencyPerMsCount.size(); ++i) {
                    index += latencyPerMsCount.at(i);

                    if (!latencyMsP50 && indexP50 < index)   latencyMsP50 = i;
                    if (!latencyMsP95 && indexP95 < index)   latencyMsP95 = i;
                    if (!latencyMsP99 && indexP99 < index)   latencyMsP99 = i;
                    if (!latencyMsP999 && indexP999 < index) latencyMsP999 = i;
                }
            }

            if (!latencyMsP50)  latencyMsP50 = 0;
            if (!latencyMsP95)  latencyMsP95 = 0;
            if (!latencyMsP99)  latencyMsP99 = 0;
            if (!latencyMsP999) latencyMsP999 = 0;

            return std::format("{:>7} records sent ({:>7} succeed, {:>7} failed)), {:9.1f} records/sec ({:7.3f} MB/sec), {:>4} ms avg latency, {:>4} ms max latency, {:>4} ms 50th, {:>4} ms 95th, {:>4} ms 99th, {:>4} ms 99.9th.",
                msgSent.load(), msgSucceed.load(), msgFailed.load(), static_cast<double>(msgSent)/durationSeconds, static_cast<double>(msgSizeSum)/durationSeconds/1000000, static_cast<int>(msgSent > 0 ? latencyMsSum / msgSent : 0), latencyMsMax, *latencyMsP50, *latencyMsP95, *latencyMsP99, *latencyMsP999);
        }
    };

    static std::shared_ptr<PeriodStats> rollOutNewPeriodStats(bool isWarmingUp) {
        if (_currentStats) {
            _currentStats->tsEndMs = getCurrentTimestampMs();

            const std::scoped_lock lock(_mtxStatsList);
            _statsList.push_back(_currentStats);
        }

        _currentStats = std::make_shared<PeriodStats>();
        _currentStats->warmup = isWarmingUp;
        _currentStats->tsBeginMs = getCurrentTimestampMs();
        return _currentStats;
    }

    static std::shared_ptr<PeriodStats> popStatsReadyForReport() {
        const std::scoped_lock lock(_mtxStatsList);
        if (!_statsList.empty()) {
            auto stats = _statsList.front();
            if (stats->tsEndMs && stats->msgSent == stats->msgSucceed + stats->msgFailed) {
                _statsList.pop_front();
                return stats;
            }
        }

        return nullptr;
    }

    static void completePeriodStats() {
        if (_currentStats) {
            if (_currentStats->msgSent > 0) {
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

            if (stats->warmup) {
                std::cout << "In steady state." << std::endl;
            }
        }
    }
}

} // end of namespace



int main(int argc, char** argv) {
    try {
        const Arguments args{argc, argv};

        ProducerConfig props;
        props.put(Config::BOOTSTRAP_SERVERS, args.brokerList);
        {
            std::ostringstream oss;
            oss << "producer-perf-" << std::this_thread::get_id();
            props.put(Config::CLIENT_ID, oss.str());
        }
        for (const auto& [key, value] : args.props) {
            props.put(key, value);
        }
        props.put(ProducerConfig::MESSAGE_TIMEOUT_MS, std::to_string(Stats::MAX_LATENCY_MS));
        props.put(Config::LOG_CB, NullLogger);

        KafkaProducer producer(props);

        const Topic                  topic{args.topic};
        const std::vector<std::byte> payload(args.recordSize);
        const Key                    key;
        const Value                  value{payload};
        const ProducerRecord         record(topic, key, value);

        std::jthread statsReporter(reportPeriodStats);

        bool isWarmingUp = (args.warmupRecords > 0);
        if (isWarmingUp) {
            std::cout << "Warmup first " << args.warmupRecords << " records. Steady state results will print after the complete test summary." << std::endl;
        }

        const std::uint64_t  startTimestampMs  = getCurrentTimestampMs();

        std::uint64_t  periodStartTimestampMs  = startTimestampMs;

        auto currentStats = Stats::rollOutNewPeriodStats(isWarmingUp);

        std::size_t numSent = 0;
        while (numSent < args.numRecords) {

            std::size_t numToSend = static_cast<std::size_t>(static_cast<double>(getCurrentTimestampMs() - startTimestampMs) / 1000.0 * args.throughput) - numSent;
            numToSend = std::min(numToSend, args.numRecords - numSent);

            for (; numToSend > 0; --numToSend) {
                if (!isWarmingUp) {
                    auto currentTs = getCurrentTimestampMs();
                    if (currentTs - periodStartTimestampMs > args.reportingIntervalMs) {

                        periodStartTimestampMs += args.reportingIntervalMs;

                        currentStats = Stats::rollOutNewPeriodStats(isWarmingUp);
                    }
                }

                producer.send(record,
                              [stats=currentStats](const RecordMetadata& metadata, const Error& error) {
                                if (!error) {
                                    stats->update(metadata);
                                } else {
                                    stats->update(error);
                                }
                              });
                currentStats->increaseMsgSentCount();
                ++numSent;


                if (isWarmingUp && numSent == args.warmupRecords) {

                    isWarmingUp = false;
                    currentStats = Stats::rollOutNewPeriodStats(isWarmingUp);

                    periodStartTimestampMs = getCurrentTimestampMs();
                }
            }
        }

        Stats::completePeriodStats();

        while (!Stats::empty()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        statsReporter.request_stop();

    } catch (const KafkaException& e) {
        std::cerr << "Exception thrown by producer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

