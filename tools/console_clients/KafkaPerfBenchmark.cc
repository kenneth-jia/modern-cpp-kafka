#include "kafka/KafkaProducer.h"

#include <iostream>
#include <ranges>
#include <string>
#include <vector>


using namespace kafka;
using namespace kafka::clients;
using namespace kafka::clients::producer;

namespace {

struct Arguments
{
    std::string                         brokerList;
    std::string                         topic;
    std::optional<Partition>            partition;
    std::map<std::string, std::string>  props;
    std::size_t                         msgSize;
    std::size_t                         msgCount;
};

struct Stats
{
    std::size_t msgDelivered;
    std::size_t msgFailed;
    std::uint64_t latencyMsSum;
    std::size_t msgSizeSum;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv) {
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;
    bool help = false;

    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h") {
            help = true;
        } else if (arg == "--broker-list" && i + 1 < argc) {
            args->brokerList = argv[++i];
        } else if (arg == "--topic" && i + 1 < argc) {
            args->topic = argv[++i];
        } else if (arg == "--partition" && i + 1 < argc) {
            args->partition = std::stoi(argv[++i]);
        } else if (arg == "--props" && i + 1 < argc) {
            propList.emplace_back(argv[++i]);
        } else if (arg == "--size" && i + 1 < argc) {
            args->msgSize = std::stoi(argv[++i]);
        } else if (arg == "--count" && i + 1 < argc) {
            args->msgCount = std::stoi(argv[++i]);
        }
    }

    if (help || argc == 1) {
        std::cout << "Read data from the standard input and send it to the given Kafka topic" << std::endl;
        std::cout << "    (with librdkafka v" << utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << "\nOptions description:" << std::endl;
        std::cout << "  --help,-h                 Print usage information." << std::endl;
        std::cout << "  --broker-list BROKERS     REQUIRED: The server(s) to connect to." << std::endl;
        std::cout << "  --topic TOPIC             REQUIRED: The topic to publish to." << std::endl;
        std::cout << "  --size MESSAGE_SIZE       REQUIRED: The message size." << std::endl;
        std::cout << "  --count MESSAGE_COUNT     REQUIRED: The number of messages to publish." << std::endl;
        std::cout << "  --partition PARTITION     The partition to publish to." << std::endl;
        std::cout << "  --props KEY=VALUE         Kafka producer properties in key=value format." << std::endl;
        return nullptr;
    }

    for (const auto& prop: propList) {
        auto keyValue = prop | std::views::split('=') | std::ranges::to<std::vector<std::string>>();
        if (keyValue.size() != 2) {
            throw std::invalid_argument("Unexpected --props value! Expected key=value format");
        }
        args->props[keyValue.at(0)] = keyValue.at(1);
    }

    return args;
}

std::int64_t getCurrentTimestampMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

} // end of namespace

int main (int argc, char **argv) {

    try {

        std::unique_ptr<Arguments> args;
        args = ParseArguments(argc, argv);
        if (!args) return EXIT_SUCCESS;

        ProducerConfig props;
        props.put(Config::BOOTSTRAP_SERVERS, args->brokerList);
        std::ostringstream oss;
        oss << "producer-" << std::this_thread::get_id();
        props.put(Config::CLIENT_ID, oss.str());
        for (const auto& prop: args->props) {
            props.put(prop.first, prop.second);
        }
        props.put(Config::LOG_CB, NullLogger);

        KafkaProducer producer(props);

        const Topic                      topic{args->topic};
        const std::optional<Partition>   partitionOption{args->partition};
        const std::vector<std::byte>     payload{args->msgSize};
        const Key                        key;
        const Value                      value{payload};
        const producer::ProducerRecord   record
            = (partitionOption ? producer::ProducerRecord(topic, *partitionOption, key, value) : producer::ProducerRecord(topic, key, value));

        std::size_t numSent = 0;
        std::size_t numAck = 0;
        Stats       stats{};

        const std::int64_t startTimestampMs = getCurrentTimestampMs();
        int seconds = 0;

        auto printPerSecondStats = [&]() {
            std::cout << stats.msgDelivered << " delivered, " << stats.msgFailed << " failed. " << stats.msgSizeSum/100000.0 << " MB/s, avg latency: "  << (stats.msgDelivered ? stats.latencyMsSum / stats.msgDelivered : 0) << " ms" << std::endl;
        };

        auto deliveryCb = [&](const RecordMetadata& metadata, const Error& error) {
                                ++numAck;

                                auto currentTimestampMs = getCurrentTimestampMs();
                                if (currentTimestampMs - startTimestampMs > 1000 * (seconds + 1)) {
                                    printPerSecondStats();
                                    ++seconds;
                                    stats = Stats{};
                                }

                                if (!error) {
                                    auto latencyMs = currentTimestampMs - metadata.timestamp().msSinceEpoch;
                                    stats.msgDelivered++;
                                    stats.latencyMsSum += latencyMs;
                                    stats.msgSizeSum += metadata.valueSize();
                                } else {
                                    stats.msgFailed++;
                                    std::cerr << "% Message delivery failed: " << error.message() << std::endl;
                                }
        };

        for (std::size_t i = 0; i < args->msgCount; ++i) {
            producer.send(record, deliveryCb);
            ++numSent;
        }


        while (numAck != numSent) {
            std::this_thread::yield();
        }

    } catch (const KafkaException& e) {
        std::cerr << "Exception thrown by producer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

