#include "kafka/KafkaConsumer.h"
#include "kafka/Timestamp.h"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <atomic>
#include <iostream>
#include <signal.h>
#include <string>
#include <vector>

std::atomic_bool running = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running) {
        running = false;
    } else {
        // Restore the signal handler, -- to avoid stucking with this handler
        signal(SIGINT, SIG_IGN);
    }
}

struct Arguments
{
    std::vector<std::string>           brokerList;
    std::string                        topic;
    std::map<std::string, std::string> props;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;

    namespace po = boost::program_options;
    po::options_description desc("Options description");
    desc.add_options()
            ("help,h",
                "Print usage information.")
            ("broker-list",
                po::value<std::vector<std::string>>(&args->brokerList)->multitoken()->required(),
                "REQUIRED: The server(s) to connect to.")
            ("topic",
                po::value<std::string>(&args->topic)->required(),
                "REQUIRED: The topic to consume from.")
            ("props",
                po::value<std::vector<std::string>>(&propList)->multitoken(),
                "Kafka consumer properties in key=value format.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help") || argc == 1) {
        std::cout << "Read data from a given Kafka topic and print out the statistics info" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::Utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << desc << std::endl;
        return nullptr;
    }

    po::notify(vm);

    for (const auto& prop: propList) {
        std::vector<std::string> keyValue;
        boost::algorithm::split(keyValue, prop, boost::is_any_of("="));
        if (keyValue.size() != 2) {
            throw std::invalid_argument("Unexpected --props value! Expected key=value format");
        }
        args->props[keyValue[0]] = keyValue[1];
    }

    return args;
}

struct StatsInfo
{
    std::chrono::steady_clock::time_point updatedTimepoint = std::chrono::steady_clock::now();
    kafka::Timestamp::Value secSinceEpoch = 0;

    std::size_t totalNum  = 0;
    std::size_t totalSize = 0;

    void print() const {
        std::cout << "[" << kafka::Timestamp::toString(secSinceEpoch * 1000) << "] count: " << std::setw(8) << totalNum << ", total size: " << totalSize << std::endl;
    }

    void reset(kafka::Timestamp::Value sec) {
        secSinceEpoch = sec;
        totalNum      = 0;
        totalSize     = 0;
    }

    void update(const kafka::ConsumerRecord& record) {
        updatedTimepoint = std::chrono::steady_clock::now();

        kafka::Timestamp::Value timestamp = record.timestamp().msSinceEpoch;

        if (timestamp / 1000 != secSinceEpoch) {
            if (totalNum > 0) {
                print();
                reset(timestamp / 1000);
            }
        }

        ++totalNum;
        totalSize += record.value().size();
    }

    void update() {
        if (std::chrono::steady_clock::now() - updatedTimepoint > std::chrono::seconds(5)) {
            updatedTimepoint = std::chrono::steady_clock::now();
            if (totalNum > 0) {
                print();
                reset(0);
            }
        }
    }
};

int main (int argc, char **argv)
{
    // Parse input arguments
    std::unique_ptr<Arguments> args;
    try {
        args = ParseArguments(argc, argv);
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Only for "help"
    if (!args) {
        return EXIT_SUCCESS;
    }

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning);

    // Prepare consumer properties
    kafka::ConsumerConfig props;
    props.put(kafka::ConsumerConfig::BOOTSTRAP_SERVERS, boost::algorithm::join(args->brokerList, ","));
    // For other properties user assigned
    for (const auto& prop: args->props) {
        props.put(prop.first, prop.second);
    }

    StatsInfo statsInfo;

    try {
        // Create a manual-commit consumer
        kafka::KafkaClient::setGlobalLogger(kafka::Logger());
        kafka::KafkaConsumer consumer(props);

        // Subscribe to topic
        consumer.subscribe({args->topic});
        std::cout << "--------------------" << std::endl;

        // Start consumer
        while (running) {
            auto records = consumer.poll(std::chrono::milliseconds(10));
            for (const auto& record: records) {
                if (record.error()) {
                    std::cout << "[" << kafka::Utility::getCurrentTime() << "] met error: " << record.error().toString() << std::endl;
                    continue;
                }

                statsInfo.update(record);
            }

            if (records.empty()) statsInfo.update();
        }
    } catch (const kafka::KafkaException& e) {
        std::cerr << "Exception thrown by consumer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

