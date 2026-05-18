#include "kafka/KafkaConsumer.h"
#include "kafka/Timestamp.h"

#include <atomic>
#include <iostream>
#include <map>
#include <ranges>
#include <signal.h>
#include <string>
#include <vector>

namespace {

std::atomic_bool running = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running)
    {
        running = false;
    }
    else
    {
        signal(SIGINT, SIG_IGN);        // NOLINT
    }
}

struct Arguments
{
    std::string                         brokerList;
    std::string                         topic;
    std::map<std::string, std::string>  props;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;
    bool help = false;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h")
        {
            help = true;
        }
        else if (arg == "--broker-list" && i + 1 < argc)
        {
            args->brokerList = argv[++i];
        }
        else if (arg == "--topic" && i + 1 < argc)
        {
            args->topic = argv[++i];
        }
        else if (arg == "--props" && i + 1 < argc)
        {
            propList.emplace_back(argv[++i]);
        }
    }

    if (help || argc == 1)
    {
        std::cout << "Read data from a given Kafka topic and write it to the standard output" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << "\nOptions description:" << std::endl;
        std::cout << "  --help,-h                 Print usage information." << std::endl;
        std::cout << "  --broker-list BROKERS     REQUIRED: The server(s) to connect to." << std::endl;
        std::cout << "  --topic TOPIC             REQUIRED: The topic to consume from." << std::endl;
        std::cout << "  --props KEY=VALUE         Kafka consumer properties in key=value format." << std::endl;
        return nullptr;
    }

    for (const auto& prop: propList)
    {

        auto keyValue = prop | std::views::split('=') | std::ranges::to<std::vector<std::string>>();
        if (keyValue.size() != 2)
        {
            throw std::invalid_argument("Unexpected --props value! Expected key=value format");
        }
        args->props[keyValue.at(0)] = keyValue.at(1);
    }

    return args;
}

void RunConsumer(const std::string& topic, const kafka::clients::Config& props)
{
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;

    KafkaConsumer consumer(props);

    consumer.subscribe({topic});
    std::cout << "--------------------" << std::endl;

    while (running)
    {
        const auto POLL_TIMEOUT = std::chrono::milliseconds(100);
        auto records = consumer.poll(POLL_TIMEOUT);
        for (const auto& record: records)
        {
            if (!record.error())
            {
                std::cout << "Current Local Time [" << kafka::utility::getCurrentTime() << "]" << std::endl;
                std::cout << "  Topic    : " << record.topic() << std::endl;
                std::cout << "  Partition: " << record.partition() << std::endl;
                std::cout << "  Offset   : " << record.offset() << std::endl;
                std::cout << "  Timestamp: " << record.timestamp().toString() << std::endl;
                std::cout << "  Headers  : " << kafka::toString(record.headers()) << std::endl;
                std::cout << "  Key   [" << std::setw(4) << record.key().size()   << " B]: " << record.key().toString() << std::endl;
                std::cout << "  Value [" << std::setw(4) << record.value().size() << " B]: " << record.value().toString() << std::endl;
                std::cout << "--------------------" << std::endl;
            }
            else
            {
                std::cerr << record.toString() << std::endl;
            }
        }
    }
}

} // end of namespace

int main (int argc, char **argv)
{
    std::unique_ptr<Arguments> args;
    try
    {
        args = ParseArguments(argc, argv);
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    if (!args)
    {
        return EXIT_SUCCESS;
    }

    signal(SIGINT, stopRunning);        // NOLINT

    using namespace kafka::clients;
    Config props;
    props.put(Config::BOOTSTRAP_SERVERS, args->brokerList);
    std::ostringstream oss;
    oss << "consumer-" << std::this_thread::get_id();
    props.put(Config::CLIENT_ID, oss.str());
    for (const auto& prop: args->props)
    {
        props.put(prop.first, prop.second);
    }
    props.put(Config::LOG_CB, kafka::NullLogger);

    try
    {
        RunConsumer(args->topic, props);
    }
    catch (const kafka::KafkaException& e)
    {
        std::cerr << "Exception thrown by consumer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
