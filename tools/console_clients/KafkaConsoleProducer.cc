#include "kafka/KafkaProducer.h"

#include <iostream>
#include <ranges>
#include <string>
#include <vector>


namespace {

struct Arguments
{
    std::string                         brokerList;
    std::string                         topic;
    std::optional<kafka::Partition>     partition;
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
        else if (arg == "--partition" && i + 1 < argc)
        {
            args->partition = std::stoi(argv[++i]);
        }
        else if (arg == "--props" && i + 1 < argc)
        {
            propList.emplace_back(argv[++i]);
        }
    }

    if (help || argc == 1)
    {
        std::cout << "Read data from the standard input and send it to the given Kafka topic" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << "\nOptions description:" << std::endl;
        std::cout << "  --help,-h                 Print usage information." << std::endl;
        std::cout << "  --broker-list BROKERS     REQUIRED: The server(s) to connect to." << std::endl;
        std::cout << "  --topic TOPIC             REQUIRED: The topic to publish to." << std::endl;
        std::cout << "  --partition PARTITION     The partition to publish to." << std::endl;
        std::cout << "  --props KEY=VALUE         Kafka producer properties in key=value format." << std::endl;
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

} // end of namespace

int main (int argc, char **argv)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;

    try
    {
        std::unique_ptr<Arguments> args;
        args = ParseArguments(argc, argv);
        if (!args) return EXIT_SUCCESS;

        ProducerConfig props;
        props.put(Config::BOOTSTRAP_SERVERS, args->brokerList);
        std::ostringstream oss;
        oss << "producer-" << std::this_thread::get_id();
        props.put(Config::CLIENT_ID, oss.str());
        for (const auto& prop: args->props)
        {
            props.put(prop.first, prop.second);
        }
        props.put(Config::LOG_CB, kafka::NullLogger);

        KafkaProducer producer(props);

        auto startPromptLine = []() { std::cout << "> "; };

        startPromptLine();

        std::string line;
        while (std::getline(std::cin, line))
        {
            const kafka::Key   key;
            const kafka::Value value(line);
            const auto         topic           = args->topic;
            const auto         partitionOption = args->partition;

            const producer::ProducerRecord record =
                (partitionOption ? producer::ProducerRecord(topic, *partitionOption, key, value) : producer::ProducerRecord(topic, key, value));

            std::cout << "Current Local Time [" << kafka::utility::getCurrentTime() << "]" << std::endl;

            const auto metadata = producer.syncSend(record);
            const auto offsetOption = metadata.offset();
            std::cout << "Just Sent Key[" << metadata.keySize()   << " B]/Value["  << metadata.valueSize() << " B]"
                << " ==> " << metadata.topic() << "-" << std::to_string(metadata.partition()) << "@" <<  (offsetOption ? std::to_string(*offsetOption) : "NA")
                << ", " << metadata.timestamp().toString() << ", " << metadata.persistedStatusString() << std::endl;

            std::cout << "--------------------" << std::endl;
            startPromptLine();
        }
    }
    catch (const kafka::KafkaException& e)
    {
        std::cerr << "Exception thrown by producer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
