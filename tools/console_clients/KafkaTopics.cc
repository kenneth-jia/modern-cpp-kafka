#include "kafka/AdminClient.h"

#include <iostream>
#include <ranges>
#include <string>
#include <vector>


namespace {

struct Arguments
{
    enum class OpType { Create, Delete, List };

    std::string brokerList;
    std::string topic;
    OpType      opType{};
    int         partitions{};
    int         replicationFactor{};

    kafka::Properties adminConfig;
    kafka::Properties topicProps;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> adminConfigList;
    std::vector<std::string> topicPropList;
    bool help = false;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h")
        {
            help = true;
        }
        else if (arg == "--bootstrap-server" && i + 1 < argc)
        {
            args->brokerList = argv[++i];
        }
        else if (arg == "--admin-config" && i + 1 < argc)
        {
            adminConfigList.emplace_back(argv[++i]);
        }
        else if (arg == "--list")
        {
            args->opType = Arguments::OpType::List;
        }
        else if (arg == "--create")
        {
            args->opType = Arguments::OpType::Create;
        }
        else if (arg == "--delete")
        {
            args->opType = Arguments::OpType::Delete;
        }
        else if (arg == "--topic" && i + 1 < argc)
        {
            args->topic = argv[++i];
        }
        else if (arg == "--partitions" && i + 1 < argc)
        {
            args->partitions = std::stoi(argv[++i]);
        }
        else if (arg == "--replication-factor" && i + 1 < argc)
        {
            args->replicationFactor = std::stoi(argv[++i]);
        }
        else if (arg == "--topic-props" && i + 1 < argc)
        {
            topicPropList.emplace_back(argv[++i]);
        }
    }

    if (help || argc == 1)
    {
        std::cout << "This tool helps in Kafka topic operations" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << "\nOptions description:" << std::endl;
        std::cout << "  --help,-h                       Print usage information." << std::endl;
        std::cout << "  --bootstrap-server BROKERS      REQUIRED: The Kafka cluster broker list." << std::endl;
        std::cout << "  --admin-config KEY=VALUE        Properties for the Admin Client." << std::endl;
        std::cout << "  --list                          List topics." << std::endl;
        std::cout << "  --create                        Create a topic." << std::endl;
        std::cout << "  --delete                        Delete a topic." << std::endl;
        std::cout << "  --topic TOPIC                   Topic name (required for create/delete)." << std::endl;
        std::cout << "  --partitions NUM                Partitions number (required for create)." << std::endl;
        std::cout << "  --replication-factor NUM        Replication factor (required for create)." << std::endl;
        std::cout << "  --topic-props KEY=VALUE         Properties for the topic (for create)." << std::endl;
        return nullptr;
    }

    for (const auto& prop: adminConfigList)
    {
        auto keyValue = prop | std::views::split('=') | std::ranges::to<std::vector<std::string>>();
        if (keyValue.size() != 2)
        {
            throw std::invalid_argument("Wrong option for --admin-config! MUST follow with key=value format!");
        }
        args->adminConfig.put(keyValue.at(0), keyValue.at(1));
    }

    auto countOps = (args->opType == Arguments::OpType::List ? 1 : 0)
                  + (args->opType == Arguments::OpType::Create ? 1 : 0)
                  + (args->opType == Arguments::OpType::Delete ? 1 : 0);
    if (countOps != 1)
    {
        throw std::invalid_argument("MUST choose exactly one operation from '--list/--create/--delete'");
    }

    switch (args->opType)
    {
        case Arguments::OpType::List:
            if (!args->topic.empty() || args->partitions != 0 || args->replicationFactor != 0 || !topicPropList.empty())
            {
                throw std::invalid_argument("The --list operation CANNOT take any '--topic/--partitions/--replication-factor/--topic-props' option!");
            }
            break;
        case Arguments::OpType::Create:
            if (args->topic.empty() || args->partitions == 0 || args->replicationFactor == 0)
            {
                throw std::invalid_argument("The --create operation MUST be with '--topic/--partitions/--replication-factor' options!");
            }

            for (const auto& prop: topicPropList)
            {
                auto keyValue = prop | std::views::split('=') | std::ranges::to<std::vector<std::string>>();
                if (keyValue.size() != 2)
                {
                    throw std::invalid_argument("Wrong option for --topic-props! MUST follow with key=value format!");
                }
                args->topicProps.put(keyValue.at(0), keyValue.at(1));
            }

            break;
        case Arguments::OpType::Delete:
            if (args->topic.empty())
            {
                throw std::invalid_argument("The --delete operation MUST be with '--topic' option!");
            }
            if (args->partitions != 0 || args->replicationFactor != 0 || !topicPropList.empty())
            {
                throw std::invalid_argument("The --delete operation CANNOT take any of '--partitions/--replication-factor/--topic-props' options!");
            }
            break;
    }
    return args;
}

} // end of namespace

int main (int argc, char **argv)
{
    using namespace kafka::clients;
    using namespace kafka::clients::admin;

    try
    {
        std::unique_ptr<Arguments> args;
        args = ParseArguments(argc, argv);
        if (!args) return EXIT_SUCCESS;

        kafka::Properties adminConf = args->adminConfig;
        adminConf.put(Config::BOOTSTRAP_SERVERS, args->brokerList);

        AdminClient adminClient(adminConf);

        if (args->opType == Arguments::OpType::List)
        {
            auto listResult = adminClient.listTopics();
            if (listResult.error)
            {
                std::cerr << "Error: " << listResult.error.message() << std::endl;
                return EXIT_FAILURE;
            }

            for (const auto& topic: listResult.topics)
            {
                std::cout << topic << std::endl;
            }
        }
        else if (args->opType == Arguments::OpType::Create)
        {
            auto createResult = adminClient.createTopics({args->topic}, args->partitions, args->replicationFactor, args->topicProps);
            if (createResult.error)
            {
                std::cerr << "Error: " << createResult.error.message() << std::endl;
                return EXIT_FAILURE;
            }
        }
        else
        {
            auto deleteResult = adminClient.deleteTopics({args->topic});
            if (deleteResult.error)
            {
                std::cerr << "Error: " << deleteResult.error.message() << std::endl;
                return EXIT_FAILURE;
            }
        }
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
