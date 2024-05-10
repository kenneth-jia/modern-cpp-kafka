#include "kafka/KafkaConsumer.h"

#include <boost/algorithm/string.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/program_options.hpp>

#include <atomic>
#include <future>
#include <iostream>
#include <signal.h>
#include <string>
#include <vector>


std::atomic_bool runningSwitch = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (runningSwitch)
    {
        runningSwitch = false;
    }
    else
    {
        // Restore the signal handler, -- to avoid stuck with this handler
        signal(SIGINT, SIG_IGN); // NOLINT
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
            ("bootstrap-server",
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

    if (vm.count("help") || argc == 1)
    {
        std::cout << "Read data from a given Kafka topic and write it to the standard output" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << desc << std::endl;
        return nullptr;
    }

    po::notify(vm);

    for (const auto& prop: propList)
    {
        std::vector<std::string> keyValue;
        boost::algorithm::split(keyValue, prop, boost::is_any_of("="));
        if (keyValue.size() != 2)
        {
            throw std::invalid_argument("Unexpected --props value! Expected key=value format");
        }
        args->props[keyValue[0]] = keyValue[1];
    }

    return args;
}

using TimestampValueNs = std::int64_t;

inline static TimestampValueNs
getCurrentTimeNs()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

inline static TimestampValueNs
getRecordTimestampNs(const kafka::clients::consumer::ConsumerRecord& record)
{
    return record.timestamp().msSinceEpoch * 1000000;
}

struct Sample
{
    explicit Sample(std::size_t size = 0, TimestampValueNs sampleTsNs = 0, TimestampValueNs msgTsNs = 0)
        : msgSize(std::move(size)), sampleTimestampNs(std::move(sampleTsNs)), msgTimestampNs(std::move(msgTsNs)) {}

    std::size_t      msgSize;
    TimestampValueNs sampleTimestampNs;
    TimestampValueNs msgTimestampNs;
};

class StatisticInfo
{
public:
    StatisticInfo() {}

    void done(TimestampValueNs ts) { endTs = ts; }

    TimestampValueNs startTimestampNs() const { return startTs; }

    void clear() { auto temp = StatisticInfo(); std::swap(*this, temp); }
    bool empty() const { return msgNum == 0; }

    void update(const Sample& sample)
    {
        if (msgNum == 0) startTs = sample.sampleTimestampNs;

        ++msgNum;
        totalSize += sample.msgSize;
        totalLatencyNs += (sample.sampleTimestampNs - sample.msgTimestampNs);
    }

    void print() const
    {
        if (msgNum == 0 || (endTs - startTs) == 0)
        {
            std::cout << 0 << " MB/s, " << 0 << " nMsg/s";
            std::cout << ", end-to-end latency: " << "-"  << " ms";
            std::cout << std::endl;
            return;
        }

        auto durationMs = (endTs - startTs)/1000000;
        std::cout << 0.001 * totalSize / durationMs << " MB/s, " << 1000.0 *msgNum / durationMs << " nMsg/s";
        std::cout << ", end-to-end latency: " << totalLatencyNs / msgNum / 1000000  << " ms";
        std::cout << std::endl;
    }

private:
    TimestampValueNs startTs = 0;
    TimestampValueNs endTs   = 0;

    unsigned long msgNum          = 0;
    unsigned long totalSize       = 0;
    double        totalLatencyNs  = 0.0;
};

using Queue = boost::lockfree::spsc_queue<Sample>;
constexpr int SPSC_QUEUE_CAPACITY = 1000000;
Queue spscQueue(SPSC_QUEUE_CAPACITY);


void RunConsumer(kafka::clients::Config props,
                 const std::string& topic,
                 std::atomic_bool& running,
                 std::promise<void>& consumerReady)
{
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;

    // Get client id
    std::ostringstream oss;
    oss << "perf-consumer-" << std::this_thread::get_id();
    props.put(ConsumerConfig::CLIENT_ID, oss.str());

    // Create a auto-commit consumer
    KafkaConsumer consumer(props);

    // Subscribe to topic
    consumer.subscribe({topic},
                       [](kafka::clients::consumer::RebalanceEventType et, const kafka::TopicPartitions& tps) {
                           if (et == kafka::clients::consumer::RebalanceEventType::PartitionsAssigned) {
                             std::cout << "partition assigned: " <<  kafka::toString(tps) << std::endl;
                           } else {
                             std::cout << "partition revolked!" << std::endl;
                           }
                       },
                       std::chrono::seconds(60));
    std::cout << "--------------------" << std::endl;

    // Notify other threads
    consumerReady.set_value();

    // Poll & print statistics
    while (running)
    {
        const auto POLL_TIMEOUT = std::chrono::milliseconds(0);
        auto records = consumer.poll(POLL_TIMEOUT);

        if (records.empty()) continue;

        for (const auto& record: records)
        {
            auto now = getCurrentTimeNs();

            if (record.error())
            {
                if (record.error().value() != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                {
                    std::cerr << record.toString() << std::endl;
                }
                continue;
            }

            spscQueue.push(Sample(record.value().size(), now, getRecordTimestampNs(record)));
        }
    }

    consumer.close();
}


void ProcessStatSamples(std::atomic_bool& running)
{
    StatisticInfo oneSecStat;
    StatisticInfo totalStat;

    while (running)
    {
        constexpr long STAT_DISPLAY_INTERVAL_NS = 1000000000;

        auto consumed = spscQueue.consume_all(
           [&oneSecStat, &totalStat, &running](const Sample& sample) {
               auto currentTsNs = sample.sampleTimestampNs;
               totalStat.update(sample);
               oneSecStat.update(sample);
               if (!oneSecStat.empty() && currentTsNs - oneSecStat.startTimestampNs() >= STAT_DISPLAY_INTERVAL_NS) {
                    oneSecStat.done(currentTsNs);
                    oneSecStat.print();
                    oneSecStat.clear();
               }
            });

        if (!oneSecStat.empty() && consumed == 0)
        {
            auto currentTsNs = getCurrentTimeNs();
            if (currentTsNs - oneSecStat.startTimestampNs() >= STAT_DISPLAY_INTERVAL_NS)
            {
                oneSecStat.done(currentTsNs);
                oneSecStat.print();
                oneSecStat.clear();
            }
        }
    }

    auto currentTsNs = getCurrentTimeNs();
    oneSecStat.done(currentTsNs);
    oneSecStat.print();
    totalStat.done(currentTsNs);
    totalStat.print();
}

int main (int argc, char **argv)
{
    // Parse input arguments
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
    if (!args) // Only for "help"
    {
        return EXIT_SUCCESS;
    }

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning); // NOLINT

    using namespace kafka::clients;
    // Prepare consumer properties
    kafka::clients::Config props;
    props.put(kafka::clients::Config::BOOTSTRAP_SERVERS, boost::algorithm::join(args->brokerList, ","));
    // Get client id
    std::ostringstream oss;
    oss << "consumer-" << std::this_thread::get_id();
    props.put(kafka::clients::Config::CLIENT_ID, oss.str());
    // For other properties user assigned
    for (const auto& prop: args->props)
    {
        props.put(prop.first, prop.second);
    }
    // Disable logging
    props.put(Config::LOG_CB, kafka::NullLogger);

    std::promise<void> consumerReady;

    // Start consumer thread
    auto consumerThread = std::unique_ptr<std::thread, std::function<void(std::thread*)>>(
                              new std::thread(RunConsumer, props, args->topic, std::ref(runningSwitch), std::ref(consumerReady)),
                              [](auto t) {if (t->joinable()){ t->join(); }}
                          );

    // Wait until the consumer is ready
    consumerReady.get_future().wait();
    std::cout << "--------------------------------------------" << std::endl;

    // Start statistic processing thread
    auto statProcessThread = std::unique_ptr<std::thread, std::function<void(std::thread*)>>(
                          new std::thread(ProcessStatSamples, std::ref(runningSwitch)),
                          [](auto t) {if (t->joinable()){ t->join(); }}
                      );

    consumerThread.reset();

    statProcessThread.reset();

    return EXIT_SUCCESS;
}

