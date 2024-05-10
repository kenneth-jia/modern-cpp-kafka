
#include "kafka/KafkaProducer.h"
#include "kafka/Timestamp.h"


#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <signal.h>
#include <sys/time.h>
#include <string>
#include <vector>
#include <iostream>
#include <optional>
#include <cstdlib>
#include <numeric>
#include <queue>
#include <atomic>




bool running = true;

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running)
    {
        char msg[] = "SIGINT received, -- the program is teminated!\n";
        write(STDOUT_FILENO, msg, sizeof(msg) - 1);
        running = false;
    }
    else
    {
        // Restore the signal handler, -- to avoid stucking with this handler
        signal(SIGINT, SIG_IGN);
    }
}

struct Arguments
{
    std::vector<std::string> brokerList;
    std::string              topic;
    std::string              kerberosServiceName;
    unsigned int             numRecords;
    unsigned int             recordSize;
    unsigned int             throughput;
    unsigned int             timeoutSec;
    std::map<std::string, std::string> props;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;

    namespace po = boost::program_options;
    po::options_description desc("Optional arguments");
    desc.add_options()
            ("help,h",
                "Print usage information.")
            ("bootstrap-server",
                po::value<std::vector<std::string>>(&args->brokerList)->multitoken()->required(),
                "REQUIRED: The server(s) to connect to.")
            ("topic",
                po::value<std::string>(&args->topic)->required(),
                "REQUIRED: The topic to produce messages to.")
            ("num-records",
                po::value<unsigned int>(&args->numRecords)->required(),
                "REQUIRED: The number of messages to produce.")
            ("record-size",
                po::value<unsigned int>(&args->recordSize)->required(),
                "REQUIRED: The message size in bytes.")
            ("throughput",
                po::value<unsigned int>(&args->throughput)->required(),
                "REQUIRED: Throttle maximum message throughput to *approximately* THROUGHPUT messages/sec")
            ("props",
                po::value<std::vector<std::string>>(&propList)->multitoken(),
                "Kafka producer related configuration properties.")
            ("timeout",
                po::value<unsigned int>(&args->timeoutSec)->default_value(0),
                "Maximum seconds for sending messages.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help") || argc == 1)
    {
        std::cout << "This tool helps in performance test for KafkaProducer (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
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
            throw std::invalid_argument("Wrong option for --props! MUST follow with key=value format!");
        }
        args->props[keyValue[0]] = keyValue[1];
    }

    return args;
}

inline static kafka::Timestamp::Value getCurrentTimeMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}



class MsgSendingPool
{
public:
    void push(long n) { numToSend.fetch_add(n); }

    int pop()
    {
        long n = numToSend.exchange(0);
        if (n <= MAX_NUM_TO_POP) return n;
        
        numToSend.fetch_add(n - MAX_NUM_TO_POP);
        return MAX_NUM_TO_POP;
    }

private:
    static constexpr long MAX_NUM_TO_POP = 100000;
    std::atomic<long>     numToSend;
};



class StatisticInfo
{
public:
    static constexpr unsigned int MAX_LATENCY_MS = 10000;

private:
    unsigned long totalNum;
    unsigned long totalSize;

    kafka::Timestamp::Value startTimeMs;
    kafka::Timestamp::Value endTimeMs;

    struct Latency
    {
        unsigned int  count[MAX_LATENCY_MS + 1];
        unsigned long overflowSumMs;
        unsigned int  maxMs;
    } latency;

public:
    StatisticInfo(): totalNum(), totalSize(), startTimeMs(), endTimeMs() { std::memset(&latency, 0, sizeof(latency)); }

    // Update statistic info according the RecordMetadata
    unsigned int update(const kafka::clients::producer::RecordMetadata& metadata)
    {
        auto now = getCurrentTimeMs();

        ++totalNum;
        totalSize += metadata.valueSize();

        if (metadata.timestamp().type != kafka::Timestamp::Type::NotAvailable)
        {
            auto tsSend = metadata.timestamp().msSinceEpoch;
            auto duration = now - tsSend;
            unsigned int latencyMs = duration > 0 ? duration : 0;

            latency.maxMs = std::max(latency.maxMs, latencyMs);
            if (latencyMs <= MAX_LATENCY_MS)
            {
                ++latency.count[latencyMs];
            }
            else
            {
                latency.overflowSumMs += latencyMs;
            }
        }
        updateEndTime(now);

        return totalNum;
    }

    void updateStartTime() { startTimeMs = getCurrentTimeMs(); }

    void updateEndTime(kafka::Timestamp::Value tv) { endTimeMs = tv; }

    void print()
    {
        unsigned int msgsSent = totalNum;

        unsigned int durationMs = (endTimeMs - startTimeMs);
        if (durationMs == 0) durationMs = 1;
        double       nMsgPerSecond = 1000.0 * msgsSent / durationMs;
        double       nMBPerSecond  = 0.001 * totalSize / durationMs;

        unsigned long latencySum = latency.overflowSumMs;
        for (unsigned int latencyMs = 0; latencyMs <= MAX_LATENCY_MS; ++latencyMs)
        {
            latencySum += latencyMs * latency.count[latencyMs];
        }

        double       avgLatency   = latencySum / msgsSent;
        unsigned int maxLatency   = latency.maxMs;
        unsigned int latencyLimit = MAX_LATENCY_MS;

        int msgIncrCount[MAX_LATENCY_MS];
        for (unsigned int i = 0; i < MAX_LATENCY_MS; ++i)
        {
            msgIncrCount[i] = latency.count[i] + (i == 0 ? 0 : msgIncrCount[i - 1]);
        }

        auto lowerBoundFor50th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_MS],
                                                  50.0 * msgsSent / 100);
        unsigned int maxLatencyForFirst50th = std::distance(&msgIncrCount[0], lowerBoundFor50th);

        auto lowerBoundFor95th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_MS],
                                                  95.0 * msgsSent / 100);
        unsigned int maxLatencyForFirst95th = std::distance(&msgIncrCount[0], lowerBoundFor95th);

        auto lowerBoundFor99th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_MS],
                                                  99.0 * msgsSent / 100);
        unsigned int maxLatencyForFirst99th = std::distance(&msgIncrCount[0], lowerBoundFor99th);

        auto lowerBoundFor999th = std::lower_bound(&msgIncrCount[0], &msgIncrCount[MAX_LATENCY_MS],
                                                   999.0 * msgsSent / 1000);
        unsigned int maxLatencyForFirst999th = std::distance(&msgIncrCount[0], lowerBoundFor999th);
        
        
        std::cout << msgsSent  << " records sent, "
            << nMsgPerSecond << " records/sec (" << nMBPerSecond << " MB/sec), "
            << avgLatency << " ms avg latency, " << maxLatency << " ms max latency,";

        std::list<std::pair<unsigned int, std::string>> latencyPairsList =
        {
            { maxLatencyForFirst50th,  " ms 50th,"  },
            { maxLatencyForFirst95th,  " ms 95th,"  },
            { maxLatencyForFirst99th,  " ms 99th,"  },
            { maxLatencyForFirst999th, " ms 99.9th." }
        };

        for (const auto& latencyPair: latencyPairsList)
        {
            if (latencyPair.first > latencyLimit)
            {
                std::cout << " >" << latencyLimit;
            }
            else
            {
                std::cout << " " << latencyPair.first;
            }
            std::cout << latencyPair.second;
        }
        std::cout << std::endl;
    }
};




void RunProducer(kafka::clients::producer::KafkaProducer& producer,
                 kafka::clients::producer::ProducerRecord record,
                 MsgSendingPool&                          sendingPool,
                 unsigned int                             numRecords, 
                 StatisticInfo&                           stat)
{
    // The delivery callback will update the statistic info
    auto deliveryCb = [&stat, numRecords, &sendingPool] (const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error)
    {
        if (error)
        {
            std::cout << __FUNCTION__ << ", error[" << error.toString() << "], metadata: " << metadata.toString() << std::endl;
            sendingPool.push(1);
        }
        else
        {
            auto gotNum = stat.update(metadata);

            if (gotNum >= numRecords)
            {
                running = false;
            }
        }
    };

    long msgCnt = 0;
    // Keep sending messages
    while (running)
    {
        auto numToSend = sendingPool.pop();
        if (numToSend)
        {
            for (long i = 0; i < numToSend; ++i)
            {
                if (msgCnt++ == 0)
                {
                    // If it's the first message, we record the start time
                    stat.updateStartTime();
                }

                record.setId(msgCnt);

                producer.send(record, deliveryCb);
            }
        }
    }

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
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    if (!args) // Only for "help"
    {
        return EXIT_SUCCESS;
    }

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning); // NOLINT

    // Prepare producer properties
    kafka::clients::Config props;
    props.put(kafka::clients::Config::BOOTSTRAP_SERVERS, boost::algorithm::join(args->brokerList, ","));
    // Get client id
    std::ostringstream oss;
    oss << "perftest-producer-" << std::this_thread::get_id();
    props.put(kafka::clients::Config::CLIENT_ID, oss.str());
    // For other properties user assigned
    for (const auto& prop: args->props)
    {
        props.put(prop.first, prop.second);
    }
    // Disable logging
    props.put(kafka::clients::Config::LOG_CB, kafka::NullLogger);

    // The place to save statistic counters, etc
    StatisticInfo statInfo;
    // The record-ids of messages waiting to be sent
    MsgSendingPool sendingPool;

    // Prepare the record
    const std::string payload(args->recordSize, 'x');
    kafka::Key   key(nullptr, 0);
    kafka::Value value(payload.c_str(), payload.size());
    kafka::clients::producer::ProducerRecord record(args->topic, key, value);

    // Prepare the producer
    kafka::clients::producer::KafkaProducer producer(props);
    
    // Try to fetch the topic metadata
    for (int i = 0; i < 3; ++i)
    {
        auto metadata = producer.fetchBrokerMetadata(args->topic);
        if (metadata)
        {
            std::cout << "--------------------" << std::endl;
            break;
        }
        if (i == 3)
        {
            std::cerr << "Failed to fetch the metadata for topic: " << args->topic << "!" << std::endl;
            return EXIT_FAILURE;
        }
    }
    
    // Start the sending thread
    std::unique_ptr<std::thread, std::function<void(std::thread*)>> sendingThread(
        new std::thread(RunProducer, std::ref(producer), record, std::ref(sendingPool), args->numRecords, std::ref(statInfo)),
        [](auto t) {if (t->joinable()){ t->join(); }});
    
    // Feeding the sending pool for these producers
    auto start = std::chrono::high_resolution_clock::now();
    unsigned long feedCount = 0;
    do
    {
        unsigned long shouldFeed = 1.0 * std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count() / 1000000 * args->throughput;
        shouldFeed = shouldFeed > args->numRecords ? args->numRecords : shouldFeed;
        sendingPool.push(shouldFeed - feedCount);
        feedCount = shouldFeed;
    } while(running && feedCount < args->numRecords);

    // Quit while timed out
    auto quitWhileTimeout = [&args]() {
        if (args->timeoutSec == 0) return;
            for (int i = args->timeoutSec; i > 0; --i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                if (!running) return;
            }
            std::cout << "Timeout, would quit..." << std::endl;
            running = false;
    };
    std::unique_ptr<std::thread, std::function<void(std::thread*)>> timeoutWaitingThread(
        new std::thread(quitWhileTimeout),
        [](auto t) {if (t->joinable()) { t->join(); }}
    );

      
    // Close the producer
    producer.close();
    
    // Waiting all producers to exit
    sendingThread.reset();

    // Print out the statistic result
    statInfo.print();

    return EXIT_SUCCESS;
}  
