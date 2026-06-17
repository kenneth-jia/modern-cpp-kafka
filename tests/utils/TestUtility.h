#pragma once

#include "kafka/AdminClient.h"
#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <cstdlib>
#include <errno.h>
#include <functional>
#include <list>
#include <ranges>
#include <regex>
#include <signal.h>
#include <thread>
#include <vector>


#define EXPECT_KAFKA_THROW(expr, err)                                   \
    do {                                                                \
        try {                                                           \
            expr;                                                       \
        } catch (const kafka::KafkaException& e) {                      \
            std::cout << "Exception caught: " << e.what() << std::endl; \
            EXPECT_EQ(err, e.error().value());                          \
            break;                                                      \
        } catch (...){                                                  \
        }                                                               \
        EXPECT_FALSE(true);                                             \
    } while(false)

#define EXPECT_KAFKA_NO_THROW(expr)                                 \
    try {                                                           \
        expr;                                                       \
    } catch (const kafka::KafkaException& e) {                      \
        std::cerr << "Exception caught: " << e.what() << std::endl; \
        EXPECT_FALSE(true);                                         \
    }

#define RETRY_FOR_ERROR(expr, errToRetry, maxRetries)                               \
    for (int cnt = 0; cnt <= (maxRetries); ++cnt) {                                 \
        try {                                                                       \
            expr;                                                                   \
            break;                                                                  \
        } catch (const kafka::KafkaException& e) {                                  \
            if (e.error().value() == (errToRetry) && cnt != (maxRetries)) continue; \
            throw;                                                                  \
        }                                                                           \
    }

#define RETRY_FOR_FAILURE(expr, maxRetries)                         \
    for (int cnt = 0; cnt <= (maxRetries); ++cnt) {                 \
        try {                                                       \
            expr;                                                   \
            break;                                                  \
        } catch (const kafka::KafkaException& e) {                  \
            std::cerr << "Met error: " << e.what() << std::endl;    \
            if (cnt != (maxRetries)) continue;                      \
            throw;                                                  \
        }                                                           \
    }


namespace KafkaTestUtility {

inline void
DumpError(const kafka::Error& error)
{
    // https://en.wikipedia.org/wiki/ANSI_escape_code
    std::cerr << "\033[1;31m" << "[" << kafka::utility::getCurrentTime() << "] ==> Met Error: " << "\033[0m";
    std::cerr << "\033[4;35m" << error.toString() << "\033[0m" << std::endl;
};

inline void
PrintDividingLine(const std::string& description = "")
{
    std::cout << "---------------" << description << "---------------" << std::endl;
}

inline std::optional<std::string>
GetEnvVar(const std::string& name)
{
    if (const auto* value = getenv(name.c_str())) // NOLINT
    {
        return std::string{value};
    }

    return std::optional<std::string>{};
}

inline kafka::Properties
GetKafkaClientCommonConfig()
{
    auto kafkaBrokerListEnv = GetEnvVar("KAFKA_BROKER_LIST");
    EXPECT_TRUE(kafkaBrokerListEnv);
    if (!kafkaBrokerListEnv) return kafka::Properties{};

    kafka::Properties props;
    props.put("bootstrap.servers", *kafkaBrokerListEnv);
    props.put(kafka::clients::Config::BROKER_ADDRESS_FAMILY, "v4");

    if (auto additionalSettingsEnv = GetEnvVar("KAFKA_CLIENT_ADDITIONAL_SETTINGS"))
    {
        auto subRanges = *additionalSettingsEnv | std::views::split(',');
        for (auto subRange: subRanges)
        {
            auto kv = std::string_view(subRange) | std::views::split('=') | std::ranges::to<std::vector<std::string>>();
            EXPECT_EQ(2, kv.size());
            if (kv.size() == 2)
            {
                props.put(kv.at(0), kv.at(1));
            }
            else
            {
                std::cout << "Wrong setting: " << std::string_view(subRange) << std::endl;
            }
        }
    }

    return props;
}

inline std::size_t
GetNumberOfKafkaBrokers()
{
    auto kafkaBrokerListEnv = GetEnvVar("KAFKA_BROKER_LIST");
    if (!kafkaBrokerListEnv) return 0;

    return static_cast<std::size_t>(std::count(kafkaBrokerListEnv->cbegin(), kafkaBrokerListEnv->cend(), ',')) + 1;
}

const auto POLL_INTERVAL             = std::chrono::milliseconds(100);
const auto MAX_POLL_MESSAGES_TIMEOUT = std::chrono::seconds(5);
const auto MAX_OFFSET_COMMIT_TIMEOUT = std::chrono::seconds(15);
const auto MAX_DELIVERY_TIMEOUT      = std::chrono::seconds(5);

inline std::vector<kafka::clients::consumer::ConsumerRecord>
ConsumeMessagesUntilTimeout(kafka::clients::consumer::KafkaConsumer& consumer,
                            std::chrono::milliseconds timeout = MAX_POLL_MESSAGES_TIMEOUT)
{
    std::vector<kafka::clients::consumer::ConsumerRecord> records;

    const auto end = std::chrono::steady_clock::now() + timeout;
    do
    {
        auto polled = consumer.poll(POLL_INTERVAL);

        for (const auto& record: polled)
        {
            auto error = record.error();
            if (!error || error.value() == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;

            std::cerr << "[" << kafka::utility::getCurrentTime() << "] met " << record.toString() << " while polling messages!" << std::endl;
            EXPECT_FALSE(error);
        }

        records.insert(records.end(), std::make_move_iterator(polled.begin()), std::make_move_iterator(polled.end()));
    } while (std::chrono::steady_clock::now() < end);

    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << consumer.name() << " polled "  << records.size() << " messages" << std::endl;

    return records;
}

inline void
WaitUntil(const std::function<bool()>& checkDone, std::chrono::milliseconds timeout)
{
    constexpr int CHECK_INTERVAL_MS = 100;

    const auto end = std::chrono::steady_clock::now() + timeout;

    for (; !checkDone() && std::chrono::steady_clock::now() < end; )
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
    }
}

inline std::vector<kafka::clients::producer::RecordMetadata>
ProduceMessages(const std::string& topic, int partition, const std::vector<std::tuple<kafka::Headers, std::string, std::string>>& msgs)
{
    kafka::clients::producer::KafkaProducer producer(GetKafkaClientCommonConfig().put(kafka::clients::Config::LOG_LEVEL, "1"));

    std::vector<kafka::clients::producer::RecordMetadata> ret;
    for (const auto& msg: msgs)
    {
        auto record = kafka::clients::producer::ProducerRecord(topic, partition, kafka::Key(std::get<1>(msg)), kafka::Value(std::get<2>(msg)));
        record.headers() = std::get<0>(msg);
        auto metadata = producer.syncSend(record);
        ret.emplace_back(metadata);
    }

    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << __FUNCTION__ << ": " << msgs.size() << " messages have been sent to " << topic << "-" << partition << std::endl;
    return ret;
}

inline void
CreateKafkaTopic(const kafka::Topic& topic, int numPartitions, int replicationFactor)
{
    kafka::clients::admin::AdminClient adminClient(GetKafkaClientCommonConfig());
    auto createResult = adminClient.createTopics({topic}, numPartitions, replicationFactor);
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << __FUNCTION__ << ": create topic[" << topic << "] "
       << "with numPartitions[" << numPartitions << "], replicationFactor[" << replicationFactor << "]. Result: " << createResult.error.message() << std::endl;
    ASSERT_FALSE(createResult.error);
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

inline void
WaitMetadataSyncUpBetweenBrokers()
{
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

inline std::vector<int>
getAllBrokersPids()
{
    std::vector<std::string> pidsString;
    if (auto kafkaBrokerPidsEnv = GetEnvVar("KAFKA_BROKER_PIDS"))
    {
        pidsString = *kafkaBrokerPidsEnv | std::views::split(',') | std::ranges::to<std::vector<std::string>>();
    }

    std::vector<int> pids;
    std::ranges::for_each(pidsString, [&pids](const auto& s) { pids.push_back(std::stoi(s)); });
    return pids;
}

#ifndef WIN32
inline void
signalToAllBrokers(int sig)
{
    auto pids = getAllBrokersPids();
    std::ranges::for_each(pids, [sig](int pid) { if (kill(pid, sig) == -1) std::cerr << "kill() failed! error: " << strerror(errno) << std::endl; }); // NOLINT

    if (sig == SIGSTOP)
    {
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Brokers paused"  << std::endl;
    }
    else if (sig == SIGCONT)
    {
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Brokers resumed"  << std::endl;
    }
}
#endif

inline void
PauseBrokers()
{
    constexpr int WAIT_AFTER_PAUSE_MS = 500;
#ifndef WIN32
    signalToAllBrokers(SIGSTOP);
#else
    std::cerr << "[" << kafka::utility::getCurrentTime() << "] Can't pause brokers (doesn't support yet) on windows!"  << std::endl;
#endif
    std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_AFTER_PAUSE_MS));
}

inline void
ResumeBrokers()
{
    constexpr int WAIT_AFTER_RESUME_SEC = 5;
#ifndef WIN32
    signalToAllBrokers(SIGCONT);
#else
    std::cerr << "[" << kafka::utility::getCurrentTime() << "] Can't resume brokers (doesn't support yet) on windows!"  << std::endl;
#endif
    std::this_thread::sleep_for(std::chrono::seconds(WAIT_AFTER_RESUME_SEC));
}

inline std::unique_ptr<std::jthread>
PauseBrokersForAWhile(std::chrono::milliseconds duration)
{
    PauseBrokers();

    return std::make_unique<std::jthread>([duration](){ std::this_thread::sleep_for(duration); ResumeBrokers(); });
}

} // end of namespace KafkaTestUtility

