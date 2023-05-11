#include "../../utility/JsonText.h"

#include "../../include/message_sender_api.h"
#include "../../include/sender_test_api.h"

#include "Statistics.h"

#include <boost/config.hpp>
#include <boost/dll/alias.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

#include <deque>
#include <iostream>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>



namespace test_runner_namespace {

class SingleSpeedPublishTest: public sender_test_api
{
public:
    std::string name() const override
    {
        return "single speed test";
    }

    void config(const std::string& testConfig) override
    {
        JsonText jsonText(testConfig);

        _testConfig.msgPerSec = jsonText.getInt({"messages-per-second"});
        _testConfig.msgSize   = jsonText.getInt({"message-size"});
        _testConfig.msgCount  = jsonText.getInt({"messages-count"});

    }

    void run(std::list<boost::shared_ptr<message_sender_api>>& msgSenders) override;

    std::string result() const override
    {
        return _stats ? _stats->output() : "";
    }

    static boost::shared_ptr<SingleSpeedPublishTest> create()
    {
        return boost::make_shared<SingleSpeedPublishTest>();
    }

private:
    class MsgPool;

    struct TestConfig
    {
        std::size_t threadNum = 1;
        std::size_t msgPerSec = 0;
        std::size_t msgSize = 0;
        std::size_t msgCount = 0;
    };    

    static void feeding(std::atomic<bool>&       running,
                        TestConfig&              testConfig,
                        std::shared_ptr<MsgPool> msgPool);


    static void sending(std::atomic<bool>&            running,
                        message_sender_api::MsgSendFn msgSendFn,
                        std::shared_ptr<MsgPool>      msgPool,
                        std::shared_ptr<Statistics>   stats);


    TestConfig                   _testConfig;

    std::shared_ptr<Statistics>  _stats;

    std::shared_ptr<MsgPool>     _msgSendingPool;
    std::atomic<bool>            _running = false;
};


BOOST_DLL_ALIAS(
    test_runner_namespace::SingleSpeedPublishTest::create,
    create
)


class SingleSpeedPublishTest::MsgPool
{
public:
    void push(std::shared_ptr<std::string> newMsg)
    {
        const std::lock_guard<std::mutex> lock(_mtx);

        _messages.emplace_back(newMsg);
    }

    std::shared_ptr<std::string> pop()
    {
        const std::lock_guard<std::mutex> lock(_mtx);

        if (_messages.empty()) return std::shared_ptr<std::string>();

        auto ret = _messages.front();
        _messages.pop_front();

        return ret;
    }

private:
    std::deque<std::shared_ptr<std::string>> _messages;
    std::mutex _mtx;
};


void
SingleSpeedPublishTest::feeding(std::atomic<bool>&          running,
                    TestConfig&               testConfig,
                    std::shared_ptr<MsgPool>    msgPool)
{
    TimestampValue start = getCurrentTimestamp<std::chrono::microseconds>();

    unsigned long feedCount = 0;

    auto sampleMsg = std::make_shared<std::string>(testConfig.msgSize, '0');
    do
    {
        unsigned long shouldFeed = 1.0 * (getCurrentTimestamp<std::chrono::microseconds>() - start) * testConfig.msgPerSec / 1000000;
        shouldFeed = std::min(shouldFeed, testConfig.msgCount);
        for (std::size_t i = feedCount; i < shouldFeed; ++i)
        {
            msgPool->push(sampleMsg);
        }

        feedCount = shouldFeed;
    } while (running && feedCount < testConfig.msgCount);

}

void
SingleSpeedPublishTest::sending(std::atomic<bool>&            running,
                                message_sender_api::MsgSendFn msgSendFn,
                                std::shared_ptr<MsgPool>      msgPool,
                                std::shared_ptr<Statistics>   stats)
{
    while (running)
    {
        if (auto msgToSend = msgPool->pop())
        {
            auto tsNow = getCurrentTimestamp<std::chrono::microseconds>();

            msgSendFn(std::make_pair<const void*, std::size_t>(msgToSend->c_str(), msgToSend->size()),
                      [msgToSend, stats, tsSent = tsNow](std::error_code ec) {
                        if (ec) {
                            std::cerr << "Message failed to be delivered: " << ec.message() << std::endl;
                        }
                        stats->update(ec, msgToSend->size(), tsSent, getCurrentTimestamp<std::chrono::microseconds>());
                      });
        }
    }
}

void 
SingleSpeedPublishTest::run(std::list<boost::shared_ptr<message_sender_api>>& msgSenders)
{   
    _stats = std::make_shared<Statistics>();
    _msgSendingPool = std::make_shared<MsgPool>();

    _running = true;

    // Start the message-sending thread
    using SendingThread = std::unique_ptr<std::thread, std::function<void(std::thread*)>>;
    std::list<SendingThread> sendingThreads;
    for (auto& msgSender: msgSenders)
    {
        auto msgSendFn= [&msgSender](const message_sender_api::MsgPayload& msg, const message_sender_api::DeliveryCb& cb) {
            msgSender->send(msg, cb);
        };

        sendingThreads.emplace_back(
            new std::thread(SingleSpeedPublishTest::sending, std::ref(_running), msgSendFn, _msgSendingPool, _stats),
            [](auto t) {if (t->joinable()){ t->join(); }});
    }

    // Start the message-feeding thread
    using FeedingThread = std::unique_ptr<std::thread, std::function<void(std::thread*)>>;
    FeedingThread feedThread(
        new std::thread(SingleSpeedPublishTest::feeding, std::ref(_running), std::ref(_testConfig), _msgSendingPool),
        [](auto t) {if (t->joinable()){ t->join(); }});

    int percentage = 0;
    while (_stats->msgTotalCount() < _testConfig.msgCount) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int newPercentage = 100 * _stats->msgTotalCount() / _testConfig.msgCount;
        if (newPercentage > percentage)
        {
            std::cout << std::string(newPercentage - percentage, '.');
            percentage = newPercentage;
        }
    }
    std::cout << std::endl;
    
    _running = false;
}


} // namespace test_runner_namespace
