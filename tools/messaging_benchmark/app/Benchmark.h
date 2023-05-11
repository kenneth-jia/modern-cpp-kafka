#pragma once


#include "../include/message_sender_api.h"
#include "../include/sender_test_api.h"

#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#include <iostream>
#include <thread>


class Benchmark
{
public:
    void        loadMsgSender(const std::string& libName, const std::string& config, std::size_t numOfSenders = 1);
    void        loadTestRunner(const std::string& libName, const std::string& config);
    void        run();
    void        quit();
    std::string result() const;

private:
    typedef boost::shared_ptr<message_sender_api> (MessageSenderCreator)();
    typedef boost::shared_ptr<sender_test_api>    (TestRunnerCreator)();

    boost::function<MessageSenderCreator> _msgSenderCreator;
    boost::function<TestRunnerCreator>    _testRunnerCreator;

    std::list<boost::shared_ptr<message_sender_api>> _msgSenders;

    boost::shared_ptr<sender_test_api> _testRunner;
    std::shared_ptr<std::string>       _testConfig;

    std::unique_ptr<std::thread, std::function<void(std::thread*)>> _msgSendingThread;

    static constexpr char* DEFAULT_MESSAGE_SENDER_LIB_PATH = "lib/message_transmitter";
    static constexpr char* DEFAULT_TEST_RUNNER_LIB_PATH    = "lib/test_runner";
};
