#include "Benchmark.h"

#include "../utility/JsonText.h"

#include <boost/dll/import.hpp>


void
Benchmark::loadMsgSender(const std::string& libName, const std::string& senderConfig, std::size_t numOfSenders)
{
    if (!_msgSenders.empty())
    {
        _msgSenders.clear();
        _msgSenderCreator.clear();
    }

    try
    {
        boost::dll::fs::path lib_path(DEFAULT_MESSAGE_SENDER_LIB_PATH);
        const std::string SYMBOL_NAME = "create";

        _msgSenderCreator = boost::dll::import_alias<MessageSenderCreator>(
            lib_path / libName,
            SYMBOL_NAME,
            boost::dll::load_mode::append_decorations
        );

        for (std::size_t i = 0; i < numOfSenders; ++i)
        {
            _msgSenders.emplace_back(_msgSenderCreator());
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Failed to load the sender[" << libName << "]! Error: " << e.what() << std::endl;
        return;
    }

    std::cout << "Message sender loaded: " << _msgSenders.front()->name() << std::endl;

    try
    {
        for (auto& sender: _msgSenders)
        {
            sender->config(senderConfig);
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Failed to config the sender! Error: " << e.what() << std::endl;
    }
}

void
Benchmark::loadTestRunner(const std::string& libName, const std::string& testConfig)
{
    if (_testRunner)
    {
        _testRunner.reset();
        _testRunnerCreator.clear();
    }

    try
    {
        boost::dll::fs::path lib_path(DEFAULT_TEST_RUNNER_LIB_PATH);
        const std::string SYMBOL_NAME = "create";

        _testRunnerCreator = boost::dll::import_alias<TestRunnerCreator>(
            lib_path / libName,
            SYMBOL_NAME,
            boost::dll::load_mode::append_decorations
        );
        _testRunner = _testRunnerCreator();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Failed to load test-runner! Error: " << e.what() << std::endl;
        return;
    }

    try
    {
        _testRunner->config(testConfig);
        _testConfig = std::make_shared<std::string>(testConfig);
    }
    catch (const std::exception& e)
    {
        std::cerr << "Failed to configure the test! Error: " << e.what() << std::endl;
    }
}

void
Benchmark::run()
{
    if (_msgSenders.empty())
    {
        std::cerr << "No message-sender has been loaded! Please load message-sender first!" << std::endl;
        return;
    }

    if (!_testRunner)
    {
        std::cerr << "No test-runner has been loaded! Please load test-runner first!" << std::endl;
        return;
    }

    if (!_testConfig)
    {
        std::cerr << "The test-runner has not been configurated yet! Please load test-runner with correct configuration first!" << std::endl;
        return;
    }

    _testRunner->run(_msgSenders);

    std::cout << "Result: " << result() << std::endl;
}

std::string
Benchmark::result() const
{
    if (!_testRunner) return "NA";

    return _testRunner->result();
}

void
Benchmark::quit()
{
    _testRunner.reset();
    _testRunnerCreator.clear();

    _msgSenders.clear();
    _msgSenderCreator.clear();

    _testConfig.reset();
}

