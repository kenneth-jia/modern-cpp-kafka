#include "Benchmark.h"

#include <boost/algorithm/string.hpp>

#include <signal.h>
#include <sys/time.h>
#include <string>
#include <deque>
#include <iostream>
#include <optional>
#include <cstdlib>
#include <numeric>
#include <queue>
#include <atomic>
#include <utility>


int main()
{
    Benchmark benchmark;

    auto printHelpMessage = []() {
        std::cout << "Available Commands: " << std::endl;
        std::cout << "    help (or, h)                                           -- show the help message" << std::endl;
        std::cout << "    quit (or, q)                                           -- quit the application" << std::endl;
        std::cout << "    load message-sender [lib path] [config]                -- load the dynamic library for the message-sender" << std::endl;
        std::cout << "    load multi-message-sender [number] [lib path] [config] -- load a number of message-senders" << std::endl;
        std::cout << "    load test-runner [lib path] [config]                   -- load the dynamic library for the test-runner" << std::endl;
        std::cout << "    run test                                               -- start the test" << std::endl;
        std::cout << "    show result                                            -- show the test result" << std::endl;
    };

    for (bool running = true; running; )
    {
        std::cout << "Please Input Command: (q to quit)" << std::endl;

        std::string command;
        if (!std::getline(std::cin, command))
        {
            std::cerr << "getline failed!" << std::endl;
            running = false;
        }

        std::deque<std::string> inputs;
        boost::algorithm::split(inputs, command, boost::is_any_of(" "));

        if (inputs.size() >= 1 && (inputs[0] == "h" || inputs[0] == "help"))
        {
            printHelpMessage();
        }
        else if (inputs.size() >= 1 && (inputs[0] == "q" || inputs[0] == "quit"))
        {
            running = false;
        }
        else if (inputs.size() >= 3 && inputs[0] == "load" && inputs[1] == "message-sender")
        {
            inputs.pop_front();
            inputs.pop_front();

            const auto libPath = inputs.front();
            inputs.pop_front();

            auto config = boost::algorithm::join(inputs, " ");
            if (config.empty()) config = "{}";

            benchmark.loadMsgSender(libPath, config);
        }
        else if (inputs.size() >= 4 && inputs[0] == "load" && inputs[1] == "multi-message-sender")
        {
            inputs.pop_front();
            inputs.pop_front();

            std::size_t numOfSenders = 1;
            const auto numOfSendersStr = inputs.front();
            try
            {
                numOfSenders = std::stoi(numOfSendersStr);
                if (numOfSenders < 1) numOfSenders = 1;
            }
            catch (...) {}
            inputs.pop_front();

            const auto libPath = inputs.front();
            inputs.pop_front();

            auto config = boost::algorithm::join(inputs, " ");
            if (config.empty()) config = "{}";

            benchmark.loadMsgSender(libPath, config, numOfSenders);
        }
        else if (inputs.size() >= 3 && inputs[0] == "load" && inputs[1] == "test-runner")
        {
            inputs.pop_front();
            inputs.pop_front();

            const auto libPath = inputs.front();
            inputs.pop_front();

            auto config = boost::algorithm::join(inputs, " ");
            if (config.empty()) config = "{}";

            benchmark.loadTestRunner(libPath, config);
        }
        else if  (inputs.size() >= 2 && inputs[0] == "run" && inputs[1] == "test")
        {
            benchmark.run();
        }
        else if  (inputs.size() >= 2 && inputs[0] == "show" && inputs[1] == "result")
        {
            std::cout << benchmark.result() << std::endl;
        }
        else if (!inputs.empty())
        {
            std::cerr << "Unknown command!" << std::endl;
            printHelpMessage();
        }
    }
}

