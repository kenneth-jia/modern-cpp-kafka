
#include "../../../include/message_sender_api.h"
#include "../../../utility/JsonText.h"

#include <boost/config.hpp>
#include <boost/dll/alias.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

#include <chrono>
#include <iostream>
#include <optional>
#include <string>
#include <thread>


namespace sender_stub_namespace {

class sender_stub: public message_sender_api
{
public:

    std::string name() const override { return "sender stub"; }

    void config(const std::string& senderConfig) override
    {
        try
        {
            JsonText jsonText(senderConfig);

            _latencyMs = jsonText.getInt({"latency-ms"});
            std::cout << "Configuration: latency-ms["  << *_latencyMs << "]" << std::endl;
        }
        catch (const std::exception& e)
        {
             std::cout << "Configuration: no latency-ms" << std::endl;
        }
    }

    void send(MsgPayload msg, DeliveryCb cb) override
    {
        if (_latencyMs)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(*_latencyMs));
        }

        cb(std::error_code());
    }

    static boost::shared_ptr<sender_stub> create()
    {
        return boost::make_shared<sender_stub>();
    }

private:
    std::optional<int> _latencyMs;
};


BOOST_DLL_ALIAS(sender_stub_namespace::sender_stub::create, create)

} // namespace sender_stub_namespace

