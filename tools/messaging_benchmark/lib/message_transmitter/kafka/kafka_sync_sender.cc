#include "../../../include/message_sender_api.h"
#include "../../../utility/JsonText.h"

#include "kafka/KafkaProducer.h"

#include <boost/config.hpp>
#include <boost/dll/alias.hpp> 
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

#include <iostream>
#include <memory>
#include <string>


namespace kafka_sender_namespace {

class kafka_sync_sender: public message_sender_api
{
public:
    std::string name() const override { return "kafka sync-sender"; }

    void config(const std::string& senderConfig) override
    {
       try
       {
            JsonText jsonText(senderConfig);
            _brokerList = jsonText.getString({"broker-list"});
            _topic      = jsonText.getString({"topic"});

            std::cout << "Configuration: broker-list[" << _brokerList << "], topic[" << _topic << "]" << std::endl;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Configuration failed! Exception: " << e.what() << std::endl;
            return;
        }

        kafka::clients::producer::ProducerConfig props;
        props.put(kafka::clients::producer::ProducerConfig::BOOTSTRAP_SERVERS, _brokerList);
        props.put(kafka::clients::producer::ProducerConfig::CLIENT_ID,         "perftest-sync-producer");
        props.put(kafka::clients::producer::ProducerConfig::MESSAGE_TIMEOUT_MS, "5000");
        props.put(kafka::clients::Config::LOG_CB,                               kafka::NullLogger);
        _producer = std::make_unique<kafka::clients::producer::KafkaProducer>(props);

        auto brokerMetadata = _producer->fetchBrokerMetadata(_topic);
        std::cout << "brokerMetadata: " << (brokerMetadata ? brokerMetadata->toString() : "NA") << std::endl;
    }

    void send(MsgPayload msg, DeliveryCb cb) override
    {
        kafka::clients::producer::ProducerRecord record(_topic, kafka::NullKey, kafka::Value(msg.first, msg.second));

        std::error_code ec;
        try
        {
            _producer->syncSend(record);
        }
        catch (const kafka::KafkaException& e)
        {
            ec = static_cast<std::error_code>(e.error());
            std::cerr << "delivery failure: " << e.what() << std::endl;
        }

        if (cb) cb(ec);
    }


    static boost::shared_ptr<kafka_sync_sender> create()
    {
        return boost::make_shared<kafka_sync_sender>();
    }

private:
    std::string _brokerList;
    std::string _topic;

    std::unique_ptr<kafka::clients::producer::KafkaProducer> _producer;
};


BOOST_DLL_ALIAS(kafka_sender_namespace::kafka_sync_sender::create, create_sender)

} // namespace kafka_sender_namespace
