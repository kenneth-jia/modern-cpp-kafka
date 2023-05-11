#include "../../../include/message_sender_api.h"
#include "../../../utility/JsonText.h"

#include "PerfTestMsg.grpc.pb.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <boost/config.hpp>
#include <boost/dll/alias.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <system_error>


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using PerfTestMsg::PerfTest;
using PerfTestMsg::PerfTestRequest;
using PerfTestMsg::PerfTestReply;


namespace grpc_sender_namespace {

class GRpcCallbackSender {
public:
    explicit GRpcCallbackSender(const std::string& serverAddr)
        : _stub(PerfTest::NewStub(grpc::CreateChannel(serverAddr, grpc::InsecureChannelCredentials())))
    {
    }

    using DeliveryCallback = std::function<void(const Status&)>;

    void send(message_sender_api::MsgPayload msg, DeliveryCallback drCb)
    {
        std::shared_ptr<PerfTestRequest> request = std::make_shared<PerfTestRequest>();
        std::shared_ptr<PerfTestReply>   reply   = std::make_shared<PerfTestReply>();
        std::shared_ptr<ClientContext>   context = std::make_shared<ClientContext>();

        request->set_payload(msg.first, msg.second);

        _stub->async()->SendReq(context.get(), request.get(), reply.get(),
                                [request, reply, context, drCb](Status status) {
                                    drCb(status);
                                });
    }

private:
    std::unique_ptr<PerfTest::Stub> _stub;
};


class grpc_callback_sender: public message_sender_api
{
public:
    std::string name() const override { return "grpc-callback-sender"; }

    void config(const std::string& senderConfig) override
    {
       try
       {
            JsonText jsonText(senderConfig);

            auto serverAddress = jsonText.getString({"server-address"});

            std::cout << "Configuration: server-address[" << serverAddress << "]" << std::endl;

            _grpcSender = std::make_unique<GRpcCallbackSender>(serverAddress);
        }
        catch (const std::exception& e)
        {
            std::cerr << "Configuration failed! Exception: " << e.what() << std::endl;
        }
    }

    void send(MsgPayload msg, DeliveryCb cb) override
    {
        _grpcSender->send(msg,
                          [cb](const Status& status) {
                              int ec = status.error_code();
                              if (cb) cb(std::error_code(ec, std::system_category()));
                              if (ec) std::cerr << "delivery failure: " << status.error_message() << std::endl;
                          });
    }


    static boost::shared_ptr<grpc_callback_sender> create()
    {
        return boost::make_shared<grpc_callback_sender>();
    }

private:
    std::unique_ptr<GRpcCallbackSender> _grpcSender;
};


BOOST_DLL_ALIAS(grpc_sender_namespace::grpc_callback_sender::create, create)

} // namespace grpc_sender_namespace

