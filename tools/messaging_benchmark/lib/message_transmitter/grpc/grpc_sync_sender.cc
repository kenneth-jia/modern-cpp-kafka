#include "../../../include/message_sender_api.h"
#include "../../../utility/JsonText.h"

#include "PerfTestMsg.grpc.pb.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <boost/config.hpp>
#include <boost/dll/alias.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <system_error>


using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using PerfTestMsg::PerfTest;
using PerfTestMsg::PerfTestRequest;
using PerfTestMsg::PerfTestReply;


namespace grpc_sender_namespace {

class GRpcSyncSender {
public:
    explicit GRpcSyncSender(const std::string& serverAddr)
        : _stub(PerfTest::NewStub(grpc::CreateChannel(serverAddr, grpc::InsecureChannelCredentials())))
    {
    }

    using DeliveryCallback = std::function<void(const Status&, const PerfTestReply&)>;

    void send(message_sender_api::MsgPayload msg, DeliveryCallback drCb)
    {
        // Data we are sending to the server.
        PerfTestRequest request;
        request.set_payload(msg.first, msg.second);
      
        ClientContext context;

        PerfTestReply reply;

        Status status = _stub->SendReq(&context, request, &reply);
        if (drCb) drCb(status, reply);
    }

private:
    std::unique_ptr<PerfTest::Stub> _stub;
};


class grpc_sync_sender: public message_sender_api
{
public:
    std::string name() const override { return "grpc sync-sender"; }

    void config(const std::string& senderConfig) override
    {
       try
       {
            JsonText jsonText(senderConfig);
            auto srvAddr = jsonText.getString({"server-address"});

            std::cout << "Configuration: server-address[" << srvAddr << "]" << std::endl;

            _grpcSender = std::make_unique<GRpcSyncSender>(srvAddr);
        }
        catch (const std::exception& e)
        {
            std::cerr << "Configuration failed! Exception: " << e.what() << std::endl;
        }
    }

    void send(MsgPayload msg, DeliveryCb cb) override
    {
        _grpcSender->send(msg, 
                          [cb](const Status& status, const PerfTestReply& reply) {
                              int ec = status.error_code();
                              if (cb) cb(std::error_code(ec, std::system_category()));
                              if (ec) std::cerr << "delivery failure: " << status.error_message() << std::endl;
                          });
    }

    static boost::shared_ptr<grpc_sync_sender> create()
    {
        return boost::make_shared<grpc_sync_sender>();
    }

private:
    std::unique_ptr<GRpcSyncSender> _grpcSender;
};


BOOST_DLL_ALIAS(grpc_sender_namespace::grpc_sync_sender::create, create)

} // namespace grpc_sender_namespace
