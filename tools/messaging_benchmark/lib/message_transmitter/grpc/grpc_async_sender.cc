#include "../../../include/message_sender_api.h"
#include "../../../utility/JsonText.h"
#include "../../../utility/Timestamp.h"

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

class GRpcAsyncSender {
public:
    explicit GRpcAsyncSender(const std::string& serverAddr)
        : _stub(PerfTest::NewStub(grpc::CreateChannel(serverAddr, grpc::InsecureChannelCredentials())))
    {
        _pollingThread = std::thread(&GRpcAsyncSender::pollRpcResp, this);
    }

    ~GRpcAsyncSender() { if (_running) close(); }

    void close()
    {
        _running = false;

        if (_pollingThread.joinable()) _pollingThread.join();
    }

    using DeliveryCallback = std::function<void(const Status&, const PerfTestReply&)>;

    struct MsgInfo
    {
        PerfTestReply    reply;
        ClientContext    context;
        Status           status;
        DeliveryCallback drCb;
    };

    void send(message_sender_api::MsgPayload msg, DeliveryCallback drCb)
    {
        PerfTestRequest request;
        request.set_payload(msg.first, msg.second);

        MsgInfo* msgInfo = new MsgInfo;
        msgInfo->drCb = drCb;

        auto rpc = _stub->AsyncSendReq(&msgInfo->context, request, &_cq);

        rpc->Finish(&msgInfo->reply, &msgInfo->status, (void*)msgInfo);

        ++_inFlightCnt;
    }

private:
    void pollRpcResp()
    {
        void* tag = nullptr;
        bool  ok  = false;

        while (_running)
        {
            while (_inFlightCnt && _cq.Next(&tag, &ok))
            {
                if (!tag || !ok) continue;

                auto* msgInfo = static_cast<MsgInfo*>(tag);
                msgInfo->drCb(msgInfo->status, msgInfo->reply);

                delete msgInfo;

                --_inFlightCnt;
            }
        };
    }

    std::unique_ptr<PerfTest::Stub> _stub;
    CompletionQueue                 _cq;
    std::atomic<int>                _inFlightCnt{};
    bool                            _running = true;
    std::thread                     _pollingThread;
};


class grpc_async_sender: public message_sender_api
{
public:
    std::string name() const override { return "grpc-async-sender"; }

    void config(const std::string& senderConfig) override
    {
       try
       {
            JsonText jsonText(senderConfig);

            auto serverAddress = jsonText.getString({"server-address"});

            std::cout << "Configuration: server-address[" << serverAddress << "]" << std::endl;

            _grpcSender = std::make_unique<GRpcAsyncSender>(serverAddress);
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


    static boost::shared_ptr<grpc_async_sender> create()
    {
        return boost::make_shared<grpc_async_sender>();
    }

private:
    std::unique_ptr<GRpcAsyncSender> _grpcSender;
};


BOOST_DLL_ALIAS(grpc_sender_namespace::grpc_async_sender::create, create)

} // namespace grpc_sender_namespace

