#include "PerfTestMsg.grpc.pb.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using PerfTestMsg::PerfTest;
using PerfTestMsg::PerfTestRequest;
using PerfTestMsg::PerfTestReply;


class CallData
{
public:
    CallData(PerfTest::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE)
    {
      Proceed();
    }

    void Proceed()
    {
        static int count = 0;

        if (status_ == CREATE)
        {
            status_ = PROCESS;

            service_->RequestSendReq(&ctx_, &request_, &responder_, cq_, cq_, this);
        }
        else if (status_ == PROCESS)
        {
            new CallData(service_, cq_);

            reply_.set_ack(count++);

            status_ = FINISH;

            responder_.Finish(reply_, Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == FINISH);

            delete this;
        }
    }

private:
    PerfTest::AsyncService* service_;
    ServerCompletionQueue*  cq_;
    ServerContext           ctx_;

    PerfTestRequest request_;
    PerfTestReply   reply_;

    ServerAsyncResponseWriter<PerfTestReply> responder_;

    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;
};


namespace grpc_server_namespace {

class GRpcServer
{
public:
    GRpcServer(const std::string& serverAddress)
        : _serverAddress(serverAddress)
    {
        _serverBuilder.AddListeningPort(_serverAddress, grpc::InsecureServerCredentials());

        _serverBuilder.RegisterService(&_asyncService);

        _cq = _serverBuilder.AddCompletionQueue();
     }

    ~GRpcServer()
    {
        _server->Shutdown();

        _cq->Shutdown();
    }

    void run()
    {
        _server = _serverBuilder.BuildAndStart();
        std::cout << "Server listening on " << _serverAddress << std::endl;

        new CallData(&_asyncService, _cq.get());

        void* tag;
        bool  ok;
        while (true)
        {
          bool result = _cq->Next(&tag, &ok);
          if (!result || !ok) continue;

          static_cast<CallData*>(tag)->Proceed();
        }
    }

private:
    const std::string       _serverAddress;
    PerfTest::AsyncService  _asyncService;
    ServerBuilder           _serverBuilder;
    std::unique_ptr<Server> _server;

    std::unique_ptr<ServerCompletionQueue> _cq;

};

} // namespace grpc_server_namespace


int main(int /*argc*/, char** /*argv*/) {

    grpc_server_namespace::GRpcServer server("0.0.0.0:50051");

    server.run();

    return 0;
}

