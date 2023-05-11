#include "PerfTestMsg.grpc.pb.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <atomic>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>


using grpc::CallbackServerContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerUnaryReactor;
using grpc::Status;
using PerfTestMsg::PerfTest;
using PerfTestMsg::PerfTestRequest;
using PerfTestMsg::PerfTestReply;


namespace grpc_server_namespace {

class GRpcCallbackServer
{
    class PerfTestServiceImpl final: public PerfTest::CallbackService
    {
        ServerUnaryReactor* SendReq(CallbackServerContext* context, const PerfTestRequest* request, PerfTestReply* reply) override
        {
            static int count = 0;
            reply->set_ack(count++);
            ServerUnaryReactor* reactor = context->DefaultReactor();
            reactor->Finish(Status::OK);
            return reactor;
        }
    };

public:
    GRpcCallbackServer(const std::string& serverAddress)
        : _serverAddress(serverAddress)
    {
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();

        _serverBuilder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());

        _serverBuilder.RegisterService(&_callbackService);
    }

    void run()
    {
        _server = _serverBuilder.BuildAndStart();
        std::cout << "Server listening on " << _serverAddress << std::endl;

        _server->Wait();
    }

private:
    const std::string       _serverAddress;
    PerfTestServiceImpl     _callbackService;
    ServerBuilder           _serverBuilder;
    std::unique_ptr<Server> _server;
};


} // namespace grpc_server_namespace

int main(int /*argc*/, char** /*argv*/)
{
    grpc_server_namespace::GRpcCallbackServer server("0.0.0.0:50051");

    server.run();

    return 0;
}

