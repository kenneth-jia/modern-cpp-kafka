#include "PerfTestMsg.grpc.pb.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>

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


namespace grpc_server_namespace {

class GRpcSyncServer
{
    class PerfTestServiceImpl final : public PerfTest::Service
    {
        Status SendReq(ServerContext* context, const PerfTestRequest* request, PerfTestReply* reply) override
        {
            static int count = 0;
            reply->set_ack(count++);
            return Status::OK;
        }
    };

public:
    GRpcSyncServer(const std::string& serverAddress)
        : _serverAddress(serverAddress)
    {
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();

        _serverBuilder.AddListeningPort(_serverAddress, grpc::InsecureServerCredentials());

        _serverBuilder.RegisterService(&_service);
    }

    void run()
    {
        _server = _serverBuilder.BuildAndStart();
        std::cout << "Server listening on " << _serverAddress << std::endl;

        _server->Wait();
    }

private:
    const std::string       _serverAddress;
    PerfTestServiceImpl     _service;
    ServerBuilder           _serverBuilder;;
    std::unique_ptr<Server> _server;
};

} // namespace grpc_server_namespace


int main(int argc, char** argv)
{
    grpc_server_namespace::GRpcSyncServer server("0.0.0.0:50051");

    server.run();

    return 0;
}

