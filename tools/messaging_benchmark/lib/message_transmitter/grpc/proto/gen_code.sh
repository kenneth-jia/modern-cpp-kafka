
protoc -I ./ --grpc_out=../gen --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` PerfTestMsg.proto
protoc -I ./ --cpp_out=../gen PerfTestMsg.proto