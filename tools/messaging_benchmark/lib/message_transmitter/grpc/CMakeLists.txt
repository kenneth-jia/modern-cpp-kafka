project("grpc-senders")

set(GRPC_INCLUDE_DIR $ENV{GRPC_INCLUDE_DIR})
include_directories(SYSTEM ${GRPC_INCLUDE_DIR})

set(GRPC_LIB_DIR $ENV{GRPC_LIB_DIR})
link_directories(${GRPC_LIB_DIR})

set(PROTOBUF_LIB_DIR $ENV{PROTOBUF_LIB_DIR})
link_directories(${PROTOBUF_LIB_DIR})

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/gen")

# Targets
add_library(perf_test_msg SHARED
    ${CMAKE_CURRENT_SOURCE_DIR}/gen/PerfTestMsg.pb.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen/PerfTestMsg.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/gen/PerfTestMsg.grpc.pb.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen/PerfTestMsg.grpc.pb.cc
)


# Async-sender
add_library(grpc_async_sender SHARED
    grpc_async_sender.cc
)
target_link_libraries(grpc_async_sender
    grpc++_reflection
    grpc
    grpc++
    protobuf
    pthread
    perf_test_msg
)

# Sync-sender
add_library(grpc_sync_sender SHARED
    grpc_sync_sender.cc
)
target_link_libraries(grpc_sync_sender
    grpc++_reflection
    grpc
    grpc++
    protobuf
    pthread
    perf_test_msg
)

# Callback-sender
add_library(grpc_callback_sender SHARED
    grpc_callback_sender.cc
)
target_link_libraries(grpc_callback_sender
    grpc++_reflection
    grpc
    grpc++
    protobuf
    pthread
    perf_test_msg
)


# Async-server
add_executable(grpc_async_server
    grpc_async_server.cc
)
target_link_libraries(grpc_async_server
    grpc++_reflection
    gpr
    grpc
    grpc++
    protobuf
    pthread
    absl_synchronization
    perf_test_msg
)

# Sync-server
add_executable(grpc_sync_server
    grpc_sync_server.cc
)
target_link_libraries(grpc_sync_server
    grpc++_reflection
    gpr
    grpc
    grpc++
    protobuf
    pthread
    absl_synchronization
    perf_test_msg
)

# Callback-server
add_executable(grpc_callback_server
    grpc_callback_server.cc
)
target_link_libraries(grpc_callback_server
    grpc++_reflection
    gpr
    grpc
    grpc++
    protobuf
    pthread
    absl_synchronization
    perf_test_msg
)

