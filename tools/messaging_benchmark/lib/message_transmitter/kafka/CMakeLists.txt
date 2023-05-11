project("kafka-senders")

set(LIBRDKAFKA_INCLUDE_DIR $ENV{LIBRDKAFKA_INCLUDE_DIR})
include_directories(SYSTEM ${LIBRDKAFKA_INCLUDE_DIR})

set(LIBRDKAFKA_LIBRARY_DIR $ENV{LIBRDKAFKA_LIBRARY_DIR})
link_directories(${LIBRDKAFKA_LIBRARY_DIR})

set(MODERN_CPP_KAFKA_INCLUDE_DIRS $ENV{MODERN_CPP_KAFKA_DIRS}/include)
include_directories(SYSTEM ${MODERN_CPP_KAFKA_INCLUDE_DIRS})

# Targets
add_library("kafka_async_sender" SHARED "kafka_async_sender.cc")
target_link_libraries("kafka_async_sender"
    rdkafka
    pthread
)

add_library("kafka_sync_sender" SHARED "kafka_sync_sender.cc")
target_link_libraries("kafka_sync_sender"
    rdkafka
    pthread
)