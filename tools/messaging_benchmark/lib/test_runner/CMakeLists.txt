project("test_runners")

set(LIBRDKAFKA_INCLUDE_DIR $ENV{LIBRDKAFKA_INCLUDE_DIR})
include_directories(SYSTEM ${LIBRDKAFKA_INCLUDE_DIR})

set(LIBRDKAFKA_LIBRARY_DIR $ENV{LIBRDKAFKA_LIBRARY_DIR})
link_directories(${LIBRDKAFKA_LIBRARY_DIR})

set(MODERN_CPP_KAFKA_INCLUDE_DIRS $ENV{MODERN_CPP_KAFKA_DIRS}/include)
include_directories(SYSTEM ${MODERN_CPP_KAFKA_INCLUDE_DIRS})

# Targets
add_library("single_speed_send_test" SHARED "single_speed_send_test.cc")
target_link_libraries("single_speed_send_test"
    rdkafka
    pthread
)
