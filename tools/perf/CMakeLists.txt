project("kafka-perf-tools")


#---------------------------
# boost
#---------------------------
if (DEFINED ENV{BOOST_ROOT})
    set(Boost_INCLUDE_DIRS $ENV{BOOST_ROOT}/include)
    set(Boost_LIBRARY_DIRS $ENV{BOOST_ROOT}/lib)
else ()
    find_package(Boost COMPONENTS program_options REQUIRED)
    if (NOT Boost_FOUND)
        message(FATAL_ERROR "Boost not found!")
    endif ()
endif ()

message(STATUS "boost include directory: ${Boost_INCLUDE_DIRS}")
message(STATUS "boost library directory: ${Boost_LIBRARY_DIRS}")

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


# Target: kafka-end-to-end-latency-test
set(KafkaEndToEndLatencyTest kafka-end-to-end-latency-test)

add_executable("${KafkaEndToEndLatencyTest}" "KafkaEndToEndLatencyTest.cc")
target_link_libraries("${KafkaEndToEndLatencyTest}" modern-cpp-kafka-api boost_program_options)

install(TARGETS "${KafkaEndToEndLatencyTest}" DESTINATION tools)

# Target: kafka-producer-perf-test
set(KafkaProducerPerfTest kafka-producer-perf-test)

add_executable("${KafkaProducerPerfTest}" "KafkaProducerPerfTest.cc")
target_link_libraries("${KafkaProducerPerfTest}" modern-cpp-kafka-api boost_program_options)

install(TARGETS "${KafkaProducerPerfTest}" DESTINATION tools)

