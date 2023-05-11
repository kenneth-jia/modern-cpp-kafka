project("messaging-benchmark-app")

set(BOOST_INCLUDE_DIRS $ENV{BOOST_ROOT}/include/)
include_directories(SYSTEM ${BOOST_INCLUDE_DIRS})

set(Boost_LIBRARY_DIRS $ENV{BOOST_ROOT}/lib/)
link_directories(${Boost_LIBRARY_DIRS})

set(RAPIDJSON_INCLUDE_DIRS $ENV{RAPIDJSON_INCLUDE_DIRS})
include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIRS})

# Target
add_executable(perf-benchmark-main main.cc Benchmark.cc)
target_link_libraries(perf-benchmark-main pthread boost_filesystem dl)
