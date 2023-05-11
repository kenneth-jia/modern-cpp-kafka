project("plugins")

set(RAPIDJSON_INCLUDE_DIRS $ENV{RAPIDJSON_INCLUDE_DIRS})
include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIRS})

set(BOOST_INCLUDE_DIRS $ENV{BOOST_ROOT}/include/)
include_directories(SYSTEM ${BOOST_INCLUDE_DIRS})

add_subdirectory(grpc)
add_subdirectory(kafka)
add_subdirectory(stub)
