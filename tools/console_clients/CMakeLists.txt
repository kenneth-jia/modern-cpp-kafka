project("kafka-tools")


# Target: kafka-topics
set(KafkaTopics kafka-topics)

add_executable("${KafkaTopics}" "KafkaTopics.cc")
target_link_libraries("${KafkaTopics}" modern-cpp-kafka-api)

install(TARGETS "${KafkaTopics}" DESTINATION tools)


# Target: kafka-console-consumer
set(KafkaConsoleConsumer kafka-console-consumer)

add_executable("${KafkaConsoleConsumer}" "KafkaConsoleConsumer.cc")
target_link_libraries("${KafkaConsoleConsumer}" modern-cpp-kafka-api)

install(TARGETS "${KafkaConsoleConsumer}" DESTINATION tools)


# Target: kafka-console-producer
set(KafkaConsoleProducer kafka-console-producer)

add_executable("${KafkaConsoleProducer}" "KafkaConsoleProducer.cc")
target_link_libraries("${KafkaConsoleProducer}" modern-cpp-kafka-api)

install(TARGETS "${KafkaConsoleProducer}" DESTINATION tools)


# Target: kafka-producer-perf-test
set(KafkaProducerPerfTest kafka-producer-perf-test)

add_executable("${KafkaProducerPerfTest}" "KafkaProducerPerfTest.cc")
target_link_libraries("${KafkaProducerPerfTest}" modern-cpp-kafka-api)

install(TARGETS "${KafkaProducerPerfTest}" DESTINATION tools)


# Target: kafka-consumer-perf-test
set(KafkaConsumerPerfTest kafka-consumer-perf-test)

add_executable("${KafkaConsumerPerfTest}" "KafkaConsumerPerfTest.cc")
target_link_libraries("${KafkaConsumerPerfTest}" modern-cpp-kafka-api)

install(TARGETS "${KafkaConsumerPerfTest}" DESTINATION tools)
