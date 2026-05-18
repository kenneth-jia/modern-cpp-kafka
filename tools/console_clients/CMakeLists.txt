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
