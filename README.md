# About the `Modern C++ based Kafka API`

## Introduction

The `Modern C++ based Kafka API` (`modern-cpp-kafka`) is a layer of C++ wrapper based on [librdkafka](https://github.com/edenhill/librdkafka) (the C part), with high quality, but more friendly to users.

- By now, `modern-cpp-kafka` is compatible with `librdkafka` **v1.6.0**.

KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by **modern-cpp-kafka**. **modern-cpp-kafka** has no
affiliation with and is not endorsed by The Apache Software Foundation.

## Why it's here

The `librdkafka` is a robust high performance C/C++ library, widely used and well maintained.

Unfortunately, to maintain C++98 compatibility, the C++ interface of `librdkafka` is not quite object-oriented or user-friendly.

Since C++ is evolving quickly, we want to take advantage of new C++ features, thus make the life easier for developers. And this led us to create a new C++ API for Kafka clients.

Eventually, we worked out the `modern-cpp-kafka`, -- a header-only library that uses idiomatic C++ features to provide a safe, efficient and easy to use way of producing and consuming Kafka messages.

## Features

* Headers only

    * Easy to deploy, and no extra library required to link

* Ease of Use

    * Interface/Naming matches the Java API

    * Object-oriented

    * RAII is used for lifetime management

    * librdkafka's polling and queue management is now hidden

* Robust

    * Verified with kinds of test cases, which cover many abnormal scenarios (edge cases)

        * Stability test with unstable brokers

        * Memory leak check for failed client with on-flight messages

        * Client failure and taking over, etc.

* Efficient

    * No extra performance cost (No deep copy introduced internally)

    * Much better (2~4 times throughput) performance result than those native language (Java/Scala) implementation, in most commonly used cases (message size: 256 B ~ 2 KB)


## Build

* No need to build for installation

* To build its `tools`/`tests`/`examples`, you should

    * Specify library locations with environment variables

        * `LIBRDKAFKA_ROOT`                 -- `librdkafka` headers and libraries

        * `GTEST_ROOT`                      -- `googletest` headers and libraries

        * `BOOST_ROOT`                      -- `boost` headers and libraries

        * `SASL_LIBRARYDIR`/`SASL_LIBRARY`  -- if SASL connection support is wanted

    * Create an empty directory for the build, and `cd` to it

    * Build commands

        * Type `cmake path-to-project-root`

        * Type `make` (could follow build options with `-D`)

            * `BUILD_OPTION_USE_ASAN=ON`      -- Use Address Sanitizer

            * `BUILD_OPTION_USE_TSAN=ON`      -- Use Thread Sanitizer

            * `BUILD_OPTION_USE_UBSAN=ON`     -- Use Undefined Behavior Sanitizer

            * `BUILD_OPTION_CLANG_TIDY=ON`    -- Enable clang-tidy checking

            * `BUILD_OPTION_GEN_DOC=ON`       -- Generate documentation as well

            * `BUILD_OPTION_DOC_ONLY=ON`       -- Only generate documentation

            * `BUILD_OPTION_GEN_COVERAGE=ON`  -- Generate test coverage, only support by clang currently

        * Type `make install`

## Install

* Include the `include/kafka` directory in your project

* To work together with `modern-cpp-kafka` API, the compiler should support

    * Option 1: C++17

    * Option 2: C++14 (with pre-requirements)

        * Need `boost` headers (for `boost::optional`)

        * GCC only (with optimization, e.g. -O2)

## To Start

* Tutorial

    * [Debuting a Modern C++ API for Apache Kafka](https://www.confluent.io/blog/modern-cpp-kafka-api-for-safe-easy-messaging)

* User's Manual

    * Kafka Client API

        * [modern-cpp-kafka API](doxygen/annotated.html)

    * Kafka Client Properties

        * In most cases, the `Properties` setting in `modern-cpp-kafka` is identical with [librdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

        * With following exceptions

            * KafkaConsumer

                * More properties than `librdkafka`
                
                    * `max.poll.records` (default: `500`): The maxmum number of records that a single call to `poll()` would return

                * Properties not supposed to be used (internally shadowed by `modern-cpp-kafka`)

                    * `enable.auto.offset.store`

                    * `enable.auto.commit`

                    * `auto.commit.interval.ms`

                * Properties with random string as default

                    * `client.id`

                    * `group.id`

            * KafkaProducer

                * Different default value from `librdkafka`

                    * KafkaSyncProducer

                        * `linger.ms` (default: `0`)
                    
                * Properties with random string as default

                    * `client.id`
    

* Test Environment (ZooKeeper/Kafka cluster) Setup

    * [Start the servers](https://kafka.apache.org/documentation/#quickstart_startserver)

* Guidelines

    * [KafkaProducer Quick Start](doc/KafkaProducerQuickStart.md)

    * [KafkaConsumer Quick Start](doc/KafkaConsumerQuickStart.md)

    * [KafkaClient Configuration]

* How to achieve High Availability & Performance

    * [Kafka Broker Configuration](doc/KafkaBrokerConfiguration.md)

    * [Good Practices to Use KafkaProducer](doc/GoodPracticesToUseKafkaProducer.md)

    * [Good Practices to Use KafkaConsumer](doc/GoodPracticesToUseKafkaConsumer.md)

    * [How to Make KafkaProducer Reliable](doc/HowToMakeKafkaProducerReliable.md)

## Other References

* Java API for Kafka clients

    * [org.apache.kafka.clients.producer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/producer/package-summary.html)

    * [org.apache.kafka.clients.consumer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/package-summary.html)

    * [org.apache.kafka.clients.admin](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/admin/package-summary.html)
