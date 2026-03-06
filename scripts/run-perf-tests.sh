#!/bin/bash

# Configuration
KAFKA_VERSION="3.6.1"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_BIN="${KAFKA_DIR}/bin"

# Ensure Kafka is downloaded (simulated, we won't actually download it here)
# In a real environment, you'd download and extract Kafka.
# wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_DIR}.tgz
# tar -xzf ${KAFKA_DIR}.tgz

# Proxy configuration (via env)
export PROXY_PORT=9092
export BACKEND_HOST=localhost
export BACKEND_PORT=9093

echo "Starting Kafka Proxy Performance Test..."

# Run internal Java performance tests
mvn test -Dtest=ProxyPerformanceTest

# Example of how you would run Kafka's performance tools:
# echo "Running Kafka Producer Performance Tool through Proxy..."
# ${KAFKA_BIN}/kafka-producer-perf-test.sh \
#   --topic test-topic \
#   --num-records 100000 \
#   --record-size 1024 \
#   --throughput -1 \
#   --producer-props bootstrap.servers=localhost:9092

echo "Performance Test Completed."
