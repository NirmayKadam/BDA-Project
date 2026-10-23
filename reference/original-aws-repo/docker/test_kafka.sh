#!/bin/bash
echo "$(pwd)"

# Check if Kafka is running
nc -zv localhost 9092

if [ $? -eq 0 ]; then
  echo "Kafka is running."
else
  echo "Kafka is not running. Please start Kafka first."
  exit 1
fi

# List all topics
echo -e "\nList of Kafka topics:"
kafka-topics.sh --bootstrap-server localhost:9092 --topic patientvitals