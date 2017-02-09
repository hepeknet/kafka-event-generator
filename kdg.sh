#!/usr/bin/env bash

if [ $# -lt 2 ]; then
  echo "Usage: kdg <schema-registry-address> <kafka-brokers> [topic-name] [events-per-second] [duration-seconds]"
  echo "Example kdg http://schema-reg:8080 127.0.0.1,127.0.0.2 myTopic 100 60"
  exit 1
fi

java -jar ./target/kafka-event-generator-0.0.1-SNAPSHOT.jar $@