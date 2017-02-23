#!/usr/bin/env bash

if [ $# -lt 2 ]; then
  echo ""
  echo "Usage: ./keg.sh <schema-registry-address> <kafka-brokers> [topic-name] [events-per-second] [duration-seconds]"
  echo "For example: ./keg.sh http://schema-reg:8080 127.0.0.1,127.0.0.2 myTopic 100 60"
  echo ""
  exit -1
fi

java -jar ./target/kafka-event-generator-0.0.1-SNAPSHOT.jar $@