# kafka-event-generator

Generates random Kafka events (Avro) based on schema found in Schema registry.

This is simple random event generator for Kafka. It uses Kafka Schema registry to find schemas for topics and then
generates random events and sends them to Kafka topic.

## How to build

mvn clean package

## How to run

You have to know the address of your kafka schema registry and address of at least one Kafka broker.
Then you just start kdg.sh script

Depending on number of parameters you pass program might ask you for extra information.

## Exploratory mode

In this mode you do not know what topics have assigned schemas. So you do not specify topic name or number of events to send

./kdg.sh ./kdg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv>

## Non-exploratory mode

In this mode you specify all parameters

./kdg.sh ./kdg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv> <topic-name> <number-of-events-per-second-to-generate> <for-how-many-seconds-to-run-generator>
