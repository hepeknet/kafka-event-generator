# kafka-event-generator

Simple random event generator for Kafka. 
It uses Kafka Schema registry to find topics with associated schemas and then generates random events and sends them to Kafka topic.
Both key and value schemas are supported.

## Requirements

Java 1.8+

Maven 3+

## How to build

~~~~
mvn clean package
~~~~

## How to run

You have to know the address of your Kafka schema registry and address of at least one Kafka broker in order to start using this generator.

You always have to specify at least those two values when starting this tool.

Depending on number of parameters kdg might ask you for extra information.

## Exploratory mode

In this mode you do not know what topics have assigned schemas. So you do not specify topic name or number of events to send

~~~~
./kdg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv>
~~~~

## Non-exploratory mode

In this mode you specify all parameters

~~~~
./kdg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv> <topic-name> <number-of-events-per-second-to-generate> <for-how-many-seconds-to-run-generator>
~~~~