# kafka-event-generator

Simple random event generator for Kafka. 
It uses Kafka Schema registry to find topics with associated schemas and then generates random events (compliant with schema associated with given topic) and then sends those events to Kafka topic.

Both key and value schemas are supported.

## Requirements

* Java 1.8+

* Maven 3+

## How to build

~~~~
mvn clean package
~~~~

## How to run

In order to start using this generator you have to know: 

* address of your Kafka schema registry
* address of at least one Kafka broker

You always have to specify at least those two values when starting this tool.

Depending on number of parameters *keg* might ask you for extra information.

## Exploratory mode

In this mode maybe you do not know what topics have assigned schemas or you want to decide that interactively. 
You do not have to specify topic name or number of events to send

~~~~
./keg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv>
~~~~

## Non-exploratory mode

In this mode you specify all parameters

~~~~
./keg.sh <kafka-schema-registry-ip-port> <kafka-brokers-csv> <topic-name> <number-of-events-per-second-to-generate> <for-how-many-seconds-to-run-generator>
~~~~