package com.github.hepeknet.generator;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.hepeknet.generator.avro.AvroDataGenerator;

public class KafkaEventGenerator {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final int EPS_DEFAULT = 100;
	private static final int DURATION_SEC_DEFAULT = 10;

	private final String[] kafkaBrokerAddresses;
	private int eventsPerSecond = EPS_DEFAULT;
	private int durationSeconds = DURATION_SEC_DEFAULT;
	private final TopicWithSchema topicWithSchema;
	private final SchemaRegistryHandler handler;

	public KafkaEventGenerator(String schemaRegistryAddress, String[] kafkaBrokerAddresses, TopicWithSchema tws) {
		if (StringUtil.isEmpty(schemaRegistryAddress)) {
			throw new IllegalArgumentException("Schema registry address must be provided");
		}
		if (kafkaBrokerAddresses == null || kafkaBrokerAddresses.length == 0) {
			throw new IllegalArgumentException("Kafka broker addresses must be provided");
		}
		if (tws == null) {
			throw new IllegalArgumentException("Topic with schema must be provided");
		}
		this.kafkaBrokerAddresses = kafkaBrokerAddresses;
		this.topicWithSchema = tws;
		handler = new SchemaRegistryHandler(schemaRegistryAddress);
	}

	public void setEventsPerSecond(int eventsPerSecond) {
		this.eventsPerSecond = eventsPerSecond;
	}

	public void setDurationSeconds(int durationSeconds) {
		this.durationSeconds = durationSeconds;
	}

	public void generateAndSend() throws Exception {
		log.debug("kafka broker addresses: {}", Arrays.toString(kafkaBrokerAddresses));
		log.debug("Generating " + eventsPerSecond + " events per second and sending them to topic " + topicWithSchema + ". Will do this for " + durationSeconds
				+ " seconds!");
		final TopicSchemas schemas = handler.getSchemaForTopicByName(topicWithSchema);
		final String valueSchemaJson = new JSONObject(schemas.getValueSchema()).getString("schema");
		String keySchemaJson = null;
		if (schemas.getKeySchema() != null) {
			keySchemaJson = new JSONObject(schemas.getKeySchema()).getString("schema");
		}
		log.debug("For topic {} found schemas {}", topicWithSchema.getTopicName(), schemas);
		final Properties producerProps = new Properties();
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < kafkaBrokerAddresses.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(kafkaBrokerAddresses[i]);
		}
		producerProps.put("bootstrap.servers", sb.toString());
		producerProps.put("acks", "all");
		producerProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		producerProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		producerProps.put("schema.registry.url", handler.getSchemaRegistryAddress());
		try (KafkaProducer<Object, Object> producer = new KafkaProducer(producerProps)) {
			final AvroDataGenerator valuesGenerator = new AvroDataGenerator(valueSchemaJson);
			AvroDataGenerator keysGenerator = null;
			if (keySchemaJson != null) {
				keysGenerator = new AvroDataGenerator(keySchemaJson);
			}
			for (int i = 0; i < durationSeconds; i++) {
				final List<GenericRecord> values = valuesGenerator.generateRandomRecords(eventsPerSecond);
				List<GenericRecord> keys = null;
				if (keysGenerator != null) {
					keys = keysGenerator.generateRandomRecords(eventsPerSecond);
				}
				for (int j = 0; j < eventsPerSecond; j++) {
					ProducerRecord<Object, Object> rec = null;
					if (keys == null) {
						rec = new ProducerRecord<Object, Object>(topicWithSchema.getTopicName(), values.get(j));
					} else {
						rec = new ProducerRecord<Object, Object>(topicWithSchema.getTopicName(), keys.get(j), values.get(j));
					}
					producer.send(rec);
				}
				log.info("Finished sending {} events in batch {} to topic {}", eventsPerSecond, i, topicWithSchema.getTopicName());
				TimeUnit.SECONDS.sleep(1);
			}
		}
		log.info("Successfully sent {} events to topic {}", (eventsPerSecond * durationSeconds), topicWithSchema.getTopicName());
	}

}
