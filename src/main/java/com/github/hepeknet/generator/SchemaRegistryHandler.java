package com.github.hepeknet.generator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegistryHandler {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final String schemaRegistryAddress;

	public SchemaRegistryHandler(String schemaRegistryAddress) {
		if (StringUtil.isEmpty(schemaRegistryAddress)) {
			throw new IllegalArgumentException("Schema registry address must not be null or empty");
		}
		this.schemaRegistryAddress = schemaRegistryAddress;
	}

	public Set<TopicWithSchema> getTopicsWithAssignedSchema() {
		log.debug("Schema registry address is [{}]", schemaRegistryAddress);
		final String response = HttpUtil.executeGetRequest(schemaRegistryAddress + "/subjects");
		log.debug("Got topics with schema {}", response);
		final JSONArray arr = new JSONArray(response);
		final Map<String, TopicWithSchema> topics = new HashMap<>();
		for (int i = 0; i < arr.length(); i++) {
			final String subjectName = arr.getString(i);
			log.debug("Parsing subject {}", subjectName);
			if (subjectName.endsWith("-value")) {
				final String topicName = subjectName.replace("-value", "");
				final TopicWithSchema existing = topics.get(topicName);
				if (existing == null) {
					final TopicWithSchema tws = new TopicWithSchema(topicName, false);
					topics.put(topicName, tws);
				}
			} else if (subjectName.endsWith("-key")) {
				final String topicName = subjectName.replace("-key", "");
				final TopicWithSchema existing = topics.get(topicName);
				if (existing == null) {
					final TopicWithSchema tws = new TopicWithSchema(topicName, false);
					topics.put(topicName, tws);
				} else {
					existing.setHasKeySchema(true);
				}
			} else {
				log.error("Do not know how to parse subject name {}. Skipping", subjectName);
			}
		}
		return new HashSet<>(topics.values());
	}

	public TopicSchemas getSchemaForTopicByName(TopicWithSchema tws) {
		if (tws == null) {
			throw new IllegalArgumentException("Unable to find schemas for null topic");
		}
		log.debug("Looking for schemas associated with topic {}", tws);
		String keySchemaUrl = null;
		final String valueSchemaUrl = String.format("%s/subjects/%s-value/versions/latest", schemaRegistryAddress, tws.getTopicName());
		log.debug("Value schema url is [{}]", valueSchemaUrl);
		if (tws.isHasKeySchema()) {
			keySchemaUrl = String.format("%s/subjects/%s-key/versions/latest", schemaRegistryAddress, tws.getTopicName());
			log.debug("Key schema url is [{}]", keySchemaUrl);
		}
		final String valueSchemaJson = HttpUtil.executeGetRequest(valueSchemaUrl);
		log.debug("Found value schema for {}", tws.getTopicName());
		String keySchemaJson = null;
		if (keySchemaUrl != null) {
			keySchemaJson = HttpUtil.executeGetRequest(keySchemaUrl);
			log.debug("Found key schema for {}", tws.getTopicName());
		}
		return new TopicSchemas(keySchemaJson, valueSchemaJson);
	}

	public String getSchemaRegistryAddress() {
		return schemaRegistryAddress;
	}

}
