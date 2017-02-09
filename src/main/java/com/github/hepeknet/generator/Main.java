package com.github.hepeknet.generator;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {

	private static final int MIN_REQUIRED_ARGS = 2;

	public static void main(String[] args) throws Exception {
		if (args == null || args.length < MIN_REQUIRED_ARGS) {
			throw new IllegalArgumentException("Not all required arguments were passed! Aborting...");
		}
		final String sr = args[0];
		final String[] kb = args[1].split(",");
		String topicName = "";
		if (args.length > 2) {
			topicName = args[2];
		}
		final SchemaRegistryHandler regHandler = new SchemaRegistryHandler(sr);
		final Set<TopicWithSchema> topicsWithSchema = regHandler.getTopicsWithAssignedSchema();
		if (topicsWithSchema.isEmpty()) {
			System.err.println("Did not find any topics with assigned schemas at [" + sr + "]!");
			return;
		}
		final Set<String> topicNames = topicsWithSchema.stream().map(tws -> tws.getTopicName()).collect(Collectors.toSet());
		while (!topicNames.contains(topicName)) {
			System.out.println("Topics with assigned schemas are [" + Arrays.toString(topicNames.toArray()) + "]. Please enter one of these values:");
			topicName = System.console().readLine();
			if (StringUtil.isEmpty(topicName)) {
				System.out.println("Exiting...");
				return;
			}
		}
		final String chosenTopicName = topicName;
		final TopicWithSchema chosenTopicWithSchema = topicsWithSchema.stream().filter(tws -> tws.getTopicName().equals(chosenTopicName)).findFirst().get();
		final KafkaEventGenerator keg = new KafkaEventGenerator(sr, kb, chosenTopicWithSchema);
		keg.generateAndSend();
	}

}
