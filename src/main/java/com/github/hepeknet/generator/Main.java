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
		final String schemaRegAddress = args[0];
		final String[] kafkaBrokerAddresses = args[1].split(",");
		String topicName = "";
		if (args.length > 2) {
			topicName = args[2];
		}
		int enteredEventsPerSecond = -1;
		if (args.length > 3) {
			enteredEventsPerSecond = Integer.parseInt(args[3]);
		}
		int enteredDuration = -1;
		if (args.length > 4) {
			enteredDuration = Integer.parseInt(args[4]);
		}
		final SchemaRegistryHandler regHandler = new SchemaRegistryHandler(schemaRegAddress);
		Set<TopicWithSchema> topicsWithSchema = null;
		try {
			topicsWithSchema = regHandler.getTopicsWithAssignedSchema();
		} catch (final Exception exc) {
			printMessage("Was not able to find topics with schema using schema registry [" + schemaRegAddress + "]");
			exc.printStackTrace();
			return;
		}
		if (topicsWithSchema.isEmpty()) {
			System.err.println("Did not find any topics with assigned schemas at [" + schemaRegAddress + "]!");
			return;
		}
		final Set<String> topicNames = topicsWithSchema.stream().map(tws -> tws.getTopicName()).collect(Collectors.toSet());
		if (!StringUtil.isEmpty(topicName) && !topicNames.contains(topicName)) {
			printMessage("The topic you entered " + topicName + " does not have associated schema. Switching to exploratory mode!");
		}
		while (!topicNames.contains(topicName)) {
			printMessage("Topics with assigned schemas are [" + Arrays.toString(topicNames.toArray()) + "].\nPlease enter one of these values:");
			topicName = System.console().readLine();
			if (StringUtil.isEmpty(topicName)) {
				System.out.println("Exiting...");
				return;
			}
		}
		final String chosenTopicName = topicName;
		printMessage("Will send messages to topic [" + topicName + "]");
		final TopicWithSchema chosenTopicWithSchema = topicsWithSchema.stream().filter(tws -> tws.getTopicName().equals(chosenTopicName)).findFirst().get();
		final KafkaEventGenerator keg = new KafkaEventGenerator(schemaRegAddress, kafkaBrokerAddresses, chosenTopicWithSchema);
		if (enteredEventsPerSecond > 0) {
			keg.setEventsPerSecond(enteredEventsPerSecond);
		}
		if (enteredDuration > 0) {
			keg.setDurationSeconds(enteredDuration);
		}
		keg.generateAndSend();
	}

	private static void printMessage(String msg) {
		System.out.println();
		System.out.println("==========================");
		System.out.println(msg);
		System.out.println();
	}

}
