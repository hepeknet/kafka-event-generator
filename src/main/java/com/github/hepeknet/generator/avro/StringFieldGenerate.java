package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class StringFieldGenerate extends SchemaFiller {

	private static int minStringLength = 0;
	private static int maxStringLength = 1024;

	public static int getMaxStringLength() {
		return maxStringLength;
	}

	public static int getMinStringLength() {
		return minStringLength;
	}

	public StringFieldGenerate(Field field) {
		super(field);
		// TODO read min and max length from config and set and use with
		// generator
	}

	@Override
	public void writeToRecord(GenericRecord genericRecord) {
		genericRecord.put(field.name(), generateString());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateString();
	}

	public String generateString() {
		return randGenerator.getNextString(minStringLength, maxStringLength);
	}

}
