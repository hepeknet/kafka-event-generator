package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class IntegerFieldGenerate extends SchemaFiller {

	private static int minIntLength = -10;
	private static int maxIntLength = Integer.MAX_VALUE;

	public static int getMinIntLength() {
		return minIntLength;
	}

	public static void setMinIntLength(int minIntLength) {
		IntegerFieldGenerate.minIntLength = minIntLength;
	}

	public static int getMaxIntLength() {
		return maxIntLength;
	}

	public static void setMaxIntLength(int maxIntLength) {
		IntegerFieldGenerate.maxIntLength = maxIntLength;
	}

	public IntegerFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord genericRecord) {
		genericRecord.put(this.field.name(), generateInteger());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateInteger();
	}

	public Integer generateInteger() {
		return randGenerator.getNextInt(minIntLength, maxIntLength);
	}

}
