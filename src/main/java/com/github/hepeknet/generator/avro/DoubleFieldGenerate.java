package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class DoubleFieldGenerate extends SchemaFiller {

	private static double minValue = 0;
	private static double maxValue = Double.MAX_VALUE;

	public static double getMinValue() {
		return minValue;
	}

	public static void setMinValue(double minValue) {
		DoubleFieldGenerate.minValue = minValue;
	}

	public static double getMaxValue() {
		return maxValue;
	}

	public static void setMaxValue(double maxValue) {
		DoubleFieldGenerate.maxValue = maxValue;
	}

	public DoubleFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) {
		record.put(field.name(), generateDouble());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateDouble();
	}

	public Double generateDouble() {
		return randGenerator.getNextDouble();
	}
}
