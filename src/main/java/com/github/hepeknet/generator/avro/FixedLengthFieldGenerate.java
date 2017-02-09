package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class FixedLengthFieldGenerate extends SchemaFiller {

	private final int fixedSize;

	public FixedLengthFieldGenerate(Field field) {
		super(field);
		fixedSize = field.schema().getFixedSize();
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateFixedLengthString());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateFixedLengthString();
	}

	public String generateFixedLengthString() {
		return randGenerator.getNextString(0, fixedSize);
	}
}
