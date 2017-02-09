package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class BooleanFieldGenerate extends SchemaFiller {

	public BooleanFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateBoolean());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateBoolean();
	}

	public boolean generateBoolean() {
		return randGenerator.getNextBoolean();
	}
}
