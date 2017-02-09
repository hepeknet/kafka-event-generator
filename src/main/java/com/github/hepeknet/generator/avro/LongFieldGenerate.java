package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class LongFieldGenerate extends SchemaFiller {

	public LongFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) {
		record.put(field.name(), generateLong());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateLong();
	}

	public Long generateLong() {
		return randGenerator.getNextLong();
	}
}
