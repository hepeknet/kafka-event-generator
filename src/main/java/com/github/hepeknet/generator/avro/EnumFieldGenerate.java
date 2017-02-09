package com.github.hepeknet.generator.avro;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class EnumFieldGenerate extends SchemaFiller {

	private final List<String> enumStrings;

	public EnumFieldGenerate(Field field) {
		super(field);
		enumStrings = field.schema().getEnumSymbols();
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateEnum());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateEnum();
	}

	public String generateEnum() {
		return enumStrings.get(randGenerator.getNextInt() % enumStrings.size());
	}

}
