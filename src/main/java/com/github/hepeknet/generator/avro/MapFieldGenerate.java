package com.github.hepeknet.generator.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class MapFieldGenerate extends SchemaFiller {

	private static int maxNumberOfMapFields = 10;

	public static int getMaxNumberOfMapFields() {
		return maxNumberOfMapFields;
	}

	public static void setMaxNumberOfMapFields(int maxNumberOfMapFields) {
		MapFieldGenerate.maxNumberOfMapFields = maxNumberOfMapFields;
	}

	public MapFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateMap());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateMap();
	}

	private Map<Utf8, Object> generateMap() throws UnknownTypeException {
		final Map<Utf8, Object> map = new HashMap<Utf8, Object>(randGenerator.getNextInt() % maxNumberOfMapFields);
		final Field fakeField = new Field(field.name(), field.schema().getValueType(), null, null);
		final int count = randGenerator.getNextInt() % getMaxNumberOfMapFields();
		for (int i = 0; i < count; i++) {
			final SchemaFiller filler = SchemaFiller.createRandomField(fakeField);
			map.put(new Utf8(field.name() + i), filler.generateRandomObject());
		}
		return map;
	}

}
