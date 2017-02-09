package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class ArrayFieldGenerate extends SchemaFiller {

	private static int maxArrayLength = 10;

	public ArrayFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateArray());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateArray();
	}

	public GenericData.Array<Object> generateArray() throws UnknownTypeException {
		final Schema innerElementSchema = field.schema().getElementType();
		final GenericData.Array<Object> array = new GenericData.Array<Object>(10, field.schema());

		for (int i = 0; i < randGenerator.getNextInt() % maxArrayLength; i++) {
			final Field fakeField = new Field(field.name() + "fake", innerElementSchema, null, null);
			final SchemaFiller schemaFill = SchemaFiller.createRandomField(fakeField);
			array.add(schemaFill.generateRandomObject());
		}
		return array;
	}
}
