package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class RecordFieldGenerate extends SchemaFiller {

	public RecordFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		record.put(field.name(), generateRecord());
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return generateRecord();
	}

	public GenericRecord generateRecord() throws UnknownTypeException {
		final GenericRecord subRecord = new GenericData.Record(field.schema());
		for (final Field field : this.field.schema().getFields()) {
			final SchemaFiller fill = SchemaFiller.createRandomField(field);
			fill.writeToRecord(subRecord);
		}
		return subRecord;
	}

}
