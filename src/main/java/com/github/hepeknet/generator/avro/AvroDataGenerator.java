package com.github.hepeknet.generator.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class AvroDataGenerator {

	private final Schema schema;

	public AvroDataGenerator(String schema) {
		if (schema == null || schema.trim().isEmpty()) {
			throw new IllegalArgumentException("Schema string must not be null or empty!");
		}
		this.schema = Schema.parse(schema);
		if (this.schema.getType() != Schema.Type.RECORD) {
			throw new IllegalStateException("The schema first level must be record");
		}
	}

	public List<GenericRecord> generateRandomRecords(int recNumber) {
		if (recNumber <= 0) {
			throw new IllegalArgumentException("Number of records must be > 0");
		}
		final List<GenericRecord> recs = new ArrayList<>();
		for (int i = 0; i < recNumber; i++) {
			final GenericRecord gr = generateRandomRecord();
			recs.add(gr);
		}
		return recs;
	}

	public GenericRecord generateRandomRecord() throws UnknownTypeException {
		final GenericRecord record = new GenericData.Record(schema);
		for (final Field field : schema.getFields()) {
			final SchemaFiller schemaFill = SchemaFiller.createRandomField(field);
			schemaFill.writeToRecord(record);
		}
		return record;
	}
}
