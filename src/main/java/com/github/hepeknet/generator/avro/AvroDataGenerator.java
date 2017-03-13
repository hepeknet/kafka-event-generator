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
  }

  public List<Object> generateRandomRecords(int recNumber) {
    if (recNumber <= 0) {
      throw new IllegalArgumentException("Number of records must be > 0");
    }
    final List<Object> recs = new ArrayList<>();
    for (int i = 0; i < recNumber; i++) {
      final Object gr = generateRandomRecord();
      recs.add(gr);
    }
    return recs;
  }

  public Object generateRandomRecord() throws UnknownTypeException {
    Schema.Type type = schema.getType();
    if (type == Schema.Type.RECORD) {
      final GenericRecord record = new GenericData.Record(schema);
      for (final Field field : schema.getFields()) {
        final SchemaFiller schemaFill = SchemaFiller.createRandomField(field);
        schemaFill.writeToRecord(record);
      }
      return record;
    } else {
      final SchemaFiller schemaFill = SchemaFiller.createRandomObjectByType(type);
      return schemaFill.generateRandomObject();
    }
  }
}
