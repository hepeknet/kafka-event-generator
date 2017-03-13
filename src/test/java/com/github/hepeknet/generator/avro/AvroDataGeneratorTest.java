package com.github.hepeknet.generator.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroDataGeneratorTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new AvroDataGenerator(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {
		new AvroDataGenerator(" ");
	}

	@Test
	public void testSimpleRecord() {
		final String schema = "{\"type\":\"record\",\"name\":\"simple\",\"fields\":[{\"name\":\"s1\",\"type\":\"string\"}]}";
		final AvroDataGenerator gen = new AvroDataGenerator(schema);
		final GenericRecord rec = (GenericRecord) gen.generateRandomRecord();
		assertNotNull(rec);
		assertEquals("simple", rec.getSchema().getName());
		assertNotNull(rec.get("s1"));
		assertTrue(rec.get("s1") instanceof String);
	}
	
	@Test
  public void testSimpleType() {
    final String schema = "{\"type\":\"string\", \"name\":\"f1\"}";
    final AvroDataGenerator gen = new AvroDataGenerator(schema);
    final String rec = (String) gen.generateRandomRecord();
    assertNotNull(rec);
  }

}
