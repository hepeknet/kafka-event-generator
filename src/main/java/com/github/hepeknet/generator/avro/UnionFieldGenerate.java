package com.github.hepeknet.generator.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class UnionFieldGenerate extends SchemaFiller {

	public UnionFieldGenerate(Field field) {
		super(field);
	}

	@Override
	public void writeToRecord(GenericRecord record) throws UnknownTypeException {
		getUnionFieldFiller().writeToRecord(record);
	}

	@Override
	public Object generateRandomObject() throws UnknownTypeException {
		return getUnionFieldFiller().generateRandomObject();
	}

	public SchemaFiller getUnionFieldFiller() throws UnknownTypeException {
		final List<Schema> schemas = field.schema().getTypes();
		Schema schema = null;
		for (final Schema s : schemas) {
			schema = s;
			if (schema.getType() != Schema.Type.NULL)
				break;
		}
		final Field tempField = new Field(field.name(), schema, null, null);
		return SchemaFiller.createRandomField(tempField);
	}

}
