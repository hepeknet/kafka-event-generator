package com.github.hepeknet.generator.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public abstract class SchemaFiller {

	protected Field field;
	protected RandomDataGenerator randGenerator;

	public SchemaFiller(Field field) {
		this.field = field;
		this.randGenerator = new DefaultRandomGenerator();
	}

	public SchemaFiller(Field field, RandomDataGenerator randGenerator) {
		this.field = field;
		this.randGenerator = randGenerator;
	}
	
	 public static SchemaFiller createRandomObjectByType(Schema.Type type) throws UnknownTypeException {
	   Field field = null;
	    if (type == Schema.Type.ARRAY) {
	      return new ArrayFieldGenerate(field);
	    } else if (type == Schema.Type.BOOLEAN) {
	      return new BooleanFieldGenerate(field);
	    } else if (type == Schema.Type.BYTES) {
	      return new BytesFieldGenerate(field);
	    } else if (type == Schema.Type.DOUBLE) {
	      return new DoubleFieldGenerate(field);
	    } else if (type == Schema.Type.ENUM) {
	      return new EnumFieldGenerate(field);
	    } else if (type == Schema.Type.FIXED) {
	      return new FixedLengthFieldGenerate(field);
	    } else if (type == Schema.Type.FLOAT) {
	      return new FloatFieldGenerate(field);
	    } else if (type == Schema.Type.INT) {
	      return new IntegerFieldGenerate(field);
	    } else if (type == Schema.Type.LONG) {
	      return new LongFieldGenerate(field);
	    } else if (type == Schema.Type.MAP) {
	      return new MapFieldGenerate(field);
	    } else if (type == Schema.Type.NULL) {
	      return new NullFieldGenerate(field);
	    } else if (type == Schema.Type.RECORD) {
	      return new RecordFieldGenerate(field);
	    } else if (type == Schema.Type.STRING) {
	      return new StringFieldGenerate(field);
	    } else if (type == Schema.Type.UNION) {
	      return new UnionFieldGenerate(field);
	    } else {
	      throw new UnknownTypeException();
	    }
	  }

	public static SchemaFiller createRandomField(Field field) throws UnknownTypeException {
		final Schema.Type type = field.schema().getType();
		if (type == Schema.Type.ARRAY) {
			return new ArrayFieldGenerate(field);
		} else if (type == Schema.Type.BOOLEAN) {
			return new BooleanFieldGenerate(field);
		} else if (type == Schema.Type.BYTES) {
			return new BytesFieldGenerate(field);
		} else if (type == Schema.Type.DOUBLE) {
			return new DoubleFieldGenerate(field);
		} else if (type == Schema.Type.ENUM) {
			return new EnumFieldGenerate(field);
		} else if (type == Schema.Type.FIXED) {
			return new FixedLengthFieldGenerate(field);
		} else if (type == Schema.Type.FLOAT) {
			return new FloatFieldGenerate(field);
		} else if (type == Schema.Type.INT) {
			return new IntegerFieldGenerate(field);
		} else if (type == Schema.Type.LONG) {
			return new LongFieldGenerate(field);
		} else if (type == Schema.Type.MAP) {
			return new MapFieldGenerate(field);
		} else if (type == Schema.Type.NULL) {
			return new NullFieldGenerate(field);
		} else if (type == Schema.Type.RECORD) {
			return new RecordFieldGenerate(field);
		} else if (type == Schema.Type.STRING) {
			return new StringFieldGenerate(field);
		} else if (type == Schema.Type.UNION) {
			return new UnionFieldGenerate(field);
		} else {
			throw new UnknownTypeException();
		}
	}

	public abstract void writeToRecord(GenericRecord record) throws UnknownTypeException;

	public abstract Object generateRandomObject() throws UnknownTypeException;

}
