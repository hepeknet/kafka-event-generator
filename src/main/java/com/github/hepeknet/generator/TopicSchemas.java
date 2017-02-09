package com.github.hepeknet.generator;

public class TopicSchemas {

	private final String keySchema;
	private final String valueSchema;

	public TopicSchemas(String keySchema, String valueSchema) {
		super();
		this.keySchema = keySchema;
		this.valueSchema = valueSchema;
	}

	public String getKeySchema() {
		return keySchema;
	}

	public String getValueSchema() {
		return valueSchema;
	}

	@Override
	public String toString() {
		return "TopicSchemas [keySchema=" + keySchema + ", valueSchema=" + valueSchema + "]";
	}

}
