package com.github.hepeknet.generator;

public class TopicWithSchema {

	private final String topicName;
	private boolean hasKeySchema;

	public TopicWithSchema(String topicName, boolean hasKeySchema) {
		if (StringUtil.isEmpty(topicName)) {
			throw new IllegalArgumentException("Topic name must not be null or empty string");
		}
		this.topicName = topicName;
		this.hasKeySchema = hasKeySchema;
	}

	public String getTopicName() {
		return topicName;
	}

	public boolean isHasKeySchema() {
		return hasKeySchema;
	}

	public void setHasKeySchema(boolean hasKeySchema) {
		this.hasKeySchema = hasKeySchema;
	}

	@Override
	public String toString() {
		return "TopicWithSchema [topicName=" + topicName + ", hasKeySchema=" + hasKeySchema + "]";
	}

}
