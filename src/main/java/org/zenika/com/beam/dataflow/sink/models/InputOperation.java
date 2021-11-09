package org.zenika.com.beam.dataflow.sink.models;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;

@DefaultCoder(SerializableCoder.class)
public class InputOperation implements Serializable {
	public InputOperation() {
		super();
	}

	private static final long serialVersionUID = 1L;
	private JsonNode payload;
	private Map<String, String> attributeMap;

	public InputOperation(PubsubMessage pubsubMessage) throws IOException {
		this.payload = JSONUtils.ToJsonNode(pubsubMessage.getPayload());
		this.attributeMap = new HashMap<>();
		this.attributeMap.putAll(pubsubMessage.getAttributeMap());
	}

	public InputOperation(JsonNode payload, Map<String, String> attributeMap) {
		this.payload = payload;
		this.attributeMap = attributeMap;
	}

	public JsonNode getPayload() {
		return payload;
	}

	public void setPayload(JsonNode payload) {
		this.payload = payload;
	}

	public Map<String, String> getAttributeMap() {
		return attributeMap;
	}

	public void setAttributeMap(Map<String, String> attributeMap) {
		this.attributeMap = attributeMap;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		InputOperation that = (InputOperation) o;
		return payload.equals(that.payload) && Objects.equals(attributeMap, that.attributeMap);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(attributeMap);
		result = 31 * result + payload.toString().hashCode();
		return result;
	}
}
