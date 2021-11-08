package org.zenika.com.beam.dataflow.sink.models;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;

import com.fasterxml.jackson.databind.JsonNode;

@DefaultCoder(SerializableCoder.class)
@lombok.Data
public class InputOperation implements Serializable {

	private static final long serialVersionUID = 1L;
	private JsonNode payload;
	private Map<String, String> attributeMap;

	public InputOperation() {
		super();
	}

	public InputOperation(final PubsubMessage pubsubMessage) throws IOException {
		this.payload = JSONUtils.ToJsonNode(pubsubMessage.getPayload());
		this.attributeMap = new HashMap<>();
		this.attributeMap.putAll(pubsubMessage.getAttributeMap());
	}
}
