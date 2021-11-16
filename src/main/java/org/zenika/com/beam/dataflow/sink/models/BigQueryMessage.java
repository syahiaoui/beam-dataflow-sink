package org.zenika.com.beam.dataflow.sink.models;

import java.io.Serializable;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

@DefaultCoder(SerializableCoder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@lombok.Data
public class BigQueryMessage implements Serializable {
	private static final long serialVersionUID = 1L;
	private String topic;
	private String key;
	private String event;
	private String publishTime;
	private long timestamp;
	private JsonNode content;
}
