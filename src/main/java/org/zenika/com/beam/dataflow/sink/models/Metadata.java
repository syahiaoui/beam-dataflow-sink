package org.zenika.com.beam.dataflow.sink.models;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.fasterxml.jackson.annotation.JsonInclude;

@DefaultCoder(SerializableCoder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@lombok.Data
public class Metadata {
	private String processDate;
	private String publishDate;
	private String ingestPubsub;
	private String key;
	private String processId;
	private String status;
}
