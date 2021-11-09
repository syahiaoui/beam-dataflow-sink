package org.zenika.com.beam.dataflow.sink.models;

import java.io.Serializable;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.fasterxml.jackson.annotation.JsonInclude;

@lombok.Data
@DefaultCoder(SerializableCoder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Metadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String key;
	private String processDate;
	private String ingestPubsub;
	private String processId;
	private String status;
}
