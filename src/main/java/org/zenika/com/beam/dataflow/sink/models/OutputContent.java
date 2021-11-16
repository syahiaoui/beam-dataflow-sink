package org.zenika.com.beam.dataflow.sink.models;

import java.io.Serializable;
import java.util.Objects;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;

@lombok.Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@DefaultCoder(SerializableCoder.class)
public class OutputContent implements Serializable {
	private static final long serialVersionUID = 1L;
	private Metadata metadata;
	private Object data;
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OutputContent other = (OutputContent) obj;
		return Objects.equals(data, other.data) && Objects.equals(metadata, other.metadata);
	}
	@Override
	public int hashCode() {
		return Objects.hash(data, metadata);
	}
}
