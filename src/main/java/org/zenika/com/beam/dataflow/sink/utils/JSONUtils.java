package org.zenika.com.beam.dataflow.sink.utils;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JSONUtils {
	private static ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);

	public static JsonNode ToJsonNode(final byte[] jsonBuffer) throws IOException {
		return mapper.get().reader().readTree(jsonBuffer);
	}
	public static String ToJsonString(final Object pojo) throws JsonProcessingException {
		mapper.get().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		return mapper.get().writeValueAsString(pojo);
	}
}
