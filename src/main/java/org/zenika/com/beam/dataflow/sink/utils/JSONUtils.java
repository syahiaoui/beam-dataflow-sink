package org.zenika.com.beam.dataflow.sink.utils;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;

public class JSONUtils {
	private static ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);

	public static JsonNode ToJsonNode(final byte[] jsonBuffer) throws IOException {
		mapper.get().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
		mapper.get().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
		return mapper.get().reader().readTree(jsonBuffer);
	}

	public static JsonNode ConvertValue(final Object data) {
		return mapper.get().convertValue(data, JsonNode.class);
	}

	public static String ToJsonString(final Object pojo) throws JsonProcessingException {
		mapper.get().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		return mapper.get().writeValueAsString(pojo);
	}

	public static TableRow ToTableRow(final InputStream inputStream) throws IOException {
		mapper.get().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		return mapper.get().readValue(inputStream, TableRow.class);
	}
}
