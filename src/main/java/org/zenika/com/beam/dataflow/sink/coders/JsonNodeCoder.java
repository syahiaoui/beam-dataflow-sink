package org.zenika.com.beam.dataflow.sink.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonNodeCoder extends CustomCoder<JsonNode> {

	private static final long serialVersionUID = 1L;
	private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final JsonNodeCoder JSON_NODE_CODER = new JsonNodeCoder();

	private JsonNodeCoder() {
	}

	public static JsonNodeCoder of() {
		return JSON_NODE_CODER;
	}

	@Override
	public void encode(JsonNode value, OutputStream outStream) throws IOException {
		if (value == null) {
			throw new CoderException("The JsonNodeCoder cannot encode a null object!");
		}
		STRING_UTF_8_CODER.encode(MAPPER.writer().writeValueAsString(value), outStream);
	}

	@Override
	public JsonNode decode(InputStream inStream) throws IOException {
		String jsonString = STRING_UTF_8_CODER.decode(inStream);
		return MAPPER.reader().readTree(jsonString);
	}
}
