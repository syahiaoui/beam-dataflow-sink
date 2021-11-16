package org.zenika.com.beam.dataflow.sink.coders;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.values.KV;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.values.FailsafeElement;

import com.fasterxml.jackson.databind.JsonNode;

public class PipelineCoders {
	public static final FailsafeElementCoder<KV<String, String>, String> FAILSAFE_ELEMENT_CODER = FailsafeElementCoder
			.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), StringUtf8Coder.of());

	public static void setCoders(final Pipeline pipeline) {
		final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
		coderRegistry.registerCoderForClass(JsonNode.class, JsonNodeCoder.of());
		coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());
		coderRegistry.registerCoderForClass(PubsubMessage.class, PubsubMessageWithAttributesCoder.of());
		coderRegistry.registerCoderForClass(FailsafeElement.class, FAILSAFE_ELEMENT_CODER);
	}
}
