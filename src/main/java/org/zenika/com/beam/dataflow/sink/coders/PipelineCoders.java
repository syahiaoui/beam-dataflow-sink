package org.zenika.com.beam.dataflow.sink.coders;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;

import com.fasterxml.jackson.databind.JsonNode;


public class PipelineCoders {

	public static void setCoders(final Pipeline pipeline) {
		final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
		coderRegistry.registerCoderForClass(JsonNode.class, JsonNodeCoder.of());
		coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());
		coderRegistry.registerCoderForClass(PubsubMessage.class, PubsubMessageWithAttributesCoder.of());
	}
}
