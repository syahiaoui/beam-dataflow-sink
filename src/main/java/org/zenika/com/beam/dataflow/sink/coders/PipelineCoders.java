package org.zenika.com.beam.dataflow.sink.coders;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;

public class PipelineCoders {

	/** Pubsub message/string coder for pipeline. */
	public static final FailsafeElementCoder<PubsubMessage, String> CODER = FailsafeElementCoder
			.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

	/** String/String Coder for FailsafeElement. */
	public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER = FailsafeElementCoder
			.of(StringUtf8Coder.of(), StringUtf8Coder.of());

	public static void setCoders(final Pipeline pipeline) {
		final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
		coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
		coderRegistry.registerCoderForType(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
	}
}
