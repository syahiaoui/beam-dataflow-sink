package org.zenika.com.beam.dataflow.sink.transforms;

import static com.google.common.truth.Truth.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.zenika.com.beam.dataflow.sink.DataflowSink;
import org.zenika.com.beam.dataflow.sink.coders.ErrorMessageCoder;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ConvertToInputOperation.class)
public class ConvertToInputOperationTest {
	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Before
	public void setUp() {
		CoderRegistry coderRegistry = pipeline.getCoderRegistry();
		coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());
		coderRegistry.registerCoderForClass(PubsubMessage.class, PubsubMessageWithAttributesCoder.of());
	}

	@Test
	@Category(NeedsRunner.class)
	public void shouldConvertValidInput() {
		final String payload = "{\"data\":{\"personalNumber\":123456,\"firstName\":\"zenika\",\"lastName\":\"labs\",\"email\":\"xyz@zenika.com\",\"creationDate\":\"2021-11-04T17:53:04.175Z\"},\"processId\":\"f0ae8dc2-c6aa-483a-ae85-cbd46a24fcc5\",\"operationType\":\"MUTATION\",\"key\":\"123456_LABS\",\"processDate\":\"2021-11-16T10:13:42.269Z\"}";
		final String expectedInputOperation = "{\"payload\":{\"data\":{\"personalNumber\":123456,\"firstName\":\"zenika\",\"lastName\":\"labs\",\"email\":\"xyz@zenika.com\",\"creationDate\":\"2021-11-04T17:53:04.175Z\"},\"processId\":\"f0ae8dc2-c6aa-483a-ae85-cbd46a24fcc5\",\"operationType\":\"MUTATION\",\"key\":\"123456_LABS\",\"processDate\":\"2021-11-16T10:13:42.269Z\"},\"attributeMap\":{\"processId\":\"6aa5f55f-241d-4307-bec6-2b5f8192fd89\",\"processDate\":\"2021-11-16T09:34:23.345Z\",\"type\":\"MUTATION\",\"ingestPubsub\":\"-290308-12-21T19:59:05.225Z\",\"key\":\"A0219961015000011766_440_DRI_FR_PER\"}}";
		final Map<String, String> attributesMap = new HashMap<String, String>();
		attributesMap.put("processId", "6aa5f55f-241d-4307-bec6-2b5f8192fd89");
		attributesMap.put("key", "A0219961015000011766_440_DRI_FR_PER");
		attributesMap.put("type", "MUTATION");
		attributesMap.put("processDate", "2021-11-16T09:34:23.345Z");
		final Map<String, String> attributes = ImmutableMap.copyOf(attributesMap);
		final PubsubMessage message = new PubsubMessage(payload.getBytes(), attributes);

		PCollectionTuple conversionPCollectionTuple = pipeline
				.apply("CreateInput", Create.of(message))
				.apply("ConvertToInputOperation",
						ConvertToInputOperation.newBuilder()
								.withSuccessTag(DataflowSink.CONVERSION_SUCCESS_TAG)
								.withFailureTag(DataflowSink.FAILURE_TAG)
								.build());

		PAssert.that(conversionPCollectionTuple.get(DataflowSink.FAILURE_TAG)).empty();
		PAssert.that(conversionPCollectionTuple.get(DataflowSink.CONVERSION_SUCCESS_TAG))
				.satisfies(
						collection ->
							{
								final InputOperation result = collection.iterator().next();
								try {
									assertThat(JSONUtils.ToJsonString(result)).isEqualTo(expectedInputOperation);
								} catch (JsonProcessingException e) {
									e.printStackTrace();
								}
								return null;
							});
		pipeline.run();
	}

	@Test
	@Category(NeedsRunner.class)
	public void shouldConvertInvalidInputToErrorMessage() {
		final String invalidPayload = "\"data\":{\"personalNumber\":123456,\"firstName\":\"zenika\",\"lastName\":\"labs\",\"email\":\"xyz@zenika.com\",\"creationDate\":\"2021-11-04T17:53:04.175Z\"},\"processId\":\"f0ae8dc2-c6aa-483a-ae85-cbd46a24fcc5\",\"operationType\":\"MUTATION\",\"key\":\"123456_LABS\",\"processDate\":\"2021-11-16T10:13:42.269Z\"}";
		final String expectedErrorMessage = "[ConvertToInputOperation] Invalid Json message";
		final String expectedStackTrace = "Unexpected character (':' (code 58)): expected a valid value (JSON String, Number, Array, Object or token 'null', 'true' or 'false')";
		final Map<String, String> attributesMap = new HashMap<String, String>();
		attributesMap.put("processId", "6aa5f55f-241d-4307-bec6-2b5f8192fd89");
		attributesMap.put("key", "A0219961015000011766_440_DRI_FR_PER");
		attributesMap.put("type", "MUTATION");
		attributesMap.put("processDate", "2021-11-16T09:34:23.345Z");
		final Map<String, String> attributes = ImmutableMap.copyOf(attributesMap);
		final PubsubMessage message = new PubsubMessage(invalidPayload.getBytes(), attributes);

		PCollectionTuple conversionPCollectionTuple = pipeline
				.apply("CreateInput", Create.of(message))
				.apply("ConvertToInputOperation",
						ConvertToInputOperation.newBuilder()
								.withSuccessTag(DataflowSink.CONVERSION_SUCCESS_TAG)
								.withFailureTag(DataflowSink.FAILURE_TAG)
								.build());

		PAssert.that(conversionPCollectionTuple.get(DataflowSink.CONVERSION_SUCCESS_TAG)).empty();
		PAssert.that(conversionPCollectionTuple.get(DataflowSink.FAILURE_TAG))
				.satisfies(
						collection ->
							{
								final ErrorMessage errorMessage = collection.iterator().next();
								assertThat(errorMessage.errorMessage()).isEqualTo(expectedErrorMessage);
								assertThat(errorMessage.errorStackTrace()).contains(expectedStackTrace);
								assertThat(errorMessage.jsonPayload()).isEqualTo(invalidPayload);
								return null;
							});
		pipeline.run();
	}

}
