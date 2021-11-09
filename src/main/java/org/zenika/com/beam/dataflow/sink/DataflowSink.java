package org.zenika.com.beam.dataflow.sink;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.zenika.com.beam.dataflow.sink.coders.PipelineCoders;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.models.OutputOperation;
import org.zenika.com.beam.dataflow.sink.options.DataflowSinkOptions;
import org.zenika.com.beam.dataflow.sink.transforms.ConvertJsonNodeToStringMessage;
import org.zenika.com.beam.dataflow.sink.transforms.ConvertToInputOperation;
import org.zenika.com.beam.dataflow.sink.transforms.ConvertToOutputOperation;

public class DataflowSink {

	public static final TupleTag<InputOperation> CONVERSION_SUCCESS_TAG = new TupleTag<InputOperation>() {
		private static final long serialVersionUID = 1L;
	};
	public static final TupleTag<OutputOperation> OUTPUT_OPERATION_TAG = new TupleTag<OutputOperation>() {
		private static final long serialVersionUID = 1L;
	};
	public static final TupleTag<ErrorMessage> FAILURE_TAG = new TupleTag<ErrorMessage>() {
		private static final long serialVersionUID = 1L;
	};
	public static final TupleTag<KV<String, String>> STRING_MESSAGE_CONVERSION_SUCCESS_TAG = new TupleTag<KV<String, String>>() {
		private static final long serialVersionUID = 1L;
	};

	public static void main(String[] args) {
		final DataflowSinkOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowSinkOptions.class);
		run(options);
	}

	private static void run(DataflowSinkOptions options) {
		Pipeline pipeline = Pipeline.create(options);
		PipelineCoders.setCoders(pipeline);

		/*
		 * Step #1: Read messages in from Pub/Sub Either from a Subscription or Topic
		 */

		PCollection<PubsubMessage> messages = null;
		if (options.getUseSubscription()) {
			messages = pipeline.apply("ReadPubSubSubscription",
					PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
		} else {
			messages = pipeline.apply("ReadPubSubTopic",
					PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
		}

		PCollectionTuple convertionToInputPCTuple = messages.apply("Convert to inputOperation",
				ConvertToInputOperation.newBuilder()
						.withSuccessTag(CONVERSION_SUCCESS_TAG)
						.withFailureTag(FAILURE_TAG)
						.build());
		PCollectionTuple convertionToStringPCTuple = convertionToInputPCTuple.get(CONVERSION_SUCCESS_TAG)
				.apply("Convert to output", ConvertToOutputOperation.newBuilder().build())
				.apply("Convert JsonNode to String", ConvertJsonNodeToStringMessage.newBuilder()
						.withConversionSuccessStringTag(STRING_MESSAGE_CONVERSION_SUCCESS_TAG)
						.withFailureTag(FAILURE_TAG)
						.build());
		PCollection<KV<String, String>> stringMessagesKV = convertionToStringPCTuple
				.get(STRING_MESSAGE_CONVERSION_SUCCESS_TAG);
		stringMessagesKV
				.apply(Values.<String>create())
				.apply("Write to pubsub",
						PubsubIO.writeStrings().to(options.getOutputTopic()));

		// #####################################################################################################
		// Write to GCS (DLQ)
		// #####################################################################################################
		convertionToInputPCTuple.get(FAILURE_TAG)
				.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
				.apply("Write Failed Messages (GCS avro)", AvroIO.write(ErrorMessage.class)
						.withWindowedWrites()
						.withNumShards(1)
						.to(options.getOutputRejectionBucket()));

		pipeline.run();

	}

}
