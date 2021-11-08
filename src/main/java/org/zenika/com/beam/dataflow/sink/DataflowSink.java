package org.zenika.com.beam.dataflow.sink;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.zenika.com.beam.dataflow.sink.coders.PipelineCoders;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.options.DataflowSinkOptions;

public class DataflowSink {

	public static final TupleTag<InputOperation> CONVERSION_SUCCESS_TAG = new TupleTag<InputOperation>();
	public static final TupleTag<ErrorMessage> FAILURE_TAG = new TupleTag<ErrorMessage>();

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

//		PCollectionTuple convertedTableRows = messages
//				/*
//				 * Step #2: Transform the PubsubMessages into TableRows
//				 */
//				.apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

	}

}
