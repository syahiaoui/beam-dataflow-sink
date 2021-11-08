package org.zenika.com.beam.dataflow.sink.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface DataflowSinkOptions extends PipelineOptions {
	@Description("Table spec to write the output to")
	ValueProvider<String> getOutputTableSpec();

	void setOutputTableSpec(ValueProvider<String> value);

	@Description("Pub/Sub topic to read the input from")
	ValueProvider<String> getInputTopic();

	void setInputTopic(ValueProvider<String> value);

	@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
			+ "projects/<project-id>/subscriptions/<subscription-name>.")
	ValueProvider<String> getInputSubscription();

	void setInputSubscription(ValueProvider<String> value);

	@Description("This determines whether the template reads from " + "a pub/sub subscription or a topic")
	@Default.Boolean(false)
	Boolean getUseSubscription();

	void setUseSubscription(Boolean value);

	@Description("The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
			+ "format. If it doesn't exist, it will be created during pipeline execution.")
	ValueProvider<String> getOutputDeadletterTable();

	void setOutputDeadletterTable(ValueProvider<String> value);
}
