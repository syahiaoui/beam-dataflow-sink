package org.zenika.com.beam.dataflow.sink.transforms.bigquery;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.zenika.com.beam.dataflow.sink.DataflowSink;
import org.zenika.com.beam.dataflow.sink.coders.PipelineCoders;
import org.zenika.com.beam.dataflow.sink.values.FailsafeElement;

public class ConvertToTableRow extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

	private static final long serialVersionUID = 1L;

	public PCollectionTuple expand(PCollection<KV<String, String>> input) {
		// Map the incoming messages into FailsafeElements so we can recover from
		// failures
		// across multiple transforms.
		return input
				.apply("MapToRecord", ParDo.of(new BigQueryMessageToFailsafeElementFn()))
				.setCoder(PipelineCoders.FAILSAFE_ELEMENT_CODER)
				.apply("String BigQueryMessage To TableRow", BigQueryConverters.FailsafeBigQueryMessageToTableRow
						.newBuilder()
						.setSuccessTag(DataflowSink.TRANSFORM_OUT)
						.setFailureTag(DataflowSink.TRANSFORM_DEADLETTER_OUT)
						.build());
	}

	static class BigQueryMessageToFailsafeElementFn
			extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext context) {
			String message = context.element().getValue();
			context.output(FailsafeElement.of(KV.of(context.element().getKey(), message), message));
		}
	}
}
