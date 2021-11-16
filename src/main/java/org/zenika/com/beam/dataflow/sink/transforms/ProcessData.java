package org.zenika.com.beam.dataflow.sink.transforms;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.zenika.com.beam.dataflow.sink.DataflowSink;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;

import com.google.auto.value.AutoValue;


@AutoValue
public abstract class ProcessData extends PTransform<PCollection<InputOperation>, PCollectionTuple> {

	private static final long serialVersionUID = 1L;

	public static Builder newBuilder() {
		return new AutoValue_ProcessData.Builder();
	}
	
	@Override
	public PCollectionTuple expand(PCollection<InputOperation> input) {
		return input
				.apply("Convert to output", ConvertToBigQueryMessage.newBuilder().build())
				.apply("Convert JsonNode to String", ConvertBigQueryMessageToStringMessage.newBuilder()
						.withConversionSuccessStringTag(DataflowSink.STRING_MESSAGE_CONVERSION_SUCCESS_TAG)
						.withFailureTag(DataflowSink.FAILURE_TAG)
						.build());
	}
	@AutoValue.Builder
	public abstract static class Builder {
		public abstract ProcessData build();
	}
}
