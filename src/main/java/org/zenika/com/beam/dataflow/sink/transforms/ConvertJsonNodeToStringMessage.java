package org.zenika.com.beam.dataflow.sink.transforms;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.models.OutputOperation;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.value.AutoValue;
import com.google.common.base.Throwables;

@AutoValue
abstract public class ConvertJsonNodeToStringMessage
		extends PTransform<PCollection<OutputOperation>, PCollectionTuple> {

	private static final long serialVersionUID = 1L;

	public static Builder newBuilder() {
		return new AutoValue_ConvertJsonNodeToStringMessage.Builder();
	}

	abstract TupleTag<KV<String, String>> conversionSuccessStringTag();

	abstract TupleTag<ErrorMessage> failureTag();

	@Override
	public PCollectionTuple expand(PCollection<OutputOperation> input) {
		return input.apply("ConvertJsonNodeToStringMessage", ParDo.of(new DoFn<OutputOperation, KV<String, String>>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext context) {
				try {
					final OutputOperation outputOperation = context.element();
					Metrics.counter(ConvertJsonNodeToStringMessage.class, "SUCCESS_CONVERSION_TO_STRING").inc();
					context.output(conversionSuccessStringTag(),
							KV.of(outputOperation.getMetadata().getKey(), JSONUtils.ToJsonString(outputOperation)));
				} catch (JsonProcessingException e) {
					final String message = "[ConvertJsonNodeToStings] Unable to convert to String";
					final ErrorMessage em = ErrorMessage.newBuilder()
							.withJsonPayload(context.element().toString())
							.withErrorMessage(message)
							.withErrorStackTrace(Throwables.getStackTraceAsString(e))
							.build();
					Metrics.counter(ConvertJsonNodeToStringMessage.class, "FAILED_CONVERSION_TO_STRING").inc();
					context.output(failureTag(), em);
				}
			}
		}).withOutputTags(conversionSuccessStringTag(), TupleTagList.of(failureTag())));
	}

	@AutoValue.Builder
	public abstract static class Builder {

		abstract Builder setConversionSuccessStringTag(TupleTag<KV<String, String>> conversionSuccessStringTag);

		abstract Builder setFailureTag(TupleTag<ErrorMessage> failureTag);

		public abstract ConvertJsonNodeToStringMessage build();

		public Builder withConversionSuccessStringTag(TupleTag<KV<String, String>> conversionSuccessPubSubTag) {
			checkArgument(conversionSuccessPubSubTag != null,
					"withConversionSuccessStringTag(setConversionSuccessStringTag) called with null value.");
			return setConversionSuccessStringTag(conversionSuccessPubSubTag);
		}

		public Builder withFailureTag(TupleTag<ErrorMessage> failureTag) {
			checkArgument(failureTag != null, "withFailureTag(failureTag) called with null value.");
			return setFailureTag(failureTag);
		}
	}

}
