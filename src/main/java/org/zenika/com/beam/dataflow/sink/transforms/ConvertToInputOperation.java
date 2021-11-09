package org.zenika.com.beam.dataflow.sink.transforms;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Map;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenika.com.beam.dataflow.sink.common.ErrorMessage;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.value.AutoValue;
import com.google.common.base.Throwables;

@AutoValue
abstract public class ConvertToInputOperation extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ConvertToInputOperation.class);

	public static Builder newBuilder() {
		return new AutoValue_ConvertToInputOperation.Builder();
	}

	abstract TupleTag<InputOperation> successTag();

	abstract TupleTag<ErrorMessage> failureTag();

	@Override
	public PCollectionTuple expand(PCollection<PubsubMessage> input) {
		return input.apply("ConvertToInputOperation", ParDo.of(new DoFn<PubsubMessage, InputOperation>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext context) throws JsonProcessingException {
				final Map<String, String> pubsubAttributesMap = context.element().getAttributeMap();

				try {
					final InputOperation inputOperation = new InputOperation(context.element());
					final Map<String, String> attributes = inputOperation.getAttributeMap();
					attributes.put(ConvertToOutputOperation.INGEST_PUBSUB, context.timestamp().toString());
					inputOperation.setAttributeMap(attributes);
					System.out.println(inputOperation);
					context.output(successTag(), inputOperation);
					Metrics.counter(ConvertToInputOperation.class, "SUCCESS_CONVERSION_TO_INPUTOPERATION").inc();
				} catch (IOException e) {
					final String message = "[ConvertToInputOperation] Invalid Json message";
					final ErrorMessage em = ErrorMessage.newBuilder()
							.withJsonPayload(new String(context.element().getPayload())).withErrorMessage(message)
							.withErrorStackTrace(Throwables.getStackTraceAsString(e)).build();
					LOG.error(message.concat(": {}"), JSONUtils.ToJsonString(em));
					context.output(failureTag(), em);
					Metrics.counter(ConvertToInputOperation.class, "FAILURE_CONVERSION_TO_INPUTOPERATION").inc();
				}
			}
		}).withOutputTags(successTag(), TupleTagList.of(failureTag())));
	}

	@AutoValue.Builder
	public abstract static class Builder {
		abstract Builder setSuccessTag(TupleTag<InputOperation> successTag);

		abstract Builder setFailureTag(TupleTag<ErrorMessage> failureTag);

		public abstract ConvertToInputOperation build();

		public Builder withSuccessTag(TupleTag<InputOperation> successTag) {
			checkArgument(successTag != null, "withSuccessTag(successTag) called with null value.");
			return setSuccessTag(successTag);
		}

		public Builder withFailureTag(TupleTag<ErrorMessage> failureTag) {
			checkArgument(failureTag != null, "withFailureTag(failureTag) called with null value.");
			return setFailureTag(failureTag);
		}

	}
}
