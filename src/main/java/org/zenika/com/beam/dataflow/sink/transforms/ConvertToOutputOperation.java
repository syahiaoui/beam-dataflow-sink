package org.zenika.com.beam.dataflow.sink.transforms;

import java.util.Map;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.models.Metadata;
import org.zenika.com.beam.dataflow.sink.models.OutputOperation;
import org.zenika.com.beam.dataflow.sink.utils.EventType;
import org.zenika.com.beam.dataflow.sink.utils.OperationType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;

@AutoValue
abstract public class ConvertToOutputOperation
		extends PTransform<PCollection<InputOperation>, PCollection<OutputOperation>> {

	private static final long serialVersionUID = 1L;
	public static final String INGEST_PUBSUB = "ingestPubsub";
	public static final String KEY = "key";
	public static final String PROCESS_ID = "processId";
	public static final String PROCESS_DATE = "processDate";
	public static final String EVENT_TYPE = "eventType";

	public static Builder newBuilder() {
		return new AutoValue_ConvertToOutputOperation.Builder();
	}

	public PCollection<OutputOperation> expand(PCollection<InputOperation> input) {
		return input.apply("ConvertToOutputOperation", ParDo.of(new DoFn<InputOperation, OutputOperation>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext context) {
				final InputOperation inputOperation = context.element();
				final Map<String, String> attributesMap = inputOperation.getAttributeMap();
				final String operationType = EventType.MUTATION.name().equalsIgnoreCase(attributesMap.get(EVENT_TYPE))
						? OperationType.INTEGRATED.name()
						: OperationType.DELETED.name();
				final Metadata metadata = new Metadata();
				metadata.setIngestPubsub(attributesMap.get(INGEST_PUBSUB));
				metadata.setProcessDate(attributesMap.get(PROCESS_DATE));
				metadata.setKey(attributesMap.get(KEY));
				metadata.setProcessId(attributesMap.get(PROCESS_ID));
				metadata.setStatus(operationType);
				final JsonNode valueAtPath = inputOperation.getPayload().at("data");
				final JsonNode data = (valueAtPath == null || valueAtPath.isMissingNode()) ? valueAtPath : null;
				Metrics.counter(ConvertToOutputOperation.class, "CONVERSION_TO_OUTPUT_OPERATION").inc();
				context.output(new OutputOperation(metadata, data));
			}
		}));
	}

	@AutoValue.Builder
	public abstract static class Builder {
		public abstract ConvertToOutputOperation build();

	}
}
