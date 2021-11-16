package org.zenika.com.beam.dataflow.sink.transforms;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.zenika.com.beam.dataflow.sink.models.BigQueryMessage;
import org.zenika.com.beam.dataflow.sink.models.InputOperation;
import org.zenika.com.beam.dataflow.sink.models.Metadata;
import org.zenika.com.beam.dataflow.sink.models.OutputContent;
import org.zenika.com.beam.dataflow.sink.utils.EventType;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;
import org.zenika.com.beam.dataflow.sink.utils.OperationType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;

@AutoValue
abstract public class ConvertToBigQueryMessage
		extends PTransform<PCollection<InputOperation>, PCollection<KV<String, BigQueryMessage>>> {

	private static final long serialVersionUID = 1L;
	private static final String DATE_TIME_FORMATE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";;
	public static final String INGEST_PUBSUB = "ingestPubsub";
	public static final String KEY = "key";
	public static final String PROCESS_ID = "processId";
	public static final String PROCESS_DATE = "processDate";
	public static final String EVENT_TYPE = "eventType";
	public static final String INPUT_TOPIC = "topic";
	public static final String BIGQUERY_DATASET_ID = "bigQueryDatasetId";
	public static final String BIGQUERY_TABLE_NAME = "bigQueryTableName";
	public static final String BIGQUERY_PROJECT_ID = "bigQueryProjectId";

	public static Builder newBuilder() {
		return new AutoValue_ConvertToBigQueryMessage.Builder();
	}

	public PCollection<KV<String, BigQueryMessage>> expand(PCollection<InputOperation> input) {
		return input.apply("ConvertToOutputOperation",
				ParDo.of(new DoFn<InputOperation, KV<String, BigQueryMessage>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						final InputOperation inputOperation = context.element();
						final Map<String, String> attributesMap = inputOperation.getAttributeMap();
						final Boolean isMutation = EventType.MUTATION.name()
								.equalsIgnoreCase(attributesMap.get(EVENT_TYPE));
						final String key = attributesMap.get(KEY);
						final String operationType = isMutation
								? OperationType.INTEGRATED.name()
								: OperationType.DELETED.name();
						final BigQueryMessage bqMessage = new BigQueryMessage();
						bqMessage.setKey(key);
						bqMessage.setEvent(operationType);
						bqMessage.setTopic(attributesMap.get(INPUT_TOPIC));
						bqMessage.setPublishTime(getCurrentDateTime());
						bqMessage.setTimestamp(System.currentTimeMillis());
						if (isMutation) {
							bqMessage
									.setContent(JSONUtils.ConvertValue(generateContent(inputOperation, operationType)));
						}
						Metrics.counter(ConvertToBigQueryMessage.class, "CONVERSION_TO_BIGQUERY_MESSAGE").inc();
						context.output(KV.of(generateBQTableRef(attributesMap), bqMessage));
					}
				}));
	}

	public static String getCurrentDateTime() {
		final DateFormat df = new SimpleDateFormat(DATE_TIME_FORMATE);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		return df.format(new Date());
	}

	private String generateBQTableRef(final Map<String, String> inputAttributes) {
		return String.format("%s:%s.%s",
				inputAttributes.get(BIGQUERY_PROJECT_ID),
				inputAttributes.get(BIGQUERY_DATASET_ID),
				inputAttributes.get(BIGQUERY_TABLE_NAME));
	}

	private static OutputContent generateContent(final InputOperation inputOperation, final String operationType) {
		final Map<String, String> attributesMap = inputOperation.getAttributeMap();
		final Metadata metadata = new Metadata();
		metadata.setIngestPubsub(attributesMap.get(INGEST_PUBSUB));
		metadata.setProcessDate(attributesMap.get(PROCESS_DATE));
		metadata.setKey(attributesMap.get(KEY));
		metadata.setProcessId(attributesMap.get(PROCESS_ID));
		metadata.setStatus(operationType);
		final JsonNode valueAtPath = inputOperation.getPayload().at("/data");
		final JsonNode data = (valueAtPath != null && !valueAtPath.isMissingNode()) ? valueAtPath : null;
		return new OutputContent(metadata, data);
	}

	@AutoValue.Builder
	public abstract static class Builder {
		public abstract ConvertToBigQueryMessage build();
	}
}
