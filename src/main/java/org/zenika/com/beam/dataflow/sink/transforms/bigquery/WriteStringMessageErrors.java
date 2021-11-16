package org.zenika.com.beam.dataflow.sink.transforms.bigquery;

import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.zenika.com.beam.dataflow.sink.transforms.bigquery.SinkToBigQuery.CustomFormatFunction;
import org.zenika.com.beam.dataflow.sink.transforms.bigquery.SinkToBigQuery.DynamicTableDestinationFunction;
import org.zenika.com.beam.dataflow.sink.values.FailsafeElement;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;

@AutoValue
public abstract class WriteStringMessageErrors
		extends PTransform<PCollection<FailsafeElement<KV<String, String>, String>>, WriteResult> {

	private static final long serialVersionUID = 1L;
	public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

	public static Builder newBuilder() {
		return new AutoValue_WriteStringMessageErrors.Builder();
	}

	public abstract String getErrorRecordsTableSchema();

	@Override
	public WriteResult expand(PCollection<FailsafeElement<KV<String, String>, String>> failedRecords) {

		return failedRecords
				.apply("FailedRecordToTableRow", ParDo.of(new FailedStringToTableRowFn()))
				.apply("WriteFailedRecordsToBigQuery", BigQueryIO.<KV<String, TableRow>>write()
						.to(new DynamicTableDestinationFunction())
						.withFormatFunction(new CustomFormatFunction())
						.withJsonSchema(getErrorRecordsTableSchema())
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));
	}

	/**
	 * The {@link FailedStringToTableRowFn} converts string objects which have
	 * failed processing into {@link TableRow} objects which can be output to a
	 * dead-letter table.
	 */
	public static class FailedStringToTableRowFn
			extends DoFn<FailsafeElement<KV<String, String>, String>, KV<String, TableRow>> {

		private static final long serialVersionUID = 1L;
		/**
		 * The formatter used to convert timestamps into a BigQuery compatible <a href=
		 * "https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
		 */
		private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat
				.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

		@ProcessElement
		public void processElement(ProcessContext context) {
			FailsafeElement<KV<String, String>, String> failsafeElement = context.element();
			final String message = failsafeElement.getOriginalPayload().getValue();

			// Format the timestamp for insertion
			String timestamp = TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

			// Build the table row
			final TableRow failedRow = new TableRow()
					.set("timestamp", timestamp)
					.set("errorMessage", failsafeElement.getErrorMessage())
					.set("stacktrace", failsafeElement.getStacktrace());
			// Only set the payload if it's populated on the message.
			if (message != null) {
				failedRow.set("payloadString", message)
						.set("payloadBytes", message.getBytes(StandardCharsets.UTF_8));
			}
			context.output(
					KV.of(failsafeElement.getOriginalPayload().getKey() + DEFAULT_DEADLETTER_TABLE_SUFFIX, failedRow));
		}

	}

	/** Builder for {@link WriteStringMessageErrors}. */
	@AutoValue.Builder
	public abstract static class Builder {
		public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

		public abstract WriteStringMessageErrors build();
	}
}