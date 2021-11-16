package org.zenika.com.beam.dataflow.sink.transforms.bigquery;

import java.io.IOException;
import java.util.Objects;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenika.com.beam.dataflow.sink.DataflowSink;
import org.zenika.com.beam.dataflow.sink.coders.PipelineCoders;
import org.zenika.com.beam.dataflow.sink.utils.ResourceUtils;
import org.zenika.com.beam.dataflow.sink.values.FailsafeElement;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

public class SinkToBigQuery extends PTransform<PCollection<KV<String, String>>, PDone> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SinkToBigQuery.class);

	public PDone expand(PCollection<KV<String, String>> input) {
		PCollectionTuple convertedTableRows = input.apply("ConvertMessageToTableRow", new ConvertToTableRow());
		/*
		 * Write the successful records out to BigQuery
		 */
		WriteResult writeResult = convertedTableRows.get(DataflowSink.TRANSFORM_OUT)
				.apply("WriteSuccessfulRecords",
						BigQueryIO.<KV<String, TableRow>>write()
								.withoutValidation()
								.withCreateDisposition(CreateDisposition.CREATE_NEVER)
								.withWriteDisposition(WriteDisposition.WRITE_APPEND)
								.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS).withAutoSharding()
								.withExtendedErrorInfo()
								.withAutoSharding()
								.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
								// Dynamic table destination
								.to(new DynamicTableDestinationFunction())
								.withFormatFunction(new CustomFormatFunction()));
		/*
		 * 
		 * Elements that failed inserts into BigQuery are extracted and converted to
		 * FailsafeElement
		 */
		PCollection<FailsafeElement<KV<String, String>, String>> failedInserts = writeResult
				.getFailedInsertsWithErr()
				.apply("WrapInsertionErrors",
						MapElements.into(PipelineCoders.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
								.via(SinkToBigQuery::wrapBigQueryInsertError))
				.setCoder(PipelineCoders.FAILSAFE_ELEMENT_CODER);

		// Insert records that failed insert into deadletter table
		PCollectionList.of(failedInserts).and(convertedTableRows.get(DataflowSink.TRANSFORM_DEADLETTER_OUT))
				.apply(Flatten.<FailsafeElement<KV<String, String>, String>>pCollections())
				.apply("WriteFailedRecords",
						WriteStringMessageErrors.newBuilder()
								.setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
								.build());

		return PDone.in(convertedTableRows.getPipeline());
	}

	/**
	 * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
	 *
	 * @param insertError BigQueryInsert error.
	 * @return FailsafeElement object.
	 * @throws RuntimeException When unable to parse error message JSON
	 */
	protected static FailsafeElement<KV<String, String>, String> wrapBigQueryInsertError(
			BigQueryInsertError insertError) {

		FailsafeElement<KV<String, String>, String> failsafeElement;
		try {
			final TableReference table = insertError.getTable();
			final String tableName = String.format("%s:%s.%s",
					table.getProjectId(),
					table.getDatasetId(),
					table.getTableId());
			LOG.trace(String.format("Error TableName %s", tableName));
			Metrics.counter("FaildElementsCounter",
					String.format("failsRecord_%s", insertError.getRow().get("topic"))).inc();
			failsafeElement = FailsafeElement.of(
					KV.of(tableName, insertError.getRow().toPrettyString()), insertError.getRow().toPrettyString());
			failsafeElement.setErrorMessage(insertError.getError().toPrettyString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return failsafeElement;
	}

	public static class CustomFormatFunction implements SerializableFunction<KV<String, TableRow>, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public TableRow apply(KV<String, TableRow> input) {
			return input.getValue();
		}
	}

	public static class DynamicTableDestinationFunction
			implements SerializableFunction<ValueInSingleWindow<KV<String, TableRow>>, TableDestination> {
		private static final long serialVersionUID = 1L;

		@Override
		public TableDestination apply(ValueInSingleWindow<KV<String, TableRow>> input) {
			return new TableDestination(Objects.requireNonNull(input.getValue()).getKey(), "ZENIKA Table");
		}
	}
}
