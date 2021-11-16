package org.zenika.com.beam.dataflow.sink.transforms.bigquery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;
import org.zenika.com.beam.dataflow.sink.values.FailsafeElement;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Throwables;

/** Common transforms for Teleport BigQueryIO. */
public class BigQueryConverters {

	public static final int MAX_STRING_SIZE_BYTES = 1500;
	public static final int TRUNCATE_STRING_SIZE_CHARS = 401;
	public static final String TRUNCATE_STRING_MESSAGE = "First %d characters of row %s";

	/**
	 * The {@link FailsafeRDOMessageToTableRow} transform converts JSON strings to
	 * {@link TableRow} objects. The transform accepts a {@link FailsafeElement}
	 * object so the original payload of the incoming record can be maintained
	 * across multiple series of transforms.
	 */
	@AutoValue
	public abstract static class FailsafeBigQueryMessageToTableRow
			extends PTransform<PCollection<FailsafeElement<KV<String, String>, String>>, PCollectionTuple> {

		private static final long serialVersionUID = 1L;

		public abstract TupleTag<KV<String, TableRow>> successTag();

		public abstract TupleTag<FailsafeElement<KV<String, String>, String>> failureTag();

		public static <T> Builder newBuilder() {
			return new AutoValue_BigQueryConverters_FailsafeBigQueryMessageToTableRow.Builder();
		}

		/** Builder for {@link FailsafeRDOMessageToTableRow}. */
		@AutoValue.Builder
		public abstract static class Builder {
			public abstract Builder setSuccessTag(TupleTag<KV<String, TableRow>> successTag);

			public abstract Builder setFailureTag(
					TupleTag<FailsafeElement<KV<String, String>, String>> transformDeadletterOut);

			public abstract FailsafeBigQueryMessageToTableRow build();
		}

		public PCollectionTuple expand(PCollection<FailsafeElement<KV<String, String>, String>> failsafeElements) {
			return failsafeElements.apply("JsonToTableRow",
					ParDo.of(new DoFn<FailsafeElement<KV<String, String>, String>, KV<String, TableRow>>() {
						private static final long serialVersionUID = 1L;

						@ProcessElement
						public void processElement(ProcessContext context) {
							final FailsafeElement<KV<String, String>, String> element = context.element();
							final String json = element.getPayload();
							try {
								final TableRow row = convertJsonToTableRow(json);
								context.output(KV.of(element.getOriginalPayload().getKey(), row));
							} catch (Exception e) {
								context.output(failureTag(), FailsafeElement.of(element)
										.setErrorMessage(e.getMessage())
										.setStacktrace(Throwables.getStackTraceAsString(e)));
							}
						}
					})
							.withOutputTags(successTag(), TupleTagList.of(failureTag())));
		}
	}

	/**
	 * Converts a JSON string to a {@link TableRow} object. If the data fails to
	 * convert, a {@link RuntimeException} will be thrown.
	 *
	 * @param json
	 *            The JSON string to parse.
	 * @return The parsed {@link TableRow} object.
	 */
	public static TableRow convertJsonToTableRow(String json) {
		TableRow row;
		try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
			row = JSONUtils.ToTableRow(inputStream);
		} catch (IOException e) {
			throw new RuntimeException("[convertJsonToTableRow] - Failed to serialize json to table row: " + json, e);
		}
		return row;
	}
}
