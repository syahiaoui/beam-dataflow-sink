package org.zenika.com.beam.dataflow.sink.common;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;

/**
 * A class to hold messages that fail validation either because the json is not
 * well formed or because the key element required to be present within the json
 * is missing. The class encapsulates the original payload in addition to a
 * error message and an optional stack trace to help with identifying the root
 * cause in a subsequent reprocessing attempt/debugging.
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = AutoValue_ErrorMessage.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class ErrorMessage {
	public static Builder newBuilder() {
		return new AutoValue_ErrorMessage.Builder();
	}

	@JsonProperty("jsonPayload")
	public abstract String jsonPayload();

	@JsonProperty("errorMessage")
	public abstract String errorMessage();

	@Nullable
	@JsonProperty("errorStackTrace")
	public abstract String errorStackTrace();

	@AutoValue.Builder
	public abstract static class Builder {
		abstract Builder setJsonPayload(String jsonPayload);

		abstract Builder setErrorMessage(String errorMessage);

		abstract Builder setErrorStackTrace(String errorStackTrace);

		public abstract ErrorMessage build();

		public Builder withJsonPayload(String jsonPayload) {
			checkArgument(jsonPayload != null, "withJsonPayload(jsonPayload) called with null value.");
			return setJsonPayload(jsonPayload);
		}

		public Builder withErrorMessage(String errorMessage) {
			checkArgument(errorMessage != null, "withErrorMessage(errorMessage) called with null value.");
			return setErrorMessage(errorMessage);
		}

		public Builder withErrorStackTrace(String errorStackTrace) {
			checkArgument(errorStackTrace != null, "withErrorStackTrace(errorStackTrace) called with null value.");
			return setErrorStackTrace(errorStackTrace);
		}
	}
}
