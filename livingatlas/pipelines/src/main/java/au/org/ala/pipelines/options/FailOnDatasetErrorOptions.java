package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options controlling what a pipeline does when it cannot process a dataset at all: the dataset is
 * no longer registered in the collectory, it has no verbatim AVRO to read, or its identifiers
 * changed by more than the permitted percentage and the update was blocked.
 *
 * <p>This does not cover what the validation report says about the data itself, such as a dataset
 * with no unique key terms configured or with empty or duplicated keys. There the step did run and
 * existing records keep the identifiers they already had, so it is reported and left alone. {@code
 * throwErrorOnValidationFail} is the stricter option that fails on those too.
 *
 * <p>The default is {@code false}, which preserves the previous behaviour of logging an error and
 * exiting with 0. Set it to {@code true} to make the step exit non-zero so that the orchestrating
 * job fails instead of silently reporting success for a dataset that never reached the index.
 *
 * <p>Note that running a batch of datasets with this enabled requires the batch to isolate each
 * dataset, otherwise the first failing dataset stops the rest.
 */
public interface FailOnDatasetErrorOptions extends PipelineOptions {

  @Description(
      "Exit with a non-zero status when a dataset cannot be processed at all, instead of logging "
          + "an error and exiting successfully. Does not cover validation report results such as "
          + "missing unique key terms, which throwErrorOnValidationFail covers")
  @Default.Boolean(false)
  Boolean getFailOnDatasetError();

  void setFailOnDatasetError(Boolean failOnDatasetError);
}
