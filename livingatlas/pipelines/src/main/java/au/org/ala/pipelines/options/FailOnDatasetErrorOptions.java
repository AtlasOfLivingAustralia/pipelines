package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options controlling what a pipeline does when it cannot process a dataset at all, e.g. the
 * dataset has no unique key terms configured, is no longer registered in the collectory, or has no
 * verbatim AVRO to read.
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
      "Exit with a non-zero status when a dataset cannot be processed, instead of logging an "
          + "error and exiting successfully")
  @Default.Boolean(false)
  Boolean getFailOnDatasetError();

  void setFailOnDatasetError(Boolean failOnDatasetError);
}
