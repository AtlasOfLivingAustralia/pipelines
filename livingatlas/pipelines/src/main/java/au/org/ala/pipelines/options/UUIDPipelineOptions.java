package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Options for running UUID based pipelines. */
public interface UUIDPipelineOptions
    extends InterpretationPipelineOptions, FailOnDatasetErrorOptions {

  @Description("The number of backups to keep")
  @Default.Integer(10)
  Integer getNumberOfBackupsToKeep();

  void setNumberOfBackupsToKeep(Integer numberOfBackupsToKeep);

  @Description("The permitted percentage change in UUIDs")
  @Default.Integer(50)
  Integer getPercentageChangeAllowed();

  void setPercentageChangeAllowed(Integer percentageChangeAllowed);

  @Description("Override permitted percentage change in UUIDs check")
  @Default.Boolean(false)
  Boolean getOverridePercentageCheck();

  void setOverridePercentageCheck(Boolean overridePercentageCheck);

  /**
   * @deprecated superseded by {@link FailOnDatasetErrorOptions#getFailOnDatasetError()}, which
   *     covers every condition that stops a dataset being processed, not just the validation
   *     report. Still honoured so existing configurations keep working.
   */
  @Deprecated
  @Description("Throw error on validation fail")
  @Default.Boolean(false)
  Boolean getThrowErrorOnValidationFail();

  void setThrowErrorOnValidationFail(Boolean throwErrorOnValidationFail);
}
