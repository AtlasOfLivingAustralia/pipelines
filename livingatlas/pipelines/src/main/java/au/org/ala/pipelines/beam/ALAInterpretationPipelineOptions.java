package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.FailOnDatasetErrorOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface ALAInterpretationPipelineOptions
    extends InterpretationPipelineOptions, FailOnDatasetErrorOptions {

  @Description("Events pipeline processing enabled")
  @Default.Boolean(true)
  boolean isEventsEnabled();

  void setEventsEnabled(boolean eventsEnabled);
}
