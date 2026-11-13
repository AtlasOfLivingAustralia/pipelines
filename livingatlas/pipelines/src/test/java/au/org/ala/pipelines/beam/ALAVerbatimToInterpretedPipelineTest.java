package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import au.org.ala.util.TestUtils;
import au.org.ala.utils.DatasetProcessingException;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * A dataset whose verbatim AVRO was never generated used to go through interpretation without
 * interpreting anything, logging a warning and exiting with 0, so the batch reported success and
 * the dataset silently disappeared from the index.
 */
public class ALAVerbatimToInterpretedPipelineTest {

  private static final String BASE_PATH = "/tmp/la-pipelines-test/no-verbatim";

  @Before
  public void setup() {
    FileUtils.deleteQuietly(new File(BASE_PATH));
  }

  private ALAInterpretationPipelineOptions options(String datasetId, boolean failOnDatasetError) {
    return PipelinesOptionsFactory.create(
        ALAInterpretationPipelineOptions.class,
        new String[] {
          "--datasetId=" + datasetId,
          "--attempt=1",
          "--runner=DirectRunner",
          "--interpretationTypes=ALL",
          "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
          "--targetPath=" + BASE_PATH,
          "--inputPath=" + BASE_PATH + "/" + datasetId + "/1/verbatim/*.avro",
          "--properties=" + TestUtils.getPipelinesConfigFile(),
          "--useExtendedRecordId=true",
          "--failOnDatasetError=" + failOnDatasetError
        });
  }

  /** The historical behaviour: the step does nothing and still reports success. */
  @Test
  public void testMissingVerbatimIsOnlyLoggedByDefault() {
    ALAVerbatimToInterpretedPipeline.run(options("dr1192", false));
  }

  /** With failOnDatasetError the step fails, so the job running it fails too. */
  @Test
  public void testMissingVerbatimFailsWhenRequested() {
    try {
      ALAVerbatimToInterpretedPipeline.run(options("dr1192", true));
      fail("Expected a DatasetProcessingException for a dataset without verbatim AVRO");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.NO_VERBATIM, e.getReason());
      assertTrue(e.getMessage().contains("dr1192"));
    }
  }

  /** The same, for the java (non-Beam) variant used for small datasets. */
  @Test
  public void testMissingVerbatimFailsWhenRequestedInJavaPipeline() {
    try {
      au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline.run(options("dr1161", true));
      fail("Expected a DatasetProcessingException for a dataset without verbatim AVRO");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.NO_VERBATIM, e.getReason());
      assertTrue(e.getMessage().contains("dr1161"));
    }
  }

  @Test
  public void testMissingVerbatimIsOnlyLoggedByDefaultInJavaPipeline() {
    au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline.run(options("dr1161", false));
  }
}
