package au.org.ala.pipelines.java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import au.org.ala.pipelines.beam.ALAInterpretationPipelineOptions;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.DatasetProcessingException;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * The java variant is the one la-pipelines runs in "local" mode, i.e. for the small datasets. It
 * had the same silent return as the Beam one: a dataset whose verbatim AVRO was never generated
 * went through interpretation without interpreting anything and still exited with 0.
 */
public class ALAVerbatimToInterpretedPipelineTest {

  private static final String BASE_PATH = "/tmp/la-pipelines-test/no-verbatim-java";

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
    ALAVerbatimToInterpretedPipeline.run(options("dr1161", false));
  }

  @Test
  public void testMissingVerbatimFailsWhenRequested() {
    try {
      ALAVerbatimToInterpretedPipeline.run(options("dr1161", true));
      fail("Expected a DatasetProcessingException for a dataset without verbatim AVRO");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.NO_VERBATIM, e.getReason());
      assertTrue(e.getMessage().contains("dr1161"));
    }
  }
}
