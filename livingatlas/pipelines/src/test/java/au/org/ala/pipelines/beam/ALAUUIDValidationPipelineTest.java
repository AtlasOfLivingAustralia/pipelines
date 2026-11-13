package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.DatasetProcessingException;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Validation writes the report either way, but used to return normally for a dataset that cannot be
 * validated, so the step exited with 0 and nothing downstream noticed.
 */
public class ALAUUIDValidationPipelineTest {

  private static final String BASE_PATH = "/tmp/la-pipelines-test/uuid-validation-silent-errors";

  private MockWebServer collectory;
  private String propertiesFile;

  @Before
  public void setup() {
    FileUtils.deleteQuietly(new File(BASE_PATH));
    ALAPipelinesConfigFactory.clearInstance();
    collectory = TestUtils.startMockCollectoryServer();
    propertiesFile = TestUtils.getPipelinesConfigFile();
  }

  @After
  public void tearDown() throws Exception {
    if (collectory != null) {
      collectory.shutdown();
    }
    ALAPipelinesConfigFactory.clearInstance();
  }

  private UUIDPipelineOptions options(String datasetId, boolean failOnDatasetError) {
    return PipelinesOptionsFactory.create(
        UUIDPipelineOptions.class,
        new String[] {
          "--datasetId=" + datasetId,
          "--attempt=1",
          "--runner=DirectRunner",
          "--metaFileName=" + ValidationUtils.UUID_METRICS,
          "--targetPath=" + BASE_PATH,
          "--inputPath=" + BASE_PATH,
          "--properties=" + propertiesFile,
          "--useExtendedRecordId=true",
          "--failOnDatasetError=" + failOnDatasetError
        });
  }

  /** dr9998 is registered in the mock collectory but has no termsForUniqueKey. */
  @Test
  public void testInvalidDatasetIsOnlyReportedByDefault() throws Exception {
    UUIDPipelineOptions options = options("dr9998", false);
    ALAUUIDValidationPipeline.run(options);
    assertFalse(ValidationUtils.checkValidationFile(options).getValid());
    assertEquals(
        ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED,
        ValidationUtils.checkValidationFile(options).getMessage());
  }

  /**
   * Missing unique key terms is a data problem, not a step that could not run, so it is reported
   * and the step still exits successfully even with failOnDatasetError.
   */
  @Test
  public void testMissingUniqueTermsDoesNotFailOnDatasetError() throws Exception {
    UUIDPipelineOptions options = options("dr9998", true);
    ALAUUIDValidationPipeline.run(options);
    assertEquals(
        ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED,
        ValidationUtils.checkValidationFile(options).getMessage());
  }

  /** The stricter, pre-existing option does fail on it. */
  @Test
  public void testMissingUniqueTermsFailsWithThrowErrorOnValidationFail() throws Exception {
    UUIDPipelineOptions options = options("dr9998", false);
    options.setThrowErrorOnValidationFail(true);
    try {
      ALAUUIDValidationPipeline.run(options);
      fail("Expected a DatasetProcessingException with throwErrorOnValidationFail enabled");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED, e.getReason());
    }
  }

  /** dr9997 has no fixture, so the mock collectory answers 404 for it. */
  @Test
  public void testDeregisteredDatasetFailsWhenRequested() throws Exception {
    try {
      ALAUUIDValidationPipeline.run(options("dr9997", true));
      fail("Expected a DatasetProcessingException for a dataset missing from the collectory");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.METADATA_NOT_AVAILABLE, e.getReason());
    }
  }
}
