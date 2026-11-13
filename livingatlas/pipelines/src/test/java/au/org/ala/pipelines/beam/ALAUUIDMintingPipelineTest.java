package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
 * UUID minting used to log an error and return for datasets it could not mint identifiers for, so
 * the JVM exited with 0 and the batch reported success while the dataset never reached the index.
 *
 * <p>The two cases covered here are a dataset that is no longer registered in the collectory (the
 * web service answers 404) and a dataset whose collectory entry has no {@code termsForUniqueKey}.
 */
public class ALAUUIDMintingPipelineTest {

  private static final String BASE_PATH = "/tmp/la-pipelines-test/uuid-silent-errors";

  private MockWebServer collectory;
  private String propertiesFile;

  @Before
  public void setup() throws Exception {
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

  /** dr9999 has no fixture, so the mock collectory answers 404 for it. */
  @Test
  public void testDeregisteredDatasetFailsWhenRequested() throws Exception {
    try {
      ALAUUIDMintingPipeline.run(options("dr9999", true));
      fail("Expected a DatasetProcessingException for a dataset missing from the collectory");
    } catch (DatasetProcessingException e) {
      assertEquals(ValidationUtils.METADATA_NOT_AVAILABLE, e.getReason());
    }
  }

  @Test
  public void testDeregisteredDatasetIsOnlyLoggedByDefault() throws Exception {
    UUIDPipelineOptions options = options("dr9999", false);
    ALAUUIDMintingPipeline.run(options);
    // the dataset is recorded as invalid in the validation report, but the step reports success
    assertFalse(ValidationUtils.checkValidationFile(options).getValid());
    assertEquals(
        ValidationUtils.METADATA_NOT_AVAILABLE,
        ValidationUtils.checkValidationFile(options).getMessage());
  }

  /**
   * dr9998 is registered but has no termsForUniqueKey configured. That is a data problem, not a
   * step that could not run: the records keep whatever identifiers they already had, so even with
   * failOnDatasetError the step reports it and exits successfully.
   */
  @Test
  public void testMissingUniqueTermsDoesNotFailOnDatasetError() throws Exception {
    UUIDPipelineOptions options = options("dr9998", true);
    ALAUUIDMintingPipeline.run(options);
    assertEquals(
        ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED,
        ValidationUtils.checkValidationFile(options).getMessage());
  }

  @Test
  public void testMissingUniqueTermsIsReportedByDefault() throws Exception {
    UUIDPipelineOptions options = options("dr9998", false);
    ALAUUIDMintingPipeline.run(options);
    assertEquals(
        ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED,
        ValidationUtils.checkValidationFile(options).getMessage());
  }

  /**
   * The stricter, pre-existing option still fails on anything the validation report rejects,
   * missing unique key terms included.
   */
  @Test
  public void testDeprecatedThrowErrorOnValidationFailStillFails() throws Exception {
    UUIDPipelineOptions options = options("dr9998", false);
    options.setThrowErrorOnValidationFail(true);
    try {
      ALAUUIDMintingPipeline.run(options);
      fail("Expected a DatasetProcessingException with throwErrorOnValidationFail enabled");
    } catch (DatasetProcessingException e) {
      assertTrue(e.getMessage().contains("dr9998"));
    }
  }
}
