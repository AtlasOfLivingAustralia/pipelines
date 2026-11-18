package au.org.ala.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * The validation report is the half of the detection that does not depend on any pipeline option:
 * it lists every dataset that is not in a valid state for indexing, so a job can be made to fail on
 * it even while the pipelines themselves keep exiting with 0.
 */
public class ValidationReportWriterTest {

  private static final String BASE_PATH = "/tmp/la-pipelines-test/validation-report";

  @Before
  public void setup() throws Exception {
    FileUtils.deleteQuietly(new File(BASE_PATH));
    // a dataset directory with no metrics at all: nothing has been validated for it
    FileUtils.forceMkdir(new File(BASE_PATH + "/data/dr9999/1"));
  }

  @Test
  public void testInvalidDatasetsAreReported() throws Exception {
    ValidationReportWriter writer = new ValidationReportWriter();
    writer.setInputPath(BASE_PATH + "/data");
    writer.setTargetPath(BASE_PATH + "/ready-to-index.csv");
    writer.setFullReportPath(BASE_PATH + "/full-report.csv");
    writer.setAttempt(1);
    writer.setCheckSolr(false);
    writer.setCheckSDS(false);

    List<String> invalid = writer.run();

    assertEquals(1, invalid.size());
    assertEquals("dr9999:" + ValidationUtils.NOT_VALIDATED, invalid.get(0));

    String report = FileUtils.readFileToString(new File(BASE_PATH + "/full-report.csv"), "UTF-8");
    assertTrue(report.contains("dr9999"));
    assertTrue(report.contains(ValidationUtils.NOT_VALIDATED));
  }
}
