package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Test;

/**
 * Beam rejects any option the requested interface does not declare, so a pipeline whose main()
 * builds narrower options than the config section it reads dies at startup as soon as anything is
 * added to that section. {@link ALAVerbatimToEventPipeline} did exactly that: it read the
 * "interpret" section into a plain {@code InterpretationPipelineOptions}.
 *
 * <p>These tests parse the shipped la-pipelines.yaml the same way the mains do, so a key added to
 * "interpret" or "uuid" that the ALA options classes do not declare fails here.
 *
 * <p>They cannot catch a main that builds narrower options than these: Beam's options registry is
 * JVM wide, so once any test has built ALAInterpretationPipelineOptions the narrower interface
 * parses the section too. In production each step is its own JVM, where it does not. Checking that
 * means starting each main with the shipped config.
 */
public class ALAVerbatimToEventPipelineTest {

  private static final String CONFIG = "../configs/la-pipelines.yaml";

  private String[] sectionArgs(String section) throws Exception {
    return new CombinedYamlConfiguration(
            "--datasetId=dr893", "--fsPath=/data", "--config=" + CONFIG)
        .toArgs("general", section);
  }

  /** The options every main that reads the "interpret" section must be able to build. */
  @Test
  public void testInterpretSectionParsesIntoAlaOptions() throws Exception {
    ALAInterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class, sectionArgs("interpret"));
    assertNotNull(options);
    // shipped default: the historical behaviour is preserved unless a deployment opts in
    assertFalse(options.getFailOnDatasetError());
  }

  /** The same for the "uuid" section, read by minting, validation and the migration pipeline. */
  @Test
  public void testUuidSectionParsesIntoUuidOptions() throws Exception {
    UUIDPipelineOptions options =
        PipelinesOptionsFactory.create(UUIDPipelineOptions.class, sectionArgs("uuid"));
    assertNotNull(options);
    assertFalse(options.getFailOnDatasetError());
    assertFalse(options.getThrowErrorOnValidationFail());
    assertFalse(ValidationUtils.failOnDatasetError(options));
  }
}
