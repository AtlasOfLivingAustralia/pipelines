package au.org.ala.pipelines.beam;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.ElasticUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class CompleteIngestPipelineESTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "complete_occ_es_it";

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  // Safety net to prevent indefinite hangs in CI
  @Rule public final Timeout globalTimeout = new Timeout(10, MINUTES);

  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    // (use isolated temp workspace instead of hardcoded /tmp path)
    final Path workDir = tmp.newFolder("complete-pipeline-es").toPath().toAbsolutePath();
    final String targetPath = workDir.toString();
    Files.createDirectories(workDir.resolve("all-datasets"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Make Spark runner more predictable in CI
    System.setProperty("spark.ui.enabled", "false");

    // Step 1: load a dataset and verify all records have a UUID associated
    final String datasetID = "dr893";
    loadTestDataset(datasetID, absolutePath + "/complete-pipeline/" + datasetID, targetPath);

    // clear SOLR index
    ElasticUtils.refreshIndex(INDEX_NAME + "_" + datasetID);

    // Check record for occurrences
    long occRecordCount = ElasticUtils.getRecordCount(INDEX_NAME);
    assertEquals(6, occRecordCount);

    long occBasisOfRecordCount =
        ElasticUtils.getRecordCount(INDEX_NAME, "basisOfRecord", "HUMAN_OBSERVATION");
    assertEquals(6, occBasisOfRecordCount);

    long occDatasetKeyCount = ElasticUtils.getRecordCount(INDEX_NAME, "datasetKey", "dr893");
    assertEquals(6, occDatasetKeyCount);

    long occTaxonKingdomCount =
        ElasticUtils.getRecordCount(INDEX_NAME, "taxonomy.name", "Animalia");
    assertEquals(6, occTaxonKingdomCount);

    long occTaxonClassCount = ElasticUtils.getRecordCount(INDEX_NAME, "taxonomy.name", "Insecta");
    assertEquals(3, occTaxonClassCount);

    long occTaxonSpeciesCount =
        ElasticUtils.getRecordCount(INDEX_NAME, "taxonomy.name", "Erythrotriorchis radiatus");
    assertEquals(1, occTaxonSpeciesCount);

    long occOccId1Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-1");
    assertEquals(1, occOccId1Count);

    long occOccId2Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-2");
    assertEquals(1, occOccId2Count);

    long occOccId3Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-3");
    assertEquals(1, occOccId3Count);

    long occOccId4Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-4");
    assertEquals(1, occOccId4Count);

    long occOccId5Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-5");
    assertEquals(1, occOccId5Count);

    long occOccId6Count =
        ElasticUtils.getRecordCount(INDEX_NAME, "occurrenceId.keyword", "not-an-uuid-6");
    assertEquals(1, occOccId6Count);

    System.out.println("Finished");
  }

  public void loadTestDataset(String datasetID, String inputPath, String targetPath)
      throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=" + targetPath,
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=" + targetPath,
              // read verbatim from the dataset-specific folder
              "--inputPath=" + targetPath + "/" + datasetID + "/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=" + targetPath,
              "--inputPath=" + targetPath,
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=" + targetPath,
              "--inputPath=" + targetPath,
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    // index
    IndexingPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=" + targetPath,
              "--inputPath=" + targetPath,
              "--allDatasetsInputPath=" + targetPath + "/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--includeSensitiveDataChecks=true",
              "--includeImages=false"
            });

    // check ready for index - should be true as includeSampling=true and sampling now generated
    assertTrue(ValidationUtils.checkReadyForIndexing(solrOptions).getValid());

    IndexRecordPipeline.run(solrOptions);

    // export lat lngs
    SamplingPipelineOptions samplingOptions =
        PipelinesOptionsFactory.create(
            SamplingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=" + targetPath,
              "--inputPath=" + targetPath,
              "--allDatasetsInputPath=" + targetPath + "/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler lc = new LayerCrawler();
    lc.run(samplingOptions);

    String esSchemaPath =
        Paths.get("src/test/resources/complete-event-pipeline/es-event-core-schema.json")
            .toAbsolutePath()
            .toString();

    // run event to occurrence ES pipeline
    ALAOccurrenceToEsIndexPipeline.main(
        new String[] {
          "--datasetId=" + datasetID,
          "--attempt=1",
          "--runner=SparkRunner",
          "--targetPath=" + targetPath,
          "--inputPath=" + targetPath,
          "--metaFileName=" + targetPath + "/es-meta.yml",
          "--esSchemaPath=" + esSchemaPath,
          "--esAlias=" + INDEX_NAME,
          "--esIndexName=" + INDEX_NAME + "_" + datasetID,
          "--config=" + itUtils.getPropertiesFilePath()
        });
  }
}
