package au.org.ala.pipelines.beam;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.*;

import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.util.AvroUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.Image;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ImageServicePipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  // Safety net to prevent indefinite hangs in CI
  @Rule public final Timeout globalTimeout = new Timeout(10, MINUTES);

  @Before
  public void setup() throws Exception {
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/image-service"));
  }

  /** Test the generation of UUIDs for datasets that are use non-DwC terms for unique key terms */
  @Test
  public void testNonDwC() throws Exception {
    // dr1864 - has deviceId
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/image-service/dr893", "image-service");

    Map<String, List<Image>> imagesAvro =
        AvroUtils.readImages(
            "/tmp/la-pipelines-test/image-service/dr893/1/images/image-record-*.avro");

    assertTrue(
        imagesAvro
            .keySet()
            .containsAll(
                Arrays.asList("not-an-uuid-1", "not-an-uuid-2", "not-an-uuid-3", "not-an-uuid-4")));
    assertFalse(imagesAvro.containsKey("not-an-uuid-5"));

    List<Image> image_uuid_1 = imagesAvro.get("not-an-uuid-1");
    assertEquals(2, image_uuid_1.size());
    // the imageID from image service where the identifier matches the image url
    // http://www.bowerbird.org.au/observations/49875/fakeImage.jpg,
    // http://www.bowerbird.org.au/observations/48531/fakeImage2.jpg
    List<String> identifiers =
        image_uuid_1.stream()
            .map(image -> image.get("identifier").toString())
            .collect(java.util.stream.Collectors.toList());

    assertTrue(identifiers.containsAll(Arrays.asList("image-service-id-1", "image-service-id-2")));

    image_uuid_1.forEach(
        image -> {
          assertEquals("image/jpg", image.getFormat());
          assertEquals("CC0", image.getLicense());
        });

    List<Image> image_uuid_2 = imagesAvro.get("not-an-uuid-2");
    assertEquals(1, image_uuid_2.size());
    // image url http://www.bowerbird.org.au/observations/98064/fakeImage.jpg
    assertEquals("image-service-id-3", image_uuid_2.get(0).get("identifier"));
    image_uuid_2.forEach(
        image -> {
          assertEquals("image/jpg", image.getFormat());
          assertEquals("CC0", image.getLicense());
        });

    List<Image> image_uuid_3 = imagesAvro.get("not-an-uuid-3");
    assertEquals(1, image_uuid_3.size());
    // image url http://www.bowerbird.org.au/observations/4810/fakeImage.jpg
    assertEquals("image-service-id-4", image_uuid_3.get(0).get("identifier"));
    image_uuid_3.forEach(
        image -> {
          assertEquals("image/jpg", image.getFormat());
          assertEquals("CC0", image.getLicense());
        });

    List<Image> image_uuid_4 = imagesAvro.get("not-an-uuid-4");
    assertEquals(1, image_uuid_4.size());
    // image url http://www.bowerbird.org.au/observations/76433/fakeImage.jpg
    assertEquals("image-service-id-5", image_uuid_4.get(0).get("identifier"));
    image_uuid_4.forEach(
        image -> {
          assertEquals("image/jpg", image.getFormat());
          assertEquals("CC0", image.getLicense());
        });
  }

  public void loadTestDataset(String datasetID, String inputPath, String testDir)
      throws IOException {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--pipelineStep=DWCA_TO_VERBATIM",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
              "--inputPath=/tmp/la-pipelines-test/"
                  + testDir
                  + "/"
                  + datasetID
                  + "/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    ImageServicePipelineOptions imageOptions =
        PipelinesOptionsFactory.create(
            ImageServicePipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.IMAGE_SERVICE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
              "--inputPath=/tmp/la-pipelines-test/" + testDir,
              "--properties=" + itUtils.getPropertiesFilePath()
            });

    String absolutePath = new File("src/test/resources").getAbsolutePath();
    String imageServiceExportPath =
        absolutePath + "/" + testDir + "/" + datasetID + "/image-service-export.csv";
    String imageServiceExportPathGz = imageServiceExportPath + ".gz";

    TestUtils.compressGzip(imageServiceExportPath, imageServiceExportPathGz);

    ImageServiceSyncPipeline.run(imageOptions, imageServiceExportPathGz);
  }
}
