package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.common.SolrFieldSchema;
import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.DistributionOutlierRecord;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.io.avro.RecordAnnotation;
import org.gbif.pipelines.io.avro.Relationship;
import org.gbif.pipelines.io.avro.Relationships;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Container-free memory profiler for {@link IndexRecordToSolrPipeline}.
 *
 * <p>Needs NO services (no SOLR/ZooKeeper/ES/name-matching, no network): it synthesizes all six
 * AVRO inputs of the pipeline (index-record, jackknife, clustering, expert-distribution outliers,
 * annotations, sampling) and runs the real join graph via {@link
 * IndexRecordToSolrPipeline#run(SolrPipelineOptions, Map, List)}, which takes an already-resolved
 * SOLR schema. The {@code outputAvroToFilePath} option routes the result to AvroIO instead of
 * SolrIO, so no SOLR is contacted, while every record still goes through the joins and is
 * serialized on the way out.
 *
 * <p>The knobs below dial the two shapes that can put a whole key's worth of data on the heap:
 *
 * <ul>
 *   <li>{@code solr.latLngFanOut}: how many index records share one lat/lng. This is the {@code
 *       CoGroupByKey} hot key in the sampling join, consumed via {@code getAll()}.
 *   <li>{@code solr.annotationsPerRecord}: how many annotations share one occurrence id. The
 *       annotation loader does {@code GroupByKey} then copies the group into an {@code ArrayList}
 *       inside the transform, which is the same shape as the clustering OOM that PR #16 fixed.
 *   <li>{@code solr.clusterSize}: relationships carried inside a single {@code Relationships}
 *       record (a per-record list, not a join fan-out).
 * </ul>
 *
 * <p>Runner note: profile on SparkRunner. That is what the ALA stack runs, and on one box it is
 * embedded local Spark, which spills shuffle to disk. Cap the fork heap with the {@code
 * JAVA_TOOL_OPTIONS} env var, because the failsafe plugin in this module hardcodes {@code
 * <argLine>} for the Java 17 {@code --add-opens} flags, so {@code -DargLine} is ignored.
 *
 * <pre>
 *   JAVA_TOOL_OPTIONS="-Xmx1g -XX:+UseG1GC" \
 *   mvn -pl livingatlas/pipelines failsafe:integration-test \
 *       -Dit.test=IndexRecordToSolrMemoryProfileIT \
 *       -Dsolr.records=1000000 -Dsolr.latLngFanOut=200000 -Dsolr.annotationsPerRecord=1
 * </pre>
 *
 * Emits a single {@code PROFILE_RESULT ...} line (runner, records, fan-outs, xmx, peak heap, wall).
 */
public class IndexRecordToSolrMemoryProfileIT {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  static final int RECORDS = Integer.getInteger("solr.records", 200_000);

  /** Index records sharing one lat/lng, the hot key of the sampling CoGroupByKey. */
  static final int LAT_LNG_FAN_OUT = Integer.getInteger("solr.latLngFanOut", 1_000);

  /** Annotations sharing one occurrence id, the hot key of the annotation GroupByKey. */
  static final int ANNOTATIONS_PER_RECORD = Integer.getInteger("solr.annotationsPerRecord", 1);

  /**
   * Extra annotations all keyed on a single occurrence id ({@code rec-0}). This is the direct
   * analogue of the clustering hot key: it sizes the biggest ArrayList the annotation loader
   * builds, independently of the total record count.
   */
  static final int ANNOTATION_HOT_KEY_SIZE = Integer.getInteger("solr.annotationHotKeySize", 0);

  /**
   * Index records forced onto one single lat/lng, on top of the even {@code latLngFanOut} spread.
   * This is what a real portal looks like: millions of distinct coordinates plus a handful of
   * centroids/grid points that attract a disproportionate share of records.
   */
  static final int LAT_LNG_HOT_KEY_SIZE = Integer.getInteger("solr.latLngHotKeySize", 0);

  private static final String HOT_LAT_LNG = "-9.9999,99.9999";

  /** Every per-occurrence input collection covers this many ids. */
  static final int TOTAL_RECORDS = RECORDS + LAT_LNG_HOT_KEY_SIZE;

  /** Relationships inside a single Relationships record. */
  static final int CLUSTER_SIZE = Integer.getInteger("solr.clusterSize", 2);

  /**
   * Salt buckets used to spread lat/lng keys. Must be >= 2: the pipeline calls {@code
   * Random.nextInt(numOfPartitions - 1)}, which throws for the default of 1.
   */
  static final int PARTITIONS = Integer.getInteger("solr.numOfPartitions", 2);

  static final boolean INCLUDE_SAMPLING =
      Boolean.parseBoolean(System.getProperty("solr.includeSampling", "true"));
  static final String RUNNER = System.getProperty("solr.runner", "SparkRunner");
  static final String BASE = "/tmp/la-pipelines-test/solr-join-profile";

  @Rule public final Timeout globalTimeout = new Timeout(60, TimeUnit.MINUTES);

  @Test
  public void profileSolrJoins() throws Exception {
    FileUtils.deleteQuietly(new File(BASE));
    generateInputs();

    SolrPipelineOptions options =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--runner=" + RUNNER,
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--attempt=1",
              "--datasetId=all",
              "--inputPath=" + BASE,
              "--targetPath=" + BASE,
              "--allDatasetsInputPath=" + BASE + "/all-datasets-path",
              "--jackKnifePath=" + BASE + "/jackknife",
              "--clusteringPath=" + BASE + "/clustering",
              "--outlierPath=" + BASE + "/outlier",
              "--annotationsPath=" + BASE + "/annotations",
              "--includeSampling=" + INCLUDE_SAMPLING,
              "--includeJackKnife=true",
              "--includeClustering=true",
              "--includeOutlier=true",
              "--numOfPartitions=" + PARTITIONS,
              // routes the sink to AvroIO, so no SOLR/ZK is contacted
              "--outputAvroToFilePath=" + BASE + "/solr-output/index-record",
              // never used (SolrIO is not applied) but ConnectionConfiguration.create rejects null
              "--zkHost=localhost:9983"
            });

    HeapSampler sampler = new HeapSampler();
    Thread t = new Thread(sampler, "heap-sampler");
    t.setDaemon(true);
    System.gc();
    long start = System.currentTimeMillis();
    t.start();
    try {
      IndexRecordToSolrPipeline.run(options, stubSchemaFields(), stubDynamicFieldPrefixes());
    } finally {
      sampler.stop();
      t.join(2_000);
    }
    long wallS = (System.currentTimeMillis() - start) / 1000;
    long xmxMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    System.out.println(
        "PROFILE_RESULT runner="
            + RUNNER
            + " records="
            + RECORDS
            + " latLngFanOut="
            + LAT_LNG_FAN_OUT
            + " latLngHotKeySize="
            + LAT_LNG_HOT_KEY_SIZE
            + " annotationsPerRecord="
            + ANNOTATIONS_PER_RECORD
            + " annotationHotKeySize="
            + ANNOTATION_HOT_KEY_SIZE
            + " clusterSize="
            + CLUSTER_SIZE
            + " partitions="
            + PARTITIONS
            + " sampling="
            + INCLUDE_SAMPLING
            + " xmxMB="
            + xmxMb
            + " peakHeapMB="
            + sampler.peakUsedMb()
            + " wallS="
            + wallS);
  }

  /** The pipeline only reads this map to type SOLR fields; the join graph does not use it. */
  private static Map<String, SolrFieldSchema> stubSchemaFields() {
    Map<String, SolrFieldSchema> schema = new HashMap<>();
    schema.put("id", new SolrFieldSchema("string", false));
    return schema;
  }

  private static List<String> stubDynamicFieldPrefixes() {
    return Collections.singletonList("dynamicProperties_");
  }

  private void generateInputs() throws IOException {
    generateIndexRecords();
    generateSampleRecords();
    generateJackKnifeRecords();
    generateClusteringRecords();
    generateOutlierRecords();
    generateAnnotationRecords();
  }

  /**
   * Index records keyed on id, with {@code LAT_LNG_FAN_OUT} of them sharing each lat/lng so the
   * sampling join has controllable hot keys.
   */
  private void generateIndexRecords() throws IOException {
    File dir = new File(BASE + "/all-datasets-path/index-record/drPROF/");
    FileUtils.forceMkdir(dir);
    try (DataFileWriter<IndexRecord> writer =
        open(new File(dir, "index-record.avro"), IndexRecord.getClassSchema())) {
      for (int i = 0; i < RECORDS; i++) {
        writer.append(indexRecord(i, false));
      }
      // pile a single hot coordinate on top of the even spread
      for (int i = 0; i < LAT_LNG_HOT_KEY_SIZE; i++) {
        writer.append(indexRecord(RECORDS + i, true));
      }
      writer.flush();
    }
  }

  private static IndexRecord indexRecord(int i, boolean hot) {
    int latLngBucket = i / LAT_LNG_FAN_OUT;
    Map<String, String> strings = new HashMap<>();
    Map<String, Double> doubles = new HashMap<>();
    Map<String, Integer> ints = new HashMap<>();
    strings.put("dataResourceUid", "drPROF");
    strings.put("occurrenceID", "rec-" + i);
    strings.put("scientificName", "Species " + latLngBucket);
    strings.put("countryCode", "AU");
    strings.put("stateProvince", "Victoria");
    doubles.put("decimalLatitude", -35.0 + latLngBucket * 0.0001);
    doubles.put("decimalLongitude", 145.0 + latLngBucket * 0.0001);
    ints.put("year", 2016);
    return IndexRecord.newBuilder()
        .setId("rec-" + i)
        .setTaxonID("taxon-" + latLngBucket)
        .setLatLng(hot ? HOT_LAT_LNG : latLng(latLngBucket))
        .setStrings(strings)
        .setDoubles(doubles)
        .setInts(ints)
        .build();
  }

  private static String latLng(int bucket) {
    return (-35.0 + bucket * 0.0001) + "," + (145.0 + bucket * 0.0001);
  }

  /** One sample record per distinct lat/lng (the pipeline joins these with {@code getOnly}). */
  private void generateSampleRecords() throws IOException {
    File dir = new File(BASE + "/all-datasets-path/sampling/");
    FileUtils.forceMkdir(dir);
    int buckets = (RECORDS + LAT_LNG_FAN_OUT - 1) / LAT_LNG_FAN_OUT;
    try (DataFileWriter<SampleRecord> writer =
        open(new File(dir, "sampling.avro"), SampleRecord.getClassSchema())) {
      for (int b = 0; b < buckets; b++) {
        Map<String, String> strings = new HashMap<>();
        Map<String, Double> doubles = new HashMap<>();
        strings.put("cl22", "Victoria");
        strings.put("cl20", "Bioregion " + b);
        doubles.put("el882", 12.5 + b);
        doubles.put("el889", 3.25 + b);
        writer.append(
            SampleRecord.newBuilder()
                .setLatLng(latLng(b))
                .setStrings(strings)
                .setDoubles(doubles)
                .build());
      }
      if (LAT_LNG_HOT_KEY_SIZE > 0) {
        writer.append(
            SampleRecord.newBuilder()
                .setLatLng(HOT_LAT_LNG)
                .setStrings(Collections.singletonMap("cl22", "Victoria"))
                .setDoubles(Collections.singletonMap("el882", 1.0d))
                .build());
      }
      writer.flush();
    }
  }

  private void generateJackKnifeRecords() throws IOException {
    File dir = new File(BASE + "/jackknife/outliers/");
    FileUtils.forceMkdir(dir);
    try (DataFileWriter<JackKnifeOutlierRecord> writer =
        open(new File(dir, "outliers.avro"), JackKnifeOutlierRecord.getClassSchema())) {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        List<String> items = new ArrayList<>();
        items.add("el882");
        items.add("el889");
        writer.append(
            JackKnifeOutlierRecord.newBuilder().setId("rec-" + i).setItems(items).build());
      }
      writer.flush();
    }
  }

  private void generateClusteringRecords() throws IOException {
    File dir = new File(BASE + "/clustering/relationships/");
    FileUtils.forceMkdir(dir);
    try (DataFileWriter<Relationships> writer =
        open(new File(dir, "relationships-00000.avro"), Relationships.getClassSchema())) {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        List<Relationship> rels = new ArrayList<>(CLUSTER_SIZE);
        for (int r = 0; r < CLUSTER_SIZE; r++) {
          rels.add(
              Relationship.newBuilder()
                  .setRepId("rec-" + (i - i % CLUSTER_SIZE))
                  .setDupId("rec-" + i)
                  .setRepDataset("drPROF")
                  .setDupDataset("drPROF")
                  .setJustification("sameSpecimen,sameCollector")
                  .build());
        }
        writer.append(Relationships.newBuilder().setId("rec-" + i).setRelationships(rels).build());
      }
      writer.flush();
    }
  }

  private void generateOutlierRecords() throws IOException {
    File dir = new File(BASE + "/outlier/all/");
    FileUtils.forceMkdir(dir);
    try (DataFileWriter<DistributionOutlierRecord> writer =
        open(new File(dir, "outliers.avro"), DistributionOutlierRecord.getClassSchema())) {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        writer.append(
            DistributionOutlierRecord.newBuilder()
                .setId("rec-" + i)
                .setDistanceOutOfEDL(0.0d)
                .build());
      }
      writer.flush();
    }
  }

  /**
   * {@code ANNOTATIONS_PER_RECORD} annotations per occurrence id. The loader groups these by id and
   * copies each group into an ArrayList, so this knob directly sizes that list.
   */
  private void generateAnnotationRecords() throws IOException {
    File dir = new File(BASE + "/annotations/drPROF/1/");
    FileUtils.forceMkdir(dir);
    try (DataFileWriter<RecordAnnotation> writer =
        open(new File(dir, "annotations.avro"), RecordAnnotation.getClassSchema())) {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        for (int a = 0; a < ANNOTATIONS_PER_RECORD; a++) {
          writer.append(annotation(i, a));
        }
      }
      // pile a single hot key on top of the even spread
      for (int a = 0; a < ANNOTATION_HOT_KEY_SIZE; a++) {
        writer.append(annotation(0, ANNOTATIONS_PER_RECORD + a));
      }
      writer.flush();
    }
  }

  private static RecordAnnotation annotation(int record, int index) {
    return RecordAnnotation.newBuilder()
        .setId("rec-" + record)
        .setDatasetKey("dk-" + index)
        .setDoi("10.0000/zenodo." + index)
        .setScientificName("Species " + (record / LAT_LNG_FAN_OUT))
        .setOccurrenceRemarks("annotation " + index + " for rec-" + record)
        .build();
  }

  private static <T extends SpecificRecord> DataFileWriter<T> open(
      File file, org.apache.avro.Schema schema) throws IOException {
    DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
    OutputStream output = new FileOutputStream(file);
    DataFileWriter<T> writer = new DataFileWriter<>(datumWriter);
    writer.setCodec(BASE_CODEC);
    writer.create(schema, output);
    return writer;
  }

  /** Polls heap usage and remembers the peak. */
  static final class HeapSampler implements Runnable {
    private volatile boolean running = true;
    private volatile long peakBytes = 0;

    @Override
    public void run() {
      while (running) {
        long used = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        if (used > peakBytes) {
          peakBytes = used;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    void stop() {
      running = false;
    }

    long peakUsedMb() {
      return peakBytes / (1024 * 1024);
    }
  }
}
