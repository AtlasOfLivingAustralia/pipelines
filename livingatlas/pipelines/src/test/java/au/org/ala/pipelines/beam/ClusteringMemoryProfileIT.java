package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Container-free memory profiler for {@link ClusteringPipeline}.
 *
 * <p>Unlike the full ingest ITs, this needs NO services (no SOLR/ES/name-matching/sds, no network):
 * it synthesizes an {@code index-record} AVRO of {@code clustering.records} rows and runs the
 * clustering pipeline directly. Rows are grouped into hash buckets of {@code clustering.groupSize}
 * (all rows in a bucket share scientificName + taxon + lat/lng + date, only the id differs), which
 * is exactly what makes them clustering candidates — so we can dial the GroupByKey pressure.
 *
 * <p>Runner note: {@code ClusteringPipeline} does NOT run on DirectRunner — {@code
 * au.org.ala.clustering.ClusteringCandidates} has no no-arg constructor, so Beam's reflection-based
 * coder throws {@code NoSuchMethodException} at the GroupByKey step. It only runs on SparkRunner
 * (Kryo/Objenesis serialization). On a single small box that means embedded local Spark, which
 * spills shuffle to disk — so the profiling question is whether clustering stays within a low heap
 * as the record count grows. Sweep the heap with surefire {@code argLine}:
 *
 * <pre>
 *   mvn -pl livingatlas/pipelines verify -Dit.test=ClusteringMemoryProfileIT \
 *       -Dclustering.records=500000 -Dclustering.groupSize=1000 \
 *       -Dclustering.runner=SparkRunner -DargLine="-Xmx1g -XX:+UseG1GC"
 * </pre>
 *
 * Emits a single {@code PROFILE_RESULT ...} line (runner, records, groupSize, xmx, peakHeap, wall).
 */
public class ClusteringMemoryProfileIT {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  static final int RECORDS = Integer.getInteger("clustering.records", 200_000);
  static final int GROUP_SIZE = Integer.getInteger("clustering.groupSize", 1_000);
  // DirectRunner can't instantiate ClusteringCandidates (no no-arg ctor); clustering needs Spark.
  static final String RUNNER = System.getProperty("clustering.runner", "SparkRunner");
  static final String BASE = "/tmp/la-pipelines-test/clustering-profile";

  // Generous ceiling: a big DirectRunner run under low heap can be slow before it OOMs.
  @Rule public final Timeout globalTimeout = new Timeout(40, TimeUnit.MINUTES);

  @Test
  public void profileClustering() throws Exception {
    FileUtils.deleteQuietly(new File(BASE));
    generateIndexRecords("drPROF", RECORDS, GROUP_SIZE);

    ClusteringPipelineOptions options =
        PipelinesOptionsFactory.create(
            ClusteringPipelineOptions.class,
            new String[] {
              "--runner=" + RUNNER,
              "--metaFileName=" + ValidationUtils.CLUSTERING_METRICS,
              "--clusteringPath=" + BASE + "/clustering-output",
              "--allDatasetsInputPath=" + BASE + "/all-datasets-path",
              "--inputPath=" + BASE,
              "--outputDebugAvro=false"
            });

    HeapSampler sampler = new HeapSampler();
    Thread t = new Thread(sampler, "heap-sampler");
    t.setDaemon(true);
    System.gc();
    long start = System.currentTimeMillis();
    t.start();
    try {
      ClusteringPipeline.run(options);
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
            + " groupSize="
            + GROUP_SIZE
            + " xmxMB="
            + xmxMb
            + " peakHeapMB="
            + sampler.peakUsedMb()
            + " wallS="
            + wallS);
  }

  /**
   * Writes {@code n} synthetic {@link IndexRecord}s to the all-datasets index-record path, in hash
   * buckets of {@code groupSize} (bucket members share every clustering field except the id).
   */
  private void generateIndexRecords(String dr, int n, int groupSize) throws IOException {
    File dir = new File(BASE + "/all-datasets-path/index-record/" + dr + "/");
    FileUtils.forceMkdir(dir);
    DatumWriter<IndexRecord> datumWriter = new GenericDatumWriter<>(IndexRecord.getClassSchema());
    try (OutputStream output = new FileOutputStream(new File(dir, "index-record.avro"));
        DataFileWriter<IndexRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(BASE_CODEC);
      writer.create(IndexRecord.getClassSchema(), output);
      for (int i = 0; i < n; i++) {
        int g = i / groupSize; // hash bucket
        String id = "rec-" + i;
        String taxon = "taxon-" + g;
        Map<String, String> strings = new HashMap<>();
        Map<String, Double> doubles = new HashMap<>();
        Map<String, Integer> ints = new HashMap<>();
        strings.put("dataResourceUid", dr);
        strings.put("occurrenceID", id);
        strings.put("recordedBy", "Synthetic Observer");
        strings.put("scientificName", "Species " + g);
        strings.put("taxonConceptID", taxon);
        strings.put("speciesID", taxon);
        strings.put("rank", "species");
        strings.put("kingdom", "Animalia");
        strings.put("phylum", "Arthropoda");
        strings.put("class", "Insecta");
        strings.put("order", "Coleoptera");
        strings.put("family", "Mordellidae");
        strings.put("genus", "Mordella");
        strings.put("species", "Species " + g);
        doubles.put("decimalLatitude", -35.0 + g * 0.0001);
        doubles.put("decimalLongitude", 145.0 + g * 0.0001);
        strings.put("countryCode", "AU");
        strings.put("stateProvince", "Victoria");
        ints.put("year", 2016);
        ints.put("month", 11);
        ints.put("day", 27);
        IndexRecord ir =
            IndexRecord.newBuilder()
                .setId(id)
                .setTaxonID(taxon)
                .setDoubles(doubles)
                .setStrings(strings)
                .setInts(ints)
                .build();
        writer.append(ir);
      }
      writer.flush();
    }
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
