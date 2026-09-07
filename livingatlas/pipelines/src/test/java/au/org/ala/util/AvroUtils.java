package au.org.ala.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;

/** Utilities for querying AVRO outputs */
@Slf4j
public class AvroUtils {

  public static Map<String, String> readKeysForPath(String path) {

    Map<String, ALAUUIDRecord> records =
        AvroReader.readRecords(HdfsConfigs.nullConfig(), ALAUUIDRecord.class, path);
    Map<String, String> uniqueKeyToUuid = new HashMap<>();
    for (Map.Entry<String, ALAUUIDRecord> record : records.entrySet()) {
      log.debug(record.getValue().getUniqueKey() + " -> " + record.getValue().getUuid());
      uniqueKeyToUuid.put(record.getValue().getUniqueKey(), record.getValue().getUuid());
    }
    return uniqueKeyToUuid;
  }

  public static Map<String, List<Image>> readImages(String path) {

    Map<String, ImageRecord> records =
        AvroReader.readRecords(HdfsConfigs.nullConfig(), ImageRecord.class, path);
    Map<String, List<Image>> images = new HashMap<>();
    for (Map.Entry<String, ImageRecord> record : records.entrySet()) {
      log.debug(record.getValue().getId() + " -> " + record.getValue());
      images.put(record.getKey(), record.getValue().getImageItems());
    }
    return images;
  }
}
