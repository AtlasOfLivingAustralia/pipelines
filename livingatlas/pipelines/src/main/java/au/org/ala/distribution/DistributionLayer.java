package au.org.ala.distribution;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;
import lombok.Setter;

/**
 * Represent a field in the sampling service. A field is a property derived from a spatial layer.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class DistributionLayer {
  private int gid;
  private String data_resource_uid;
  private String scientific;
  private String pid;
  private String type;
  private String genus_name;
  private String lsid;
  private float area_km;
  private String common_nam;
  private int spcode;
  private String genus_lsid;
  private String bounding_box;
  private String group_name;
  private Boolean endemic;

  @JsonProperty("imageUrl")
  private String image_url;

  private String family_lsid;
  private String specific_n;
  private String wmsurl;
  private String family;
  private int geom_idx;
}
