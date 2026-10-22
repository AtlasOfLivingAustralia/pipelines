package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;

/** Main pipeline options necessary for SOLR index for Living atlases */
public interface ALAEsIndexingPipelineOptions extends EsIndexingPipelineOptions {

  @Description("Include sensitive data checks")
  @Default.Boolean(true)
  Boolean getIncludeSensitiveDataChecks();

  void setIncludeSensitiveDataChecks(Boolean includeSensitiveDataChecks);

  @Description("ElasticSearch Connection timeout")
  @Default.Integer(180000)
  Integer getConnectionTimeout();

  void setConnectionTimeout(Integer socketTimeout);

  @Description("ElasticSearch Socket timeout")
  @Default.Integer(30000)
  Integer getSocketTimeout();

  void setSocketTimeout(Integer socketTimeout);
}
