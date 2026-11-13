package au.org.ala.utils;

import lombok.Getter;

/**
 * Thrown when a pipeline step cannot process a dataset, e.g. the dataset has no unique key terms
 * configured, is no longer registered in the collectory, or has no verbatim AVRO to read.
 *
 * <p>Historically these conditions were logged and the step returned normally, so the JVM exited
 * with 0 and the orchestrating job (Jenkins, Airflow...) reported success while the dataset
 * silently dropped out of the index. Pipelines now throw this exception when {@code
 * failOnDatasetError} is enabled, and their {@code main} methods turn it into a non-zero exit code.
 *
 * @see au.org.ala.pipelines.options.FailOnDatasetErrorOptions
 */
@Getter
public class DatasetProcessingException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** A machine readable reason, one of the constants in {@link ValidationUtils}. */
  private final String reason;

  public DatasetProcessingException(String reason, String message) {
    super(message);
    this.reason = reason;
  }
}
