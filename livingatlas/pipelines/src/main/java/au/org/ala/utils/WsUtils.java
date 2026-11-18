package au.org.ala.utils;

import au.org.ala.ws.ClientConfiguration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Cache;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.gbif.pipelines.core.config.model.WsConfig;
import retrofit2.converter.jackson.JacksonConverterFactory;

@Slf4j
public class WsUtils {

  /**
   * Construct an OkHTTP client configuration from a pipeline web-service configuration.
   *
   * <p>The pipelinesConfig is not used at present but is available for global defaults such as
   * cache directories, if these ever become a thing. </[>
   *
   * @param wsConfig The specific web service configuration
   * @return A corresponding client configuration
   * @throws IOException if unable to create the configuration
   */
  public static ClientConfiguration createConfiguration(@NonNull WsConfig wsConfig)
      throws IOException {
    return ClientConfiguration.builder()
        .baseUrl(new URL(wsConfig.getWsUrl()))
        .cache(wsConfig.getCacheSizeMb() > 0)
        .cacheSize(wsConfig.getCacheSizeMb() * 1024 * 1024)
        .timeOut(wsConfig.getTimeoutSec() * 1000)
        .build();
  }

  public static <T> T createClientWithRetries(
      WsConfig wsConfig, Class<T> theClass, ObjectMapper om) {
    OkHttpClient okHttpClient =
        createOKClient(
            wsConfig,
            wsConfig.getRetryConfig().getMaxAttempts(),
            wsConfig.getRetryConfig().getInitialIntervalMillis());

    return new retrofit2.Retrofit.Builder()
        .client(okHttpClient)
        .baseUrl(wsConfig.getWsUrl())
        .addConverterFactory(JacksonConverterFactory.create(om))
        .validateEagerly(true)
        .build()
        .create(theClass);
  }

  public static <T> T createClient(WsConfig wsConfig, Class<T> theClass) {

    OkHttpClient okHttpClient = createOKClient(wsConfig);
    // this is for https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/113
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    return new retrofit2.Retrofit.Builder()
        .client(okHttpClient)
        .baseUrl(wsConfig.getWsUrl())
        .addConverterFactory(JacksonConverterFactory.create(om))
        .validateEagerly(true)
        .build()
        .create(theClass);
  }

  public static <T> T createClient(
      OkHttpClient okHttpClient, WsConfig wsConfig, Class<T> theClass) {

    // this is for https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/113
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    return new retrofit2.Retrofit.Builder()
        .client(okHttpClient)
        .baseUrl(wsConfig.getWsUrl())
        .addConverterFactory(JacksonConverterFactory.create(om))
        .validateEagerly(true)
        .build()
        .create(theClass);
  }

  /** Create an OKHttpClient adding headers to the requests (without retries). */
  public static OkHttpClient createOKClient(WsConfig config) {
    return createOKClient(config, 0, 0L);
  }

  /**
   * Create an OKHttpClient adding headers and retry handling to the requests.
   *
   * @param config The web service configuration
   * @param maxRetries Maximum number of retries after the initial attempt (0 disables retries)
   * @param initialDelayMillis Initial backoff delay in milliseconds
   */
  public static OkHttpClient createOKClient(
      WsConfig config, int maxRetries, long initialDelayMillis) {
    OkHttpClient.Builder clientBuilder =
        new OkHttpClient.Builder()
            .connectTimeout(config.getTimeoutSec(), TimeUnit.SECONDS)
            .readTimeout(config.getTimeoutSec(), TimeUnit.SECONDS);
    if (config.getCacheSizeMb() > 0) {
      clientBuilder.cache(createCache(config.getCacheSizeMb()));
    }
    clientBuilder.addInterceptor(
        chain -> {
          Request.Builder builder = chain.request().newBuilder();
          config.getHttpHeaders().forEach(builder::addHeader);
          Request request = builder.build();
          return chain.proceed(request);
        });
    if (maxRetries > 0) {
      clientBuilder.addInterceptor(new RetryInterceptor(maxRetries, initialDelayMillis));
      clientBuilder.retryOnConnectionFailure(false);
    }
    return clientBuilder.build();
  }

  private static Cache createCache(long maxSize) {
    try {
      String cacheName = System.currentTimeMillis() + "-wsCache";
      File httpCacheDirectory = Files.createTempDirectory(cacheName).toFile();
      httpCacheDirectory.deleteOnExit();
      log.info("Cache file created - {}", httpCacheDirectory.getAbsolutePath());
      return new Cache(httpCacheDirectory, maxSize);
    } catch (IOException var4) {
      throw new IllegalStateException(
          "Cannot run without the ability to create temporary cache directory", var4);
    }
  }

  /**
   * Interceptor that retries failed requests with waiting twice as long after each failed try, up
   * to 30 seconds
   */
  private static class RetryInterceptor implements Interceptor {
    private final int maxRetries;
    private final long initialDelayMillis;

    public RetryInterceptor(int maxRetries, long initialDelayMillis) {
      this.maxRetries = maxRetries;
      this.initialDelayMillis = initialDelayMillis;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
      Request request = chain.request();

      for (int attempt = 0; ; attempt++) {
        if (chain.call().isCanceled()) {
          throw new IOException("Cancelled");
        }
        Response response;
        try {
          response = chain.proceed(request);
        } catch (IOException e) {
          if (chain.call().isCanceled()
              || !shouldRetryException(request, e)
              || attempt >= maxRetries) {
            throw e;
          }
          log.warn(
              "Request to {} failed; retrying {}/{}: {}",
              request.url(),
              attempt + 1,
              maxRetries,
              e.toString());
          waitBeforeRetry(attempt, chain);
          continue;
        }
        // Return response only if:
        //  POST request (don't want to re-submit a for eg: batch post request which may take longer
        // to process)
        //  a successful GET, request isn’t retryable or retries are exhausted
        if (!"GET".equals(request.method())
            || !shouldRetryStatus(response.code())
            || attempt >= maxRetries) {
          return response;
        }
        // If it reaches here it means there's an attempt to retry
        int status = response.code();
        response.close();
        log.warn(
            "Request to {} returned {}; retrying {}/{}",
            request.url(),
            status,
            attempt + 1,
            maxRetries);
        waitBeforeRetry(attempt, chain);
      }
    }

    private void waitBeforeRetry(int attempt, Chain chain) throws IOException {
      // Cap the delay at 30 seconds.
      long base = (long) Math.min(30_000d, initialDelayMillis * Math.pow(2d, attempt));
      long delay = ThreadLocalRandom.current().nextLong(base / 2, base + 1);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Retry interrupted", e);
      }
      if (chain.call().isCanceled()) {
        throw new IOException("Cancelled");
      }
    }

    private boolean shouldRetryStatus(int statusCode) {
      return statusCode == 429
          || statusCode == 500
          || statusCode == 502
          || statusCode == 503
          || statusCode == 504;
    }

    // Resolve issue with temporary Connection Refuse error which may be due to apt_daily_upgrade
    // script running on sampling server
    private boolean shouldRetryException(Request request, IOException e) {
      boolean failedToConnect = e instanceof UnknownHostException || e instanceof ConnectException;
      if ("GET".equals(request.method())) {
        return failedToConnect || e instanceof SocketTimeoutException;
      }
      // Only retry the submitIntersectBatch (post operation) if there was
      // DNS failure or connection refused on the submitIntersectBatch exception
      // We don't want to resend the same batch which is already submitted
      if ("POST".equals(request.method())) {
        return failedToConnect;
      }

      return false;
    }
  }
}
