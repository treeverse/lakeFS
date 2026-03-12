package io.lakefs;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * OkHttp interceptor that retries requests on transient failures with exponential backoff and jitter.
 * Retries on server errors (HTTP 408, 429, 500, 502, 503, 504) and connection/timeout exceptions.
 */
public class RetryInterceptor implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(RetryInterceptor.class);

    private static final int HTTP_REQUEST_TIMEOUT = 408;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double jitterFactor;

    public RetryInterceptor(int maxRetries, long initialBackoffMs, long maxBackoffMs, double jitterFactor) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.jitterFactor = jitterFactor;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        if (maxRetries <= 0) {
            return chain.proceed(request);
        }

        Response response = null;
        IOException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                long backoffMs = calculateBackoff(attempt);
                LOG.warn("Retrying request {} {} (attempt {}/{}) after {}ms",
                        request.method(), request.url(), attempt, maxRetries, backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("Retry interrupted");
                }
            }

            // Close previous response body if retrying
            if (response != null) {
                response.close();
                response = null;
            }

            try {
                response = chain.proceed(request);
            } catch (IOException e) {
                lastException = e;
                LOG.warn("Request {} {} failed with IOException (attempt {}/{}): {}",
                        request.method(), request.url(), attempt + 1, maxRetries + 1, e.getMessage());
                if (attempt == maxRetries) {
                    throw e;
                }
                continue;
            }

            if (!isRetryable(response.code()) || attempt == maxRetries) {
                return response;
            }

            LOG.warn("Request {} {} returned status {} (attempt {}/{})",
                    request.method(), request.url(), response.code(), attempt + 1, maxRetries + 1);
        }

        // Should not reach here, but handle edge case
        if (response != null) {
            return response;
        }
        throw lastException != null ? lastException : new IOException("Retry exhausted");
    }

    private long calculateBackoff(int attempt) {
        long backoffMs = (long) (initialBackoffMs * Math.pow(2, attempt - 1));
        backoffMs = Math.min(backoffMs, maxBackoffMs);
        // Apply jitter: backoffMs * (1 +/- jitterFactor)
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterFactor;
        return (long) (backoffMs * jitter);
    }

    private static boolean isRetryable(int statusCode) {
        return statusCode == HTTP_REQUEST_TIMEOUT
                || statusCode == HTTP_TOO_MANY_REQUESTS
                || (statusCode >= 500 && statusCode <= 504);
    }
}
