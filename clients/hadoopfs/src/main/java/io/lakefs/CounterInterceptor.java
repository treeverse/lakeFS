package io.lakefs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

class CounterInterceptor implements Interceptor {
  public static class RequestType {
    private String method;
    private String path;

    public RequestType(String method, String path) {
      this.method = method;
      this.path = path;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((method == null) ? 0 : method.hashCode());
      result = prime * result + ((path == null) ? 0 : path.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      RequestType other = (RequestType) obj;
      if (method == null) {
        if (other.method != null)
          return false;
      } else if (!method.equals(other.method))
        return false;
      if (path == null) {
        if (other.path != null)
          return false;
      } else if (!path.equals(other.path))
        return false;
      return true;
    }

    public String getMethod() {
      return method;
    }

    public String getPath() {
      return path;
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystem.class);
  private Map<RequestType, AtomicLong> counters = new HashMap<>();

  @Override public Response intercept(Interceptor.Chain chain) throws IOException {
      Request request = chain.request();
  
      long t1 = System.nanoTime();
      // LOG.warn(String.format("Sending %s request %s on %s",
      //     request.method(), request.url(), chain.connection()));
      counters.compute(new RequestType(request.method(), request.url().uri().getPath()), (k, v) -> {
        if (v == null) {
          return new AtomicLong(1);
        }
        v.incrementAndGet();
        return v;
      });

      Response response = chain.proceed(request);
      
      // long t2 = System.nanoTime();
      // LOG.warn(String.format("Received response for %s in %.1fms%n%s",
      //     response.request().url(), (t2 - t1) / 1e6d, response.headers()));
  
      return response;
    }

    Map<RequestType, AtomicLong> resetAndGetCounters() {
      Map<RequestType, AtomicLong> ret = counters;
      counters = new HashMap<>();
      return ret;
    }
}