package io.lakefs;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class RetryInterceptorTest {

    private MockWebServer server;
    private OkHttpClient client;

    @Before
    public void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @After
    public void tearDown() throws IOException {
        server.shutdown();
    }

    private OkHttpClient buildClient(int maxRetries, long initialBackoffMs, long maxBackoffMs, double jitter) {
        return new OkHttpClient.Builder()
                .addInterceptor(new RetryInterceptor(maxRetries, initialBackoffMs, maxBackoffMs, jitter))
                .build();
    }

    @Test
    public void testNoRetryOnSuccess() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(1, server.getRequestCount());
        response.close();
    }

    @Test
    public void testNoRetryOn4xx() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(404).setBody("not found"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(404, response.code());
        assertEquals(1, server.getRequestCount());
        response.close();
    }

    @Test
    public void testRetryOn503ThenSuccess() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(2, server.getRequestCount());
        response.close();
    }

    @Test
    public void testRetryOn500ThenSuccess() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(2, server.getRequestCount());
        response.close();
    }

    @Test
    public void testRetryOn429ThenSuccess() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(429));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(2, server.getRequestCount());
        response.close();
    }

    @Test
    public void testExhaustedRetriesReturnsLastResponse() throws IOException {
        client = buildClient(2, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(503));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(503, response.code());
        assertEquals(3, server.getRequestCount()); // initial + 2 retries
        response.close();
    }

    @Test
    public void testRetryDisabledWhenMaxRetriesZero() throws IOException {
        client = buildClient(0, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(503));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(503, response.code());
        assertEquals(1, server.getRequestCount());
        response.close();
    }

    @Test
    public void testMultipleRetriesBeforeSuccess() throws IOException {
        client = buildClient(5, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(502));
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(504));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(4, server.getRequestCount());
        response.close();
    }

    @Test
    public void testNoRetryOn401() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(401));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(401, response.code());
        assertEquals(1, server.getRequestCount());
        response.close();
    }

    @Test
    public void testNoRetryOn409() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(409));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(409, response.code());
        assertEquals(1, server.getRequestCount());
        response.close();
    }

    @Test
    public void testRetryOn408() throws IOException {
        client = buildClient(3, 100, 1000, 0.0);
        server.enqueue(new MockResponse().setResponseCode(408));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        Request request = new Request.Builder().url(server.url("/")).build();
        Response response = client.newCall(request).execute();

        assertEquals(200, response.code());
        assertEquals(2, server.getRequestCount());
        response.close();
    }
}
