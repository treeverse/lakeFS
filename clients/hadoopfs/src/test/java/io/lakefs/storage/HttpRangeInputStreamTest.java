package io.lakefs.storage;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class HttpRangeInputStreamTest {

    final Dispatcher dispatcher = new Dispatcher() {
        Map<Integer, String> contentByLength = new HashMap<>();

        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            int contentLength = Integer.valueOf(StringUtils.substringAfterLast(request.getPath(), "/"));
            String content = contentByLength.get(contentLength);
            if (content == null) {
                content = RandomStringUtils.randomAlphanumeric(contentLength);
            }
            String[] range = StringUtils.substringAfter(request.getHeader("Range"), "bytes=").split("-");
            int start = Integer.valueOf(range[0]);
            int end = Integer.valueOf(range[1]);
            return new MockResponse()
                    .setHeader("Content-Range", String.format("bytes %d-%d/%d", start, end, contentLength))
                    .setResponseCode(200)
                    .setBody(content.substring(start, end));
        }
    };
    private MockWebServer server;

    @Before
    public void init() throws Exception {
        server = new MockWebServer();
        server.setDispatcher(dispatcher);
        server.start(1080);
    }

    @Test
    public void testReadBigBuffer() throws IOException {
        HttpUrl url = server.url("/100");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 1000);
        byte[] buffer = new byte[25];
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(25, stream.getPos());
        Assert.assertEquals(75, stream.available());
        buffer = new byte[75];

        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(100, stream.getPos());
        Assert.assertEquals(0, stream.available());

        System.out.println(new String(buffer));
        stream.close();
    }

    @Test
    public void testReadSmallBuffer() throws IOException {
        HttpUrl url = server.url("/100");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 7);
        byte[] buffer = new byte[25];
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(25, stream.getPos());
        Assert.assertEquals(75, stream.available());
        buffer = new byte[75];

        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(100, stream.getPos());
        Assert.assertEquals(0, stream.available());

        System.out.println(new String(buffer));
        stream.close();
    }

    @Test
    public void testEmptyFile() throws IOException {
        HttpUrl url = server.url("/0");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 1000);
        Assert.assertEquals(0, stream.getPos());
        Assert.assertEquals(0, stream.available());

        byte[] buffer = new byte[0];
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(0, stream.getPos());
        Assert.assertEquals(0, stream.available());
        stream.close();
    }

    @Test
    public void testSeek() throws IOException {
        HttpUrl url = server.url("/100");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 7);
        byte[] buffer = new byte[3];

        stream.seek(20);
        Assert.assertEquals(20, stream.getPos());
        Assert.assertEquals(80, stream.available());
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(23, stream.getPos());
        Assert.assertEquals(77, stream.available());

        stream.seek(60);
        Assert.assertEquals(60, stream.getPos());
        Assert.assertEquals(40, stream.available());
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(63, stream.getPos());
        Assert.assertEquals(37, stream.available());

        stream.seek(97);
        Assert.assertEquals(97, stream.getPos());
        Assert.assertEquals(3, stream.available());
        IOUtils.readFully(stream, buffer);
        Assert.assertEquals(100, stream.getPos());
        Assert.assertEquals(0, stream.available());

        Assert.assertEquals(4, server.getRequestCount());
        stream.close();
    }

    @Test
    public void testSeekAfterEnd() throws IOException {
        HttpUrl url = server.url("/100");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 7);
        stream.seek(101);
        Assert.assertEquals(-1, stream.read());
        stream.close();
    }

    @Test
    public void testSeekBeforeStart() throws IOException {
        HttpUrl url = server.url("/100");
        HttpRangeInputStream stream = new HttpRangeInputStream(url.toString(), 7);
        Exception exception = Assert.assertThrows(EOFException.class, () -> stream.seek(-1));
        Assert.assertTrue(String.format("Exception message should contain %s", FSExceptionMessages.NEGATIVE_SEEK),
                exception.getMessage().contains(FSExceptionMessages.NEGATIVE_SEEK));
        stream.close();
    }
}
