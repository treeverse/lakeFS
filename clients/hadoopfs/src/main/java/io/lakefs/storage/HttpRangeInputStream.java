package io.lakefs.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;

public class HttpRangeInputStream extends FSInputStream {
    private static final int DEFAULT_BUFFER_SIZE_BYTES = 1024 * 1024;
    private final String url;
    private final int bufferSize;

    private long start = Long.MAX_VALUE;
    private long pos;
    private long len = 0;
    private byte[] rangeContent;

    private boolean closed;
    
    public HttpRangeInputStream(String url) throws IOException {
        this(url, DEFAULT_BUFFER_SIZE_BYTES);
    }

    public HttpRangeInputStream(String url, int bufferSize) throws IOException {
        this.url = url;
        this.bufferSize = bufferSize;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Range", "bytes=0-0");
        String contentRangeHeader = connection.getHeaderField("Content-Range");
        if (contentRangeHeader == null || !contentRangeHeader.startsWith("bytes 0-0/")) {
            // empty file
            return;
        }
        len = Long.parseLong(contentRangeHeader.substring("bytes 0-0/".length()));
    }

    private void updateInputStream(long targetPos) throws MalformedURLException, IOException {
        if (targetPos >= start && targetPos < start + bufferSize) {
            // no need to update the stream
            return;
        }
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        long rangeEnd = Math.min(targetPos + bufferSize, len);
        connection.setRequestProperty("Range", "bytes=" + targetPos + "-" + rangeEnd);
        rangeContent = new byte[(int) (rangeEnd - targetPos)];
        try (InputStream inputStream = connection.getInputStream()) {
            IOUtils.readFully(inputStream, rangeContent);
        }
        start = targetPos;
    }

    @Override
    public synchronized void seek(long targetPos) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (targetPos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
                    + " " + targetPos);
        }
        if (targetPos >= len) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF
                    + " " + targetPos + " > " + len);
        }
        this.pos = targetPos;
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized int available() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (len - pos > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.max(len - pos, 0);
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (pos >= len) {
            return -1;
        }
        updateInputStream(pos);
        int res = rangeContent[(int) (pos - start)] & 0xff;
        pos++;
        return res;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
    }
}
