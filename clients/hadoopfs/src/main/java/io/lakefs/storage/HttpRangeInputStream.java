package io.lakefs.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;

public class HttpRangeInputStream extends FSInputStream {
    private static final int BUFFER_SIZE = 3;
    private long rangeStart = Long.MAX_VALUE;
    private long pos;
    private InputStream in;
    private final String url;
    private long len = 0;
    private boolean closed;

    public HttpRangeInputStream(String url) throws IOException {
        this.url = url;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Range", "bytes=0-0");
        String contentRangeHeader = connection.getHeaderField("Content-Range");
        if (contentRangeHeader == null || !contentRangeHeader.startsWith("bytes 0-0/")) {
            // empty file
            return;
        }
        try {
            len = Long.parseLong(contentRangeHeader.substring("bytes 0-0/".length()));
        } catch (NumberFormatException e) {
            // empty file
        }
    }

    private void updateInputStream(long targetPos) throws MalformedURLException, IOException {
        if (targetPos >= rangeStart && targetPos < rangeStart + BUFFER_SIZE) {
            // no need to update the stream
            return;
        }
        if (in != null) {
            in.close();
        }
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        long rangeEnd = Math.min(targetPos + BUFFER_SIZE, len);
        connection.setRequestProperty("Range", "bytes=" + targetPos + "-" + rangeEnd);
        in = connection.getInputStream();
        rangeStart = targetPos;
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
        pos++;
        return in.read();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (in == null) {
            return;
        }
        in.close();
    }
}
