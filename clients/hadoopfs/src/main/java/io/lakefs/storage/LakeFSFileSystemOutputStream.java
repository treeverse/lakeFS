package io.lakefs.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.ObjectUtils;
import io.lakefs.LakeFSLinker;

/**
 * Handle writes into a storage URL. Will set the request Content-Length header. When closed, links
 * the address in lakeFS.
 * 
 * TODO(johnnyaug): do not hold everything in memory
 * TODO(johnnyaug): support multipart uploads
 */
public class LakeFSFileSystemOutputStream extends OutputStream {
    private final HttpURLConnection connection;
    private final ByteArrayOutputStream buffer;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final LakeFSLinker linker;

    public LakeFSFileSystemOutputStream(HttpURLConnection connection, LakeFSLinker linker)
            throws IOException {
        this.connection = connection;
        this.buffer = new ByteArrayOutputStream();
        this.linker = linker;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.write(b);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.getAndSet(true)) {
            return;
        }
        connection.setRequestProperty("Content-Length", String.valueOf(buffer.size()));
        connection.setRequestProperty("x-ms-blob-type", "BlockBlob");
        OutputStream out = connection.getOutputStream();
        out.write(buffer.toByteArray());
        out.close();
        String eTag = ObjectUtils.firstNonNull(connection.getHeaderField("Content-MD5"),
                connection.getHeaderField("ETag"));
        linker.link(eTag, Long.valueOf(buffer.size()));
        if (connection.getResponseCode() > 299) {
            throw new IOException("Failed to finish writing to presigned link. Response code: "
                    + connection.getResponseCode());
        }
    }
}
