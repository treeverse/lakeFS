package io.lakefs.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lakefs.LakeFSLinker;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

/**
 * Handle writes into a storage URL. Will set the request Content-Length header. When closed, links
 * the address in lakeFS.
 * <p>
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
        String eTag = extractETag();
        if (eTag == null) {
            throw new IOException("Failed to finish writing to presigned link. No ETag found.");
        }
        linker.link(eTag, buffer.size(), null);
        if (connection.getResponseCode() > 299) {
            throw new IOException("Failed to finish writing to presigned link. Response code: "
                    + connection.getResponseCode());
        }
    }

    /**
     * Extracts the ETag from the response headers. Use Content-MD5 if present, otherwise use ETag.
     * @return the ETag of the uploaded object, or null if not found
     */
    private String extractETag() {
        String contentMD5 = connection.getHeaderField("Content-MD5");
        if (contentMD5 != null) {
            //noinspection VulnerableCodeUsages
            byte[] dataMD5 = Base64.decodeBase64(contentMD5);
            return Hex.encodeHexString(dataMD5);
        }

        String eTag = connection.getHeaderField("ETag");
        if (eTag != null) {
            return StringUtils.strip(eTag, "\" ");
        }
        return null;
    }
}
