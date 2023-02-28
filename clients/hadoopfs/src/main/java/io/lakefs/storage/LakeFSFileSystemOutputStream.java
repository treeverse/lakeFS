package io.lakefs.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.fs.Path;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.clients.api.model.StagingMetadata;
import io.lakefs.utils.ObjectLocation;

public class LakeFSFileSystemOutputStream extends OutputStream {
    private final HttpURLConnection connection;
    private final ByteArrayOutputStream buffer;
    private final StagingLocation stagingLocation;
    private final ObjectLocation objectLoc;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final LakeFSFileSystem lfs;
    private final LakeFSClient lakeFSClient;

    public LakeFSFileSystemOutputStream(LakeFSFileSystem lfs, LakeFSClient lfsClient, HttpURLConnection connection, ObjectLocation objectLoc, StagingLocation stagingLocation) throws IOException {
        this.connection = connection;
        this.objectLoc = objectLoc;
        this.stagingLocation = stagingLocation;
        this.lfs = lfs;
        this.lakeFSClient = lfsClient;
        this.buffer = new ByteArrayOutputStream();
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
        String eTag = ObjectUtils.firstNonNull(
            connection.getHeaderField("Content-MD5"),
            connection.getHeaderField("ETag")
        );
        StagingApi staging = lakeFSClient.getStagingApi();
        StagingMetadata stagingMetadata = new StagingMetadata().checksum(eTag).sizeBytes(Long.valueOf(buffer.size())).staging(stagingLocation);
        try {
            staging.linkPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), stagingMetadata);
        } catch (ApiException e) {
            throw new IOException("link lakeFS path to physical address", e);
        }
        lfs.deleteEmptyDirectoryMarkers(new Path(objectLoc.toString()).getParent());
        
        String body = IOUtils.toString(connection.getInputStream());
        System.out.println(body);
        if (connection.getResponseCode() > 299) {
            throw new IOException("Failed to finish writing to presigned link. Response code: " + connection.getResponseCode());
        }
    }
}