package io.lakefs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.clients.api.model.StagingMetadata;
import io.lakefs.utils.ObjectLocation;

class LakeFSFileSystemOutputStream extends OutputStream {
    private HttpURLConnection connection;
    private ByteArrayOutputStream buffer;
    private StagingLocation stagingLocation;
    private ObjectLocation objectLoc;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private LakeFSFileSystem lfs;
    public LakeFSFileSystemOutputStream(LakeFSFileSystem lfs, HttpURLConnection connection, ObjectLocation objectLoc, StagingLocation stagingLocation) throws IOException {
        this.connection = connection;
        this.objectLoc = objectLoc;
        this.stagingLocation = stagingLocation;
        this.lfs = lfs;
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
        
        try {
            lfs.linkPhysicalAddress(objectLoc, new StagingMetadata().checksum(connection.getHeaderField("Content-MD5")).sizeBytes(Long.valueOf(buffer.size())).staging(stagingLocation));
        } catch (ApiException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        lfs.deleteEmptyDirectoryMarkers(new Path(objectLoc.toString()).getParent());
        
        String body = IOUtils.toString(connection.getInputStream());
        System.out.println(body);
        if (connection.getResponseCode() > 299) {
            throw new IOException("Failed to finish writing to presigned link. Response code: " + connection.getResponseCode());
        }
    }
}
