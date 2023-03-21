package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.Path;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.clients.api.model.StagingMetadata;
import io.lakefs.utils.ObjectLocation;

/**
 * Wraps a FSDataOutputStream to link file on staging when done writing
 */
class LinkOnCloseOutputStream extends OutputStream {
    private final LakeFSFileSystem lakeFSFileSystem;
    private final LakeFSClient lakeFSClient;
    private final StagingLocation stagingLoc;
    private final ObjectLocation objectLoc;
    private final URI physicalUri;
    private final MetadataClient metadataClient;
    private final OutputStream out;
    private final AtomicBoolean isLinked = new AtomicBoolean(false);

    /**
     * @param lfs LakeFS file system
     * @param stagingLoc physical location of object data on S3.
     * @param objectLoc location of object on lakeFS.
     * @param physicalUri translated physical location of object data for underlying FileSystem.
     * @param metadataClient client used to request metadata information from the underlying FileSystem.
     * @param out stream on underlying filesystem to wrap.
     */
    LinkOnCloseOutputStream(LakeFSFileSystem lfs, LakeFSClient lfsClient, StagingLocation stagingLoc, ObjectLocation objectLoc,
                            URI physicalUri, MetadataClient metadataClient, OutputStream out) {
        this.lakeFSFileSystem = lfs;
        this.lakeFSClient = lfsClient;
        this.stagingLoc = stagingLoc;
        this.objectLoc = objectLoc;
        this.physicalUri = physicalUri;
        this.metadataClient = metadataClient;
        this.out = out;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void close() throws IOException {
        out.close();

        // Now the object is on the underlying store, find its parameters (sadly lost by
        // the underlying Hadoop FileSystem) so we can link it on lakeFS.
        if (!this.isLinked.getAndSet(true)) {
            try {
                ObjectMetadata objectMetadata = metadataClient.getObjectMetadata(physicalUri);
                StagingMetadata metadata = new StagingMetadata()
                        .staging(stagingLoc)
                        .checksum(objectMetadata.getETag())
                        .sizeBytes(objectMetadata.getContentLength());
                StagingApi staging = lakeFSClient.getStagingApi();
                staging.linkPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), metadata);
            } catch (io.lakefs.clients.api.ApiException e) {
                throw new IOException("link lakeFS path to physical address", e);
            }
            lakeFSFileSystem.deleteEmptyDirectoryMarkers(new Path(objectLoc.toString()).getParent());
        }
    }
}
