package io.lakefs;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.clients.api.model.StagingMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * Wraps a FSDataOutputStream to link file on staging when done writing
 */
class LinkOnCloseOutputStream extends OutputStream {
    private StagingApi staging;
    private StagingLocation stagingLoc;
    private ObjectLocation objectLoc;
    private URI physicalUri;
    private final FileSystem physicalFs;
    private OutputStream out;

    /**
     * @param staging client used to access metadata on lakeFS.
     * @param stagingLoc physical location of object data on S3.
     * @param objectLoc location of object on lakeFS.
     * @param physicalUri translated physical location of object data for underlying FileSystem.
     * @param physicalFs underlying filesystem used to query status for checksum.
     * @param out stream on underlying filesystem to wrap.
     */
    LinkOnCloseOutputStream(StagingApi staging, StagingLocation stagingLoc, ObjectLocation objectLoc, URI physicalUri, FileSystem physicalFs, OutputStream out) {
        this.staging = staging;
        this.stagingLoc = stagingLoc;
        this.objectLoc = objectLoc;
        this.physicalUri = physicalUri;
        this.physicalFs = physicalFs;
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
        Path physicalPath = new Path(physicalUri.getPath());
        FileStatus fileStatus = physicalFs.getFileStatus(physicalPath);
        S3AFileSystem fs2;
        long sizeBytes = fileStatus.getLen();

        String etag = getEtagFromFileStatus(fileStatus);
        if (etag.isEmpty()) {
            // Missing etag - try to use the underlying s3 client and get the Etag
            try {
                String bucket = physicalUri.getHost();
                String key = physicalUri.getPath().substring(1);
                Method amazonS3ClientGetter = physicalFs.getClass().getDeclaredMethod("getAmazonS3Client");
                amazonS3ClientGetter.setAccessible(true);
                AmazonS3Client s3 = (AmazonS3Client)amazonS3ClientGetter.invoke(physicalFs);
                ObjectMetadata objectMetadata = s3.getObjectMetadata(bucket, key);
                etag = objectMetadata.getETag();
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IOException("failed to get etag with underlying s3 client", e);
            }
        }

        // TODO(ariels): Can we add metadata here?
        StagingMetadata metadata = new StagingMetadata()
                .staging(stagingLoc)
                .checksum(etag)
                .sizeBytes(sizeBytes);
        try {
            staging.linkPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), metadata);
        } catch (io.lakefs.clients.api.ApiException e) {
            throw new IOException("link lakeFS path to physical address", e);
        }
    }

    private String getEtagFromFileStatus(FileStatus fileStatus) throws IOException {
        try {
            for (PropertyDescriptor pd : Introspector.getBeanInfo(fileStatus.getClass()).getPropertyDescriptors()) {
                if (pd.getReadMethod() != null && pd.getName().equals("etag")) {
                    String etag = (String) pd.getReadMethod().invoke(fileStatus);
                    if (etag != null && !etag.isEmpty()) {
                        return etag;
                    }
                }
            }
        } catch (IntrospectionException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException("failed to extract etag from FileStatus", e);
        }
        return "";
    }
}
