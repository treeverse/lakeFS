package io.lakefs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.clients.api.model.StagingMetadata;

/**
 * Wraps a FSDataOutputStream to link file on staging when done writing
 */
class LinkOnCloseOutputStream extends OutputStream {
    private AmazonS3 s3Client;
    private StagingApi staging;
    private StagingLocation stagingLoc;
    private ObjectLocation objectLoc;
    private URI physicalUri;
    private OutputStream out;

    /**
     * @param s3Client client used to access data on S3.
     * @param staging client used to access metadata on lakeFS.
     * @param stagingLoc physical location of object data on S3.
     * @param objectLoc location of object on lakeFS.
     * @param physicalUri translated physical location of object data for underlying FileSystem.
     * @param out stream on underlying filesystem to wrap.
     */
    LinkOnCloseOutputStream(AmazonS3 s3Client, StagingApi staging, StagingLocation stagingLoc, ObjectLocation objectLoc, URI physicalUri, OutputStream out) {
	this.s3Client = s3Client;
	this.staging = staging;
	this.stagingLoc = stagingLoc;
	this.objectLoc = objectLoc;
	this.physicalUri = physicalUri;
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
	String bucket = physicalUri.getHost();
	String key = ObjectLocation.trimLeadingSlash(physicalUri.getPath());
	ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucket, key);

	// TODO(ariels): Can we add metadata here?
	StagingMetadata metadata = new StagingMetadata().staging(stagingLoc)
	    .checksum(objectMetadata.getETag())
	    .sizeBytes(objectMetadata.getContentLength());

	try {
	    staging.linkPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), metadata);
	} catch (io.lakefs.clients.api.ApiException e) {
	    throw new IOException("link lakeFS path to physical address", e);
	}
    }
}
