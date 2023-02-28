package io.lakefs;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * MetadataClient used to extract ObjectMetadata with content size and etag information from the underlying filesystem.
 */
public class MetadataClient {
    public static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
    private final FileSystem fs;

    public MetadataClient(FileSystem fs) {
        if (fs == null) {
            throw new java.lang.IllegalArgumentException();
        }
        this.fs = fs;
    }

    /**
     * Get object metadata by physical address. First it will try to extract the information from the FileSystem's FileStatus.
     * Fallback by extracting s3 client and call getObjectMetadata.
     * @param physicalUri physical uri of object
     * @return ObjectMetadata filled with Etag and content length
     * @throws IOException case etag can't be extracted by s3 or file status
     */
    public ObjectMetadata getObjectMetadata(URI physicalUri) throws IOException {
        String bucket = physicalUri.getHost();
        String key = physicalUri.getPath().substring(1);

        // use underlying filesystem to get the file status and extract
        // content length and etag (using reflection)
        Path physicalPath = new Path(physicalUri.getPath());
        FileStatus fileStatus = this.fs.getFileStatus(physicalPath);
        try {
            Method getETagMethod = fileStatus.getClass().getMethod("getETag");
            String etag = (String) getETagMethod.invoke(fileStatus);
            // return the specific properties over object metadata for easy fallback
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(fileStatus.getLen());
            objectMetadata.setHeader("ETag", etag);
            return objectMetadata;
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOG.debug("failed to get etag from file status", e);
        } catch (NoSuchMethodException ignored) {
        }

        // fallback - get the underlying s3 client and request object metadata
        try {
            Method amazonS3ClientGetter = fs.getClass().getDeclaredMethod("getAmazonS3Client");
            amazonS3ClientGetter.setAccessible(true);
            Object s3Client = amazonS3ClientGetter.invoke(fs);
            Method getObjectMetadataMethod = s3Client.getClass().getDeclaredMethod("getObjectMetadata", GetObjectMetadataRequest.class);
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucket, key);
            return (ObjectMetadata) getObjectMetadataMethod.invoke(s3Client, metadataRequest);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            LOG.debug("failed to get object metadata using underlying s3 client", e);
        }
        // fallback - get the underlying s3 client from the databricks wrapper and request object metadata
        try {
            Method fsGetter = fs.getClass().getDeclaredMethod("getWrappedFs");
            Object s3fs = fsGetter.invoke(fs);
            Method amazonS3ClientGetter = s3fs.getClass().getDeclaredMethod("getAmazonS3Client");
            amazonS3ClientGetter.setAccessible(true);
            Object s3Client = amazonS3ClientGetter.invoke(s3fs);
            Method getObjectMetadataMethod = s3Client.getClass().getDeclaredMethod("getObjectMetadata", GetObjectMetadataRequest.class);
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucket, key);
            return (ObjectMetadata) getObjectMetadataMethod.invoke(s3Client, metadataRequest);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            LOG.warn("failed to get object metadata using underlying wrapped s3 client", e);
            throw new IOException("get object metadata using underlying wrapped s3 client", e);
        }
    }
}
