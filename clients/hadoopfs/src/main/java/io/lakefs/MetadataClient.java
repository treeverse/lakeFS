package io.lakefs;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

public class MetadataClient {
    public static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
    private final FileSystem fs;
    private Method getMetadata;
    private Object s3client;

    public MetadataClient(FileSystem fs) {
        this.fs = fs;
        if (this.fs == null) {
            return;
        }
        try {
            Method amazonS3ClientGetter = fs.getClass().getDeclaredMethod("getAmazonS3Client");
            amazonS3ClientGetter.setAccessible(true);
            this.s3client = amazonS3ClientGetter.invoke(fs);
            this.getMetadata = s3client.getClass().getDeclaredMethod("getObjectMetadata", GetObjectMetadataRequest.class);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOG.warn("get underlying get object metadata method:", e);
        }
    }

    /**
     * get object metadata by physical address
     * @param physicalUri physical uri of object
     * @return ObjectMetadata filled with Etag and content length
     * @throws IOException
     */
    ObjectMetadata getObjectMetadata(URI physicalUri) throws IOException {
        String bucket = physicalUri.getHost();
        String key = physicalUri.getPath().substring(1);
        if (this.s3client != null && this.getMetadata != null) {
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucket, key);
            try {
                return (ObjectMetadata) getMetadata.invoke(this.s3client, metadataRequest);
            } catch (IllegalAccessException | InvocationTargetException e) {
                LOG.warn("failed to get object metadata using underlying s3 client", e);
            }
        }

        if (this.fs == null) {
            throw new IOException("no underlying file system");
        }

        // use underlying filesystem to get the file status and extract
        // content length and etag (using reflection)
        Path physicalPath = new Path(physicalUri.getPath());
        FileStatus fileStatus = this.fs.getFileStatus(physicalPath);
        try {
            String etag = getEtagFromFileStatus(fileStatus);
            ObjectMetadata o = new ObjectMetadata();
            o.setContentLength(fileStatus.getLen());
            o.setHeader("ETag", etag);
            return o;
        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException("etag from file status", e);
        }
    }

    private static String getEtagFromFileStatus(FileStatus fileStatus) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        for (PropertyDescriptor pd : Introspector.getBeanInfo(fileStatus.getClass()).getPropertyDescriptors()) {
            if (pd.getReadMethod() != null && pd.getName().equals("etag")) {
                String etag = (String) pd.getReadMethod().invoke(fileStatus);
                if (etag != null && !etag.isEmpty()) {
                    return etag;
                }
            }
        }
        return "";
    }

}
