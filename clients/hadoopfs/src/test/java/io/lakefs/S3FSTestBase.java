package io.lakefs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;

import io.lakefs.clients.sdk.model.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

/**
 * Base for all LakeFSFilesystem tests that need to access S3.  It adds a
 * MinIO container to FSTestBase, and configures to use it.
 * The visibility of this class is public as it's being used by other libraries for testing purposes
 */
public abstract class S3FSTestBase extends FSTestBase {
    static private final Logger LOG = LoggerFactory.getLogger(S3FSTestBase.class);

    protected String s3Endpoint;
    protected AmazonS3 s3Client;

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-06-07T21-40-51Z");

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withEnv("MINIO_DOMAIN", "s3.local.lakefs.io").
        withEnv("MINIO_UPDATE", "off").
        withExposedPorts(9000);

    @Before
    public void logS3Container() {
        Logger s3Logger = LoggerFactory.getLogger("s3 container");
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(s3Logger)
            .withMdc("container", "s3")
            .withSeparateOutputStreams();
        s3.followOutput(logConsumer);
    }

    public void s3ClientSetup() {
        AWSCredentials creds = new BasicAWSCredentials(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withSignerOverride("AWSS3V4SignerType");
        s3Endpoint = String.format("http://s3.local.lakefs.io:%d", s3.getMappedPort(9000));

        s3Client = new AmazonS3Client(creds, clientConfiguration);

        S3ClientOptions s3ClientOptions = new S3ClientOptions()
            .withPathStyleAccess(true);
        s3Client.setS3ClientOptions(s3ClientOptions);
        s3Client.setEndpoint(s3Endpoint);

        s3Bucket = makeS3BucketName();
        s3Base = String.format("s3://%s/", s3Bucket);
        LOG.info("S3: bucket {} => base URL {}", s3Bucket, s3Base);

        CreateBucketRequest cbr = new CreateBucketRequest(s3Bucket);
        s3Client.createBucket(cbr);
    }

    /**
     * @return all pathnames under s3Prefix that start with prefix.  (Obvious not scalable!)
     */
    protected List<String> getS3FilesByPrefix(String prefix) {

        ListObjectsRequest req = new ListObjectsRequest()
            .withBucketName(s3Bucket)
            .withPrefix(prefix)
            .withDelimiter(null);

        ObjectListing listing = s3Client.listObjects(req);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (listing.isTruncated()) {
            Assert.fail(String.format("[internal] no support for test that creates >%d S3 objects", listing.getMaxKeys()));
        }

        return Lists.transform(summaries, S3ObjectSummary::getKey);
    }

    protected void assertS3Object(StagingLocation stagingLocation, String contents) {
        String s3Key = getS3Key(stagingLocation);
        List<String> actualFiles = ImmutableList.of("<not yet listed>");
        try (S3Object obj =
             s3Client.getObject(new GetObjectRequest(s3Bucket, "/" + s3Key))) {
            actualFiles = getS3FilesByPrefix("");
            String actual = IOUtils.toString(obj.getObjectContent());
            Assert.assertEquals(contents, actual);

            Assert.assertEquals(ImmutableList.of(s3Key), actualFiles);
        } catch (Exception e) {
            throw new RuntimeException("Files " + actualFiles +
                                       "; read key " + s3Key + " failed", e);
        }
    }

    protected void moreHadoopSetup() {
        s3ClientSetup();

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, S3_ACCESS_KEY_ID);
        conf.set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, S3_SECRET_ACCESS_KEY);
        conf.set(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, s3Endpoint);
        conf.set(org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR, "/tmp/s3a");
    }
}
