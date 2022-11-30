package io.lakefs;

import io.lakefs.clients.api.*;
import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.utils.ObjectLocation;
import io.lakefs.LakeFSClient;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.HttpStatus;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LakeFSOutputCommitterTest {
    Logger logger = LoggerFactory.getLogger("LakeFSOutputCommitterTest");

    protected AmazonS3 s3Client;

    protected String s3Base;
    protected String s3Bucket;

    protected SparkSession spark;

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-06-07T21-40-51Z");
    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withEnv("MINIO_DOMAIN", "s3.local.lakefs.io").
        withEnv("MINIO_UPDATE", "off").
        withExposedPorts(9000);


    @After
    public void closeSparkSession(){
        spark.close();
    }


    @Before
    public void setUp() throws Exception {
        AWSCredentials creds = new BasicAWSCredentials(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withSignerOverride("AWSS3V4SignerType");
        String s3Endpoint = String.format("http://s3.local.lakefs.io:%d", s3.getMappedPort(9000));

        s3Client = new AmazonS3Client(creds, clientConfiguration);

        S3ClientOptions s3ClientOptions = new S3ClientOptions()
            .withPathStyleAccess(true);
        s3Client.setS3ClientOptions(s3ClientOptions);
        s3Client.setEndpoint(s3Endpoint);

        s3Bucket = "example";
        s3Base = String.format("s3://%s", s3Bucket);
        CreateBucketRequest cbr = new CreateBucketRequest(s3Bucket);
        s3Client.createBucket(cbr);

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Spark test")
                .set("spark.sql.shuffle.partitions", "17")
                .set("spark.hadoop.mapred.output.committer.class","io.lakefs.committer.LakeFSOutputCommitter")
                .set("spark.hadoop.fs.lakefs.impl","io.lakefs.LakeFSFileSystem")
                .set("fs.lakefs.access.key", "AKIAIOSFODNN7EXAMPLE")
                .set("fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .set("fs.lakefs.endpoint", "http://localhost:8000/api/v1")
                .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, S3_ACCESS_KEY_ID)
                .set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, S3_SECRET_ACCESS_KEY)
                .set(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, s3Endpoint);
        this.spark = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    @Ignore
    @Test
    public void testWriteTextfile() throws ApiException, IOException {
        String output = "lakefs://example/main/some_df";

        StructType structType = new StructType().add("A", DataTypes.StringType, false);

        List<Row> nums = ImmutableList.of(
                RowFactory.create("value1"),
                RowFactory.create("value2"));
        Dataset<Row> df = spark.createDataFrame(nums, structType);

        ArrayList<String> failed = new ArrayList<String>();

        // TODO(Lynn): Add relevant file formats
        List<String> fileFormats = Arrays.asList("text");
        for (String fileFormat : fileFormats) {
            try {
                // save data in selected format
                String outputPath = String.format("%s.%s", output, fileFormat);
                logger.info(String.format("writing file in format %s to path %s", fileFormat, outputPath));
                df.write().format(fileFormat).save(outputPath);

                // read the data we just wrote
                logger.info(String.format("reading file in format %s", fileFormat));
                Dataset<Row> df_read = spark.read().format(fileFormat).load(outputPath);
                Dataset<Row> diff = df.exceptAll(df_read);
                Assert.assertTrue("all rows written were read", diff.isEmpty());
                diff = df_read.exceptAll(df);
                Assert.assertTrue("all rows read were written", diff.isEmpty());

            } catch (Exception e) {
                logger.error(String.format("Format %s unexpected exception: %s", fileFormat, e));
                failed.add(fileFormat);
            }
        }

        Assert.assertEquals("Failed list is not empty: %s" , 0, failed.size());
    }
}
