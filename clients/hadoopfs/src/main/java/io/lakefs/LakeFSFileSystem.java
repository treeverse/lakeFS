package io.lakefs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.auth.HttpBasicAuth;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.StagingLocation;

import javax.annotation.Nonnull;

/**
 * A dummy implementation of the core lakeFS Filesystem.
 * This class implements a {@link LakeFSFileSystem} that can be registered to Spark and support limited write and read actions.
 *
 * Configure Spark to use lakeFS filesystem by property:
 *   spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem.
 *
 * Configure the application or the filesystem application by properties:
 *   fs.lakefs.endpoint=http://localhost:8000/api/v1
 *   fs.lakefs.access.key=AKIAIOSFODNN7EXAMPLE
 *   fs.lakefs.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
 */
public class LakeFSFileSystem extends FileSystem {
    public static final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystem.class);
    public static final String SCHEME = "lakefs";
    public static final String FS_LAKEFS_ENDPOINT = "fs.lakefs.endpoint";
    public static final String FS_LAKEFS_ACCESS_KEY = "fs.lakefs.access.key";
    public static final String FS_LAKEFS_SECRET_KEY = "fs.lakefs.secret.key";

    private static final String BASIC_AUTH = "basic_auth";

    private Configuration conf;
    private URI uri;
    private Path workingDirectory = new Path(Constants.SEPARATOR);
    private ApiClient apiClient;
    private AmazonS3 s3Client;

    private URI translateUri(URI uri) throws java.net.URISyntaxException {
        switch (uri.getScheme()) {
        case "s3":
            return new URI("s3a", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(),
                           uri.getFragment());
        default:
            throw new RuntimeException(String.format("unsupported URI scheme %s", uri.getScheme()));
        }
    }

    public URI getUri() { return uri; }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.conf = conf;
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ initialize: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$", name);

        String host = name.getHost();
        if (host == null) {
            throw new IOException("Invalid repository specified");
        }
        setConf(conf);
        this.uri = name;

        // setup lakeFS api client
        String endpoint = conf.get(FS_LAKEFS_ENDPOINT, "http://localhost:8000/api/v1");
        String accessKey = conf.get(FS_LAKEFS_ACCESS_KEY);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }
        String secretKey = conf.get(FS_LAKEFS_SECRET_KEY);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }
        this.apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        this.apiClient.setBasePath(endpoint);
        HttpBasicAuth basicAuth = (HttpBasicAuth)this.apiClient.getAuthentication(BASIC_AUTH);
        basicAuth.setUsername(accessKey);
        basicAuth.setPassword(secretKey);

        s3Client = createS3ClientFromConf(conf);
    }

    /**
     * @return an Amazon S3 client configured much like S3A configure theirs.
     */
    static private AmazonS3 createS3ClientFromConf(Configuration conf) {
        String accessKey = conf.get(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, null);
        String secretKey = conf.get(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, null);
        AWSCredentialsProviderChain credentials = new AWSCredentialsProviderChain(
            new BasicAWSCredentialsProvider(accessKey, secretKey),
            new InstanceProfileCredentialsProvider(),
            new AnonymousAWSCredentialsProvider());

        ClientConfiguration awsConf = new ClientConfiguration();
        awsConf.setMaxConnections(conf.getInt(org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS,
                                              org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS));
        boolean secureConnections = conf.getBoolean(org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS,
                                                    org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS);
        awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
        awsConf.setMaxErrorRetry(conf.getInt(org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES,
          org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_ERROR_RETRIES));
        awsConf.setConnectionTimeout(conf.getInt(org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT,
            org.apache.hadoop.fs.s3a.Constants.DEFAULT_ESTABLISH_TIMEOUT));
        awsConf.setSocketTimeout(conf.getInt(org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT,
                        org.apache.hadoop.fs.s3a.Constants.DEFAULT_SOCKET_TIMEOUT));

        // TODO(ariels): Also copy proxy configuration?

        AmazonS3 s3 = new AmazonS3Client(credentials, awsConf);
        String endPoint = conf.getTrimmed(org.apache.hadoop.fs.s3a.Constants.ENDPOINT,"");
        if (!endPoint.isEmpty()) {
                try {
                        s3.setEndpoint(endPoint);
                } catch (IllegalArgumentException e) {
                        String msg = "Incorrect endpoint: " + e.getMessage();
                        LOG.error(msg);
                        throw new IllegalArgumentException(msg, e);
                }
        }
        return s3;
    }

    /**
     *{@inheritDoc}
     * Called on a file read Spark action. This method returns a FSDataInputStream with a static string,
     * regardless of the given file path.
     */
    @Override
    public FSDataInputStream open(Path path, int bufSize) throws IOException {
        try {
            LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ open(" + path.getName() + ") $$$$$$$$$$$$$$$$$$$$$$$$$$$$");

            ObjectsApi objects = new ObjectsApi(apiClient);
            ObjectLocation objectLoc = pathToObjectLocation(path);
            ObjectStats stats = objects.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            URI physicalUri = translateUri(new URI(stats.getPhysicalAddress()));

            Path physicalPath = new Path(physicalUri.toString());
            FileSystem physicalFs = physicalPath.getFileSystem(conf);
            return physicalFs.open(physicalPath, bufSize);
        } catch (io.lakefs.clients.api.ApiException e) {
            throw new RuntimeException("lakeFS API exception", e);
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *{@inheritDoc}
     * Called on a file write Spark/Hadoop action. This method writes the content of the file in path into stdout.
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
                                     int bufferSize, short unusedReplication, long unusedBlockSize,
                                     Progressable progress) throws IOException {
        try {
            LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ create path: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", path.toString());

            // BUG(ariels): overwrite ignored.

            StagingApi staging = new StagingApi(apiClient);
            ObjectLocation objectLoc = pathToObjectLocation(path);
            StagingLocation stagingLoc = staging.getPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            URI physicalUri = translateUri(new URI(stagingLoc.getPhysicalAddress()));

            Path physicalPath = new Path(physicalUri.toString());
            FileSystem physicalFs = physicalPath.getFileSystem(conf);

            // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
            return new FSDataOutputStream(new LinkOnCloseOutputStream(s3Client, staging, stagingLoc, objectLoc,
                                                                      physicalUri,
                                                                      // FSDataOutputStream is a kind of OutputStream(!)
                                                                      physicalFs.create(physicalPath, false, bufferSize, progress)),
                                          null);
        }  catch (io.lakefs.clients.api.ApiException e) {
            throw new RuntimeException("API exception: " + e.getResponseBody());
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("Append is not supported by LakeFSFileSystem");
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ rename $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return false;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Delete path {} - recursive {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$",
                path, recursive);

        ObjectLocation objectLoc = pathToObjectLocation(path);
        ObjectsApi objectsApi = new ObjectsApi(this.apiClient);
        try {
            objectsApi.deleteObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
        } catch (ApiException e) {
            // This condition mimics s3a behaviour in https://github.com/apache/hadoop/blob/7f93349ee74da5f35276b7535781714501ab2457/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L2741
            if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
                LOG.error("Could not delete: {}, reason: {}", path, e.getResponseBody());
                return false;
            }
            throw new IOException("deleteObject", e);
        }
        LOG.debug("Successfully deleted {}", path.toString());
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ List status is called for: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$", path.toString());
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDirectory = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDirectory;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ mkdirs, path: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", path.toString());
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ getFileStatus, path: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", path.toString());
        ObjectLocation objectLoc = pathToObjectLocation(path);
        if (objectLoc == null) {
            throw new FileNotFoundException(path.toString());
        }
        try {
            ObjectsApi objectsApi = new ObjectsApi(this.apiClient);
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            long length = 0;
            Long sizeBytes = objectStat.getSizeBytes();
            if (sizeBytes != null) {
                length = sizeBytes;
            }
            long modificationTime = 0;
            Long mtime = objectStat.getMtime();
            if (mtime != null) {
                modificationTime = TimeUnit.SECONDS.toMillis(mtime);
            }
            Path filePath = path.makeQualified(this.uri, this.workingDirectory);
            return new FileStatus(length, false, 0, 0, modificationTime, filePath);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                throw new FileNotFoundException(path + " not found");
            }
            throw new IOException("statObject", e);
        }
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return lakefs scheme
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ globStatus $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        FileStatus fStatus = new FileStatus(0, false, 1, 20, 1,
                new Path("tal-test"));
        FileStatus[] res = new FileStatus[1];
        res[0] = fStatus;
        return res;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return false;
    }

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     * @param path
     * @return lakeFS Location with repository, ref and path
     */
    @Nonnull
    public ObjectLocation pathToObjectLocation(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(this.workingDirectory, path);
        }

        URI uri = path.toUri();

        ObjectLocation loc = new ObjectLocation();
        loc.setRepository(uri.getHost());
        // extract ref and rest of the path after removing the '/' prefix
        String s = ObjectLocation.trimLeadingSlash(uri.getPath());
        int i = s.indexOf(Constants.SEPARATOR);
        if (i == -1) {
            loc.setRef(s);
        } else {
            loc.setRef(s.substring(0, i));
            loc.setPath(s.substring(i+1));
        }
        return loc;
    }
}
