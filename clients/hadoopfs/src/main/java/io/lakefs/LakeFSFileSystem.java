package io.lakefs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.lakefs.Constants.*;

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

    private Configuration conf;
    private URI uri;
    private Path workingDirectory = new Path(Constants.SEPARATOR);
    private LakeFSClient lfsClient;
    private AmazonS3 s3Client;
    private int listAmount;
    private FileSystem fsForConfig;

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


        lfsClient = new LakeFSClient(conf);
        s3Client = createS3ClientFromConf(conf);

        listAmount = conf.getInt(FS_LAKEFS_LIST_AMOUNT_KEY, DEFAULT_LIST_AMOUNT);

        Path path = new Path(name);

        // TODO(ariels): Retrieve base filesystem configuration for URI from new API.  Needed
        //     when this fs is constructed in order to create a new file, which cannot be Stat'ed
        try {
            ObjectsApi objects = lfsClient.getObjects();
            ObjectLocation objectLoc = pathToObjectLocation(path);
            ObjectStats stats = objects.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            URI physicalUri = translateUri(new URI(stats.getPhysicalAddress()));
            Path physicalPath = new Path(physicalUri.toString());
            fsForConfig = physicalPath.getFileSystem(conf);
        } catch (Exception e) {
            LOG.warn("get underlying filesystem for {}: {} (use default values)", path, e);
        }
    }

    @FunctionalInterface
    private interface BiFunctionWithIOException<U, V, R> {
        R apply(U u, V v) throws IOException;
    }

    /**
     * @return FileSystem suitable for the translated physical address
     */
    protected<R> R withFileSystemAndTranslatedPhysicalPath(String physicalAddress, BiFunctionWithIOException<FileSystem, Path, R> f) throws java.net.URISyntaxException, IOException {
        URI uri = translateUri(new URI(physicalAddress));
        Path path = new Path(uri.toString());
        FileSystem fs = path.getFileSystem(conf);
        return f.apply(fs, path);
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
        awsConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
        awsConf.setMaxErrorRetry(conf.getInt(org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES,
                org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_ERROR_RETRIES));
        awsConf.setConnectionTimeout(conf.getInt(org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT,
                org.apache.hadoop.fs.s3a.Constants.DEFAULT_ESTABLISH_TIMEOUT));
        awsConf.setSocketTimeout(conf.getInt(org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT,
                org.apache.hadoop.fs.s3a.Constants.DEFAULT_SOCKET_TIMEOUT));

        // TODO(ariels): Also copy proxy configuration?

        AmazonS3 s3 = new AmazonS3Client(credentials, awsConf);
        String endPoint = conf.getTrimmed(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, "");
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

    @Override
    public long getDefaultBlockSize(Path path) {
        if (fsForConfig != null) {
            return fsForConfig.getDefaultBlockSize(path);
        }
        return Constants.DEFAULT_BLOCK_SIZE;
    }

    @Override
    public long getDefaultBlockSize() {
        if (fsForConfig != null) {
            return fsForConfig.getDefaultBlockSize();
        }
        return Constants.DEFAULT_BLOCK_SIZE;
    }

    @Override
    public FSDataInputStream open(Path path, int bufSize) throws IOException {
        try {
            LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ open(" + path.getName() + ") $$$$$$$$$$$$$$$$$$$$$$$$$$$$");

            ObjectsApi objects = lfsClient.getObjects();
            ObjectLocation objectLoc = pathToObjectLocation(path);
            ObjectStats stats = objects.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            return withFileSystemAndTranslatedPhysicalPath(stats.getPhysicalAddress(), (FileSystem fs, Path p) -> fs.open(p, bufSize));
        } catch (ApiException e) {
            throw new IOException("open: " + path, e);
        } catch (java.net.URISyntaxException e) {
            throw new IOException("open physical", e);
        }
    }


    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ listFiles path: {}, recursive {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", f.toString(), recursive);

        return new ListingIterator(f, recursive, listAmount);
    }

    /**
     *{@inheritDoc}
     * Called on a file write Spark/Hadoop action. This method writes the content of the file in path into stdout.
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        try {
            LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ create path: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", path.toString());

            // BUG(ariels): overwrite ignored.

            StagingApi staging = lfsClient.getStaging();
            ObjectLocation objectLoc = pathToObjectLocation(path);
            StagingLocation stagingLoc = staging.getPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            URI physicalUri = translateUri(new URI(stagingLoc.getPhysicalAddress()));

            Path physicalPath = new Path(physicalUri.toString());
            FileSystem physicalFs = physicalPath.getFileSystem(conf);

            // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
            return new FSDataOutputStream(new LinkOnCloseOutputStream(s3Client, staging, stagingLoc, objectLoc,
                                                                      physicalUri,
                                                                      // FSDataOutputStream is a kind of OutputStream(!)
                                                                      physicalFs.create(physicalPath, false, bufferSize, replication, blockSize, progress)),
                                          null);
        }  catch (io.lakefs.clients.api.ApiException e) {
            throw new IOException("staging.getPhysicalAddress: " + e.getResponseBody(), e);
        } catch (java.net.URISyntaxException e) {
            throw new IOException("underlying storage uri", e);
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("Append is not supported by LakeFSFileSystem");
    }

    /**
     * This method is implemented under the following assumptions:
     * 1. rename is only supported for uncommitted data on the same branch.
     * 2. file rename operation is supported, directories rename is unsupported.
     * 3. the rename dst  path can be an uncommitted file, that will be overridden as a result of the rename operation.
     * 4. On rename operation a new mtime is generated, therefore we don't preserve the mtime of the src object.
     *
     * @throws IOException
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Rename path {} to {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$", src, dst);

        ObjectLocation srcObjectLoc = pathToObjectLocation(src);
        ObjectLocation dstObjectLoc = pathToObjectLocation(dst);
        if (srcObjectLoc.equals(dstObjectLoc)) {
            LOG.debug("rename: src and dst refer to the same lakefs object location: {}", dst);
            return true;
        }

        if (!srcObjectLoc.onSameBranch(dstObjectLoc)) {
            LOG.error("rename: src {} and dst {} are not on the same branch. rename outside this scope is unsupported "
                    + "by lakefs.", src, dst);
            return false;
        }

        ObjectStats srcStat;
        ObjectsApi objects = lfsClient.getObjects();
        try {
            // Stat src file to get its metadata
            srcStat = objects.statObject(srcObjectLoc.getRepository(), srcObjectLoc.getRef(),
                    srcObjectLoc.getPath());
        } catch (ApiException e) {
            LOG.error("rename: could not get src object stats. src:{}", src, e);
            return false;
        }

        return renameObject(srcStat, srcObjectLoc, dstObjectLoc);
    }

    /**
     * Non-atomic rename operation.
     * @return true if rename succeeded, false otherwise
     */
    private boolean renameObject(ObjectStats srcStat, ObjectLocation srcObjectLoc, ObjectLocation dstObjectLoc)
            throws IOException {
        ObjectsApi objects = lfsClient.getObjects();

        //TODO (Tals): Can we add metadata? we currently don't have an API to get the metadata of an object.
        ObjectStageCreation creationReq = new ObjectStageCreation()
                .checksum(srcStat.getChecksum())
                .sizeBytes(srcStat.getSizeBytes())
                .physicalAddress(srcStat.getPhysicalAddress());

        try {
            objects.stageObject(dstObjectLoc.getRepository(), dstObjectLoc.getRef(), dstObjectLoc.getPath(),
                    creationReq);
        } catch (ApiException e) {
            LOG.error("rename: Could not stage object on dst:{}", dstObjectLoc.getPath(), e);
            return false;
        }

        // delete src path
        try {
            objects.deleteObject(srcObjectLoc.getRepository(), srcObjectLoc.getRef(), srcObjectLoc.getPath());
        } catch (ApiException e) {
            // This condition mimics s3a behaviour in https://github.com/apache/hadoop/blob/2960d83c255a00a549f8809882cd3b73a6266b6d/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L2741
            if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
                LOG.error("Could not delete: {}, reason: {}", srcObjectLoc.getPath(), e.getResponseBody());
                return false;
            }
            throw new IOException("deleteObject", e);
        }

        LOG.debug("rename: successfully renamed {} to {}", srcObjectLoc.getPath(), dstObjectLoc.getPath());
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Delete path {} - recursive {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$",
                path, recursive);
        if (recursive) {
            ListingIterator iterator = new ListingIterator(path, true, listAmount);
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                deleteHelper(fileStatus.getPath());
            }
        } else {
            if (!deleteHelper(path)) {
                return false;
            }
        }
        LOG.debug("Successfully deleted {}", path);
        return true;
    }

    private boolean deleteHelper(Path path) throws IOException {
        try {
            ObjectsApi objectsApi = lfsClient.getObjects();
            ObjectLocation objectLoc = pathToObjectLocation(path);
            objectsApi.deleteObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
        } catch (ApiException e) {
            // This condition mimics s3a behaviour in https://github.com/apache/hadoop/blob/7f93349ee74da5f35276b7535781714501ab2457/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L2741
            if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
                LOG.error("Could not delete: {}, reason: {}", path, e.getResponseBody());
                return false;
            }
            throw new IOException("deleteObject", e);
        }
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
        ObjectsApi objectsApi = lfsClient.getObjects();
        try {
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            return convertObjectStatsToFileStatus(objectLoc.getRepository(), objectLoc.getRef(), objectStat);
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        // not found as a file; check if path is a "directory", i.e. a prefix.
        ListingIterator iterator = new ListingIterator(path, true, 1);
        if (iterator.hasNext()) {
            Path filePath = new Path(objectLoc.toString());
            return new FileStatus(0, true, 0, 0, 0, filePath);
        }
        throw new FileNotFoundException(path + " not found");
    }

    @Nonnull
    private FileStatus convertObjectStatsToFileStatus(String repository, String ref, ObjectStats objectStat) throws IOException {
        try {
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
            Path filePath = new Path(ObjectLocation.formatPath(repository, ref, objectStat.getPath()));
            boolean isDir = objectStat.getPathType() == ObjectStats.PathTypeEnum.COMMON_PREFIX;
            long blockSize = 0;
            if (!isDir) {
                blockSize = withFileSystemAndTranslatedPhysicalPath(objectStat.getPhysicalAddress(), FileSystem::getDefaultBlockSize);
            }
            return new FileStatus(length, isDir, 0, blockSize, modificationTime, filePath);
        } catch (java.net.URISyntaxException e) {
            throw new IOException("uri", e);
        }
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return lakefs scheme
     */
    @Override
    public String getScheme() {
        return Constants.URI_SCHEME;
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
    public boolean exists(Path path) throws IOException {
        ObjectsApi objects = lfsClient.getObjects();
        ObjectLocation objectLoc = pathToObjectLocation(path);
        try {
            objects.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
            return true;
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        // use listing to check if directory exists
        ListingIterator iterator = new ListingIterator(path, true, 1);
        return iterator.hasNext();
    }

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     * @param path to extract information from.
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
            loc.setPath("");
        } else {
            loc.setRef(s.substring(0, i));
            loc.setPath(s.substring(i+1));
        }
        return loc;
    }

    class ListingIterator implements RemoteIterator<LocatedFileStatus> {
        private final ObjectLocation objectLocation;
        private final String delimiter;
        private final int amount;
        private String nextOffset;
        private boolean last;
        private List<ObjectStats> chunk;
        private int pos;

        /**
         * Returns iterator for files under path.
         * When recursive is set, the iterator will list all files under the target path (delimiter is ignored).
         * Parameter amount controls the limit for each request for listing.
         *
         * @param path the location to list
         * @param recursive boolean for recursive listing
         * @param amount buffer size to fetch listing
         */
        public ListingIterator(Path path, boolean recursive, int amount) {
            this.chunk = Collections.emptyList();
            this.objectLocation = pathToObjectLocation(path);
            String locationPath = this.objectLocation.getPath();
            // we assume that 'path' is a directory by default
            if (!locationPath.isEmpty() && !locationPath.endsWith(SEPARATOR)) {
                this.objectLocation.setPath(locationPath + SEPARATOR);
            }
            this.delimiter = recursive ? "" : SEPARATOR;
            this.last = false;
            this.pos = 0;
            this.amount = amount == 0 ? DEFAULT_LIST_AMOUNT : amount;
            this.nextOffset = "";
        }

        @Override
        public boolean hasNext() throws IOException {
            // read next chunk if needed
            if (!this.last && this.pos >= this.chunk.size()) {
                this.readNextChunk();
            }
            // return if there is next item available
            return this.pos < this.chunk.size();
        }

        private void readNextChunk() throws IOException {
            do {
                try {
                    ObjectsApi objectsApi = lfsClient.getObjects();
                    ObjectStatsList resp = objectsApi.listObjects(objectLocation.getRepository(), objectLocation.getRef(), objectLocation.getPath(), nextOffset, amount, delimiter);
                    chunk = resp.getResults();
                    pos = 0;
                    Pagination pagination = resp.getPagination();
                    if (pagination != null) {
                        nextOffset = pagination.getNextOffset();
                        if (!pagination.getHasMore()) {
                            last = true;
                        }
                    } else if (chunk.isEmpty()) {
                        last = true;
                    }
                } catch (ApiException e) {
                    throw new IOException("listObjects", e);
                }
                // filter objects
                chunk = chunk.stream().filter(stat -> stat.getPathType() != ObjectStats.PathTypeEnum.COMMON_PREFIX).collect(Collectors.toList());
                // loop until we have something or last chunk
            } while (!chunk.isEmpty() && !last);
        }

        @Override
        public LocatedFileStatus next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries");
            }
            ObjectStats objectStats = chunk.get(pos++);
            FileStatus fileStatus = convertObjectStatsToFileStatus(objectLocation.getRepository(), objectLocation.getRef(), objectStats);
            // currently do not pass locations of the file blocks - until we understand if it is required in order to work
            return new LocatedFileStatus(fileStatus, null);
        }
    }
}
