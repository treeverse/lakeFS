package io.lakefs;

import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.auth.HttpBasicAuth;
import io.lakefs.clients.api.model.ObjectStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectStats;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String SEPARATOR = "/";

    private Configuration conf;
    private URI uri;
    private Path workingDirectory = new Path(SEPARATOR);
    private ApiClient apiClient;

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
	    ObjectLocation objLoc = pathToObjectLocation(path);
	    ObjectStats stats = objects.statObject(objLoc.getRepository(), objLoc.getRef(), objLoc.getPath());
	    URI physicalUri = translateUri(new URI(stats.getPhysicalAddress()));

	    Path physicalPath = new Path(physicalUri.toString());
	    FileSystem physicalFs = physicalPath.getFileSystem(conf);
	    return physicalFs.open(physicalPath, bufSize);
	} catch (io.lakefs.clients.api.ApiException e) {
	    throw new RuntimeException("API exception: " + e.getResponseBody());
	} catch (java.net.URISyntaxException e) {
	    throw new RuntimeException(e);
	}
    }

    /**
     *{@inheritDoc}
     * Called on a file write Spark/Hadoop action. This method writes the content of the file in path into stdout.
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                     Progressable progressable) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ create path: {} $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", path.toString());
        return new FSDataOutputStream(System.out, null);
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
    public boolean delete(Path path, boolean b) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ delete $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return false;
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
        ObjectLocation loc = pathToObjectLocation(path);
        if (loc == null) {
            throw new FileNotFoundException(path.toString());
        }
        try {
            ObjectsApi objectsApi = new ObjectsApi(this.apiClient);
            ObjectStats objectStat = objectsApi.statObject(loc.getRepository(), loc.getRef(), loc.getPath());
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

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     * @param path
     * @return lakeFS Location with repository, ref and path
     */
    private ObjectLocation pathToObjectLocation(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(this.workingDirectory, path);
        }

        URI uri = path.toUri();
        if (uri.getScheme() != null && uri.getPath().isEmpty()) {
            return null;
        }

        ObjectLocation loc = new ObjectLocation();
        loc.setRepository(uri.getHost());
        // extract ref and rest of the path after removing the '/' prefix
        String s = uri.getPath();
        if (s.startsWith(SEPARATOR)) {
            s = s.substring(1);
        }
        int i = s.indexOf(SEPARATOR);
        if (i == -1) {
            loc.setRef(s);
        } else {
            loc.setRef(s.substring(0, i));
            loc.setPath(s.substring(i+1));
        }
        return loc;
    }

    private static class ObjectLocation {
        private String repository;
        private String ref;
        private String path;
        
        public String getRepository() {
            return repository;
        }

        public void setRepository(String repository) {
            this.repository = repository;
        }

        public String getRef() {
            return ref;
        }

        public void setRef(String ref) {
            this.ref = ref;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }
}
