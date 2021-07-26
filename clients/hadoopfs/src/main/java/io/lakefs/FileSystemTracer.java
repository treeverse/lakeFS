package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * This class implements a troubleshooting tool for the LakeFSFileSystem. You can use it to compare the output of file
 * system operations done by {@link LakeFSFileSystem} and by {@link S3AFileSystem}.
 *
 * How to use the FileSystemTracer:
 * To use the FileSystemTracer, you should set the value of the Hadoop configuration 'fs.lakefs.impl' to FileSystemTracer.
 * This configures the FileSystemTracer to be the file system that handles paths with the lakefs scheme. i.e. paths with
 * lakefs:// prefix.
 *
 * What does the FileSystemTracer do:
 * The FileSystemTracer holds instances of {@link LakeFSFileSystem} and {@link S3AFileSystem}. On a file system operation,
 * the FileSystemTracer invokes the operation on both file systems, logs the output of both calls, and returns the result
 * of one of the file systems according to a configuration (S3AFileSystem output by default).

 * Additional configurations:
 * Mandatory -
 * - fs.lakefs.tracer.working.dir - the s3 location in which the tracer can operate. This should be an S3 bucket name or
 *   an absolute path of a directory on a bucket.
 * Optional -
 * - fs.lakefs.tracer.use.lakefs - tells the tracer whether it should return the response coming from the lakefs file
 *   system or return s3a's response. by default it is set to false and returns s3a's response.
 *
 * Assumptions:
 * - listing results of lakefs://repository/branch/$PATH and s3a://${fs.lakefs.tracer.working.dir}/$PATH are identical.
 * - The s3 credentials available for Spark allow access to fs.lakefs.tracer.working.dir on s3.
 */
public class FileSystemTracer extends FileSystem {

    public static final Logger LOG = LoggerFactory.getLogger(FileSystemTracer.class);
    private static final String TRACER_WORKING_DIR = "fs.lakefs.tracer.working.dir";
    private static final String USE_LAKEFS_RESPONSE = "fs.lakefs.tracer.use.lakefs";
    private static final Object S3_URI_PREFIX = "s3";
    private static final Object RESULTS_COMPARISON = "[RESULTS_COMPARISON]";


    /*
     A property that determines which file system's response the FileSystemTracer returns.
     */
    private boolean useLakeFSFileSystemRes;
    private LakeFSFileSystem lfsFileSystem;
    private FileSystem s3AFileSystem;
    private String s3aPathPrefix;
    private String lfsPathPrefix;

    /**
     * Transforms a lakefs path into an s3 path.
     * Example:
     *   in: lakefs://repository/branch/key=1/a.parquet
     *   out: s3://${fs.lakefs.tracer.working.dir}/key=1/a.parquet
     */
    private Path translateLakeFSPathToS3APath(Path path) {
        ObjectLocation objectLoc = lfsFileSystem.pathToObjectLocation(path);
        String lakefsPath = path.toString();
        String lakefsPrefix = String.format("%s://%s/%s", objectLoc.getScheme(), objectLoc.getRepository(),
                objectLoc.getRef());

        String s3aPath = lakefsPath.replace(lakefsPrefix, s3aPathPrefix);
        LOG.trace("Converted {} to {}", path, s3aPath);

        return new Path(s3aPath);
    }

    /**
     * Transforms an S3A path into a lakefs path.
     * This method is used to transform the output of S3AFileSystem operations that include paths, so that Spark
     * continues to direct it's requests to the FileSystemTracer that handles paths with the "lakefs://" prefix.
     * Example:
     *   in: s3://${fs.lakefs.tracer.working.dir}/key=1/a.parquet
     *   out: lakefs://repository/branch/key=1/a.parquet
     */
    private Path translateS3APathToLakeFSPath(Path path) {
        String p = path.toString();
        String lfsPath = p.replace(s3aPathPrefix, lfsPathPrefix);
        LOG.trace("Converted {} to {}", path, lfsPath);
        return new Path(lfsPath);
    }


    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        lfsFileSystem = new LakeFSFileSystem();
        lfsFileSystem.initialize(name, conf);
        ObjectLocation loc = lfsFileSystem.pathToObjectLocation(new Path(name));
        lfsPathPrefix = String.format("%s://%s/%s", loc.getScheme(), loc.getRepository(), loc.getRef());

        String tracerWorkingDir = conf.get(TRACER_WORKING_DIR);
        s3aPathPrefix = String.format("%s://%s", S3_URI_PREFIX, tracerWorkingDir);
        Path s3aPath = new Path(s3aPathPrefix);
        s3AFileSystem = s3aPath.getFileSystem(conf);

        useLakeFSFileSystemRes = conf.getBoolean(USE_LAKEFS_RESPONSE, false);
        LOG.trace("Initialized FileSystemTracer, fs.lakefs.tracer.use.lakefs: {}", useLakeFSFileSystemRes);
    }

    @Override
    public URI getUri() {
        LOG.trace("getUri");

        URI lakefsRes = lfsFileSystem.getUri();
        URI s3aRes = s3AFileSystem.getUri();
        LOG.trace("{}[getUri] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public Path makeQualified(Path path) {
        LOG.trace("makeQualified");

        Path lakefsRes = lfsFileSystem.makeQualified(path);
        Path s3aRes = s3AFileSystem.makeQualified(translateLakeFSPathToS3APath(path));
        LOG.trace("{}[makeQualified] lakefs: {}, s3a: {}",RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return translateS3APathToLakeFSPath(s3aRes);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        LOG.trace("open(Path {}, bufferSize {})", f, bufferSize);

        FSDataInputStream lakefsRes = lfsFileSystem.open(f, bufferSize);
        FSDataInputStream s3aRes = s3AFileSystem.open(translateLakeFSPathToS3APath(f), bufferSize);
        LOG.trace("{}[open]: lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        LOG.trace("create(Path {}, permission {}, overwrite {}, bufferSize {}, replication {}, blockSize {}, progress {})",
                f, permission, overwrite, bufferSize, replication, blockSize, progress);

        TracerOutputTStream tOutputStream = new TracerOutputTStream(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        return new FSDataOutputStream(tOutputStream, null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        LOG.trace("append(f {}, bufferSize {}, progress {})", f, bufferSize, progress);

        FSDataOutputStream lakefsRes = lfsFileSystem.append(f, bufferSize, progress);
        FSDataOutputStream s3aRes = s3AFileSystem.append(translateLakeFSPathToS3APath(f), bufferSize, progress);
        LOG.trace("{}[append] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;//unused
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.trace("rename(src {}, dst {})", src, dst);

        boolean lakefsRes = lfsFileSystem.rename(src, dst);
        boolean s3aRes = s3AFileSystem.rename(translateLakeFSPathToS3APath(src), translateLakeFSPathToS3APath(dst));
        LOG.trace("{}[rename] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        LOG.trace("delete(f {}, recursive {})", f, recursive);

        boolean lakefsRes = lfsFileSystem.delete(f, recursive);
        boolean s3aRes = s3AFileSystem.delete(translateLakeFSPathToS3APath(f), recursive);
        LOG.trace("{}[delete] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        LOG.trace("listStatus(f {})", f);

        FileStatus[] lakefsRes = lfsFileSystem.listStatus(f);
        FileStatus[] s3aRes = s3AFileSystem.listStatus(translateLakeFSPathToS3APath(f));
        LOG.trace("{}[listStatus] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }

        for (FileStatus stat : s3aRes) {
            Path s3aPath = stat.getPath();
            Path lfsPath = translateS3APathToLakeFSPath(s3aPath);
            stat.setPath(lfsPath);
        }
        return s3aRes;
    }

    /**
     * Set the current working directory for the given file system. All relative
     * paths will be resolved relative to it.
     *
     * @param new_dir
     */
    @Override
    public void setWorkingDirectory(Path new_dir) {
        LOG.trace("setWorkingDirectory(new_dir {})", new_dir);

        if (useLakeFSFileSystemRes) {
            lfsFileSystem.setWorkingDirectory(new_dir);
        }
        s3AFileSystem.setWorkingDirectory(translateLakeFSPathToS3APath(new_dir));
    }

    @Override
    public Path getWorkingDirectory() {
        LOG.trace("getWorkingDirectory()");

        Path lakefsRes = lfsFileSystem.getWorkingDirectory();
        Path s3aRes = s3AFileSystem.getWorkingDirectory();
        LOG.trace("{}[getWorkingDirectory] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        LOG.trace("mkdirs(f {}, permission {})", f, permission);

        boolean lakefsRes = lfsFileSystem.mkdirs(f, permission);
        boolean s3aRes = s3AFileSystem.mkdirs(translateLakeFSPathToS3APath(f), permission);
        LOG.trace("{}s[mkdirs] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LOG.trace("getFileStatus(f {})", f);

        FileStatus lakefsRes = new FileStatus();
        FileStatus s3aRes = new FileStatus();
        Path s3aPath = translateLakeFSPathToS3APath(f);
        boolean fileNotFoundOnLakeFS = false;
        boolean fileNotFoundOnLakeS3A = false;
        try {
            lakefsRes = lfsFileSystem.getFileStatus(f);
        } catch (Exception e) {
            LOG.error("[getFileStatus] Can't get {} file status with lakeFSFileSystem", f);
            fileNotFoundOnLakeFS = true;
        }
        try {
            s3aRes = s3AFileSystem.getFileStatus(s3aPath);
        } catch (Exception e) {
            LOG.error("[getFileStatus] Can't get {} file status with S3AFileSystem", s3aPath);
            fileNotFoundOnLakeS3A = true;
        }

        LOG.trace("{}[getFileStatus] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemRes) {
            if (fileNotFoundOnLakeFS) {
                throw new FileNotFoundException("getFileStatus, not found on lakeFS");
            }
            return lakefsRes;
        }

        if (fileNotFoundOnLakeS3A) {
            throw new FileNotFoundException("getFileStatus, not found on S3A");
        }
        Path lfsPath = translateS3APathToLakeFSPath(s3aPath);
        s3aRes.setPath(lfsPath);
        return s3aRes;
    }

    /**
     * An output stream encapsulating two output streams, one for each file system the tracer uses.
     */
    private class TracerOutputTStream extends OutputStream {

        private FSDataOutputStream lakeFSStream;
        private FSDataOutputStream s3aStream;

        public TracerOutputTStream(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
            lakeFSStream = lfsFileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
            s3aStream = s3AFileSystem.create(translateLakeFSPathToS3APath(f), permission, overwrite, bufferSize,
                    replication, blockSize, progress);
        }

        @Override
        public void write(int b) throws IOException {
            lakeFSStream.write(b);
            s3aStream.write(b);
        }

        @Override
        public void write(@NotNull byte[] b) throws IOException {
            lakeFSStream.write(b);
            s3aStream.write(b);
        }

        @Override
        public void write(@NotNull byte[] b, int off, int len) throws IOException {
            lakeFSStream.write(b, off, len);
            s3aStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            lakeFSStream.flush();
            s3aStream.flush();
        }

        @Override
        public void close() throws IOException {
            lakeFSStream.close();
            s3aStream.close();
        }
    }
}
