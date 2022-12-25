package io.lakefs;

import io.lakefs.utils.ObjectLocation;
import io.lakefs.utils.StringUtils;

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
 * How does the FileSystemTracer work:
 * The FileSystemTracer holds instances of {@link LakeFSFileSystem} and {@link S3AFileSystem}. On a file system operation,
 * the FileSystemTracer invokes the operation on both file systems, logs the output of both calls, and returns the result
 * of one of the file systems based on a configuration (S3AFileSystem output by default).

 * Configuration:
 * - fs.lakefs.tracer.working_dir - the s3 location in which the tracer can operate. This should be an S3 bucket name or
 *   an absolute path of a directory on a bucket.
 * Optional -
 * - fs.lakefs.tracer.use_lakefs_output - tells the tracer whether it should return the response coming from the lakefs file
 *   system or return s3a's response. by default it is set to false and returns s3a's response.
 *
 * Assumptions:
 * - The content of lakefs://repository/branch/ and s3a://${fs.lakefs.tracer.working.dir}/ should be identical.
 * - The s3 credentials available for Spark allow access to fs.lakefs.tracer.working.dir on s3.
 */
public class FileSystemTracer extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemTracer.class);
    private static final String TRACER_WORKING_DIR = "fs.lakefs.tracer.working_dir";
    private static final String USE_LAKEFS_OUTPUT = "fs.lakefs.tracer.use_lakefs_output";
    private static final Object S3_URI_SCHEME = "s3";
    private static final Object RESULTS_COMPARISON = "[RESULTS_COMPARISON]";


    /*
     A property that determines which file system's response the FileSystemTracer returns.
     */
    private boolean useLakeFSFileSystemResults;
    private LakeFSFileSystem lfsFileSystem;
    private FileSystem s3AFileSystem;
    private String s3aPathPrefix;
    private String lfsPathPrefix;

    /**
     * Transforms a lakefs path into an s3 path.
     * Example:
     *   in: lakefs://repository/branch/key=1/a.parquet
     *   out: s3://${fs.lakefs.tracer.working_dir}/key=1/a.parquet
     */
    private Path translateLakeFSPathToS3APath(Path path) {
        return replacePathPrefix(path, lfsPathPrefix, s3aPathPrefix);
    }

    /**
     * Transforms an S3A path into a lakefs path.
     * This method is used to transform the output of S3AFileSystem operations that include paths, so that Spark
     * continues to direct it's requests to the FileSystemTracer that handles paths with the "lakefs://" prefix.
     * Example:
     *   in: s3://${fs.lakefs.tracer.working_dir}/key=1/a.parquet
     *   out: lakefs://repository/branch/key=1/a.parquet
     */
    private Path translateS3APathToLakeFSPath(Path path) {
        return replacePathPrefix(path, s3aPathPrefix, lfsPathPrefix);
    }

    private Path replacePathPrefix(Path path, String curPrefix, String newPrefix) {
        String p = path.toString();
        boolean isValidPath = p.startsWith(curPrefix);
        if (isValidPath) {
            String objRelativePath = StringUtils.trimLeadingSlash(p.substring(curPrefix.length()));
            String newPath = String.format("%s/%s", newPrefix, objRelativePath);
            LOG.trace("Converted {} to {}", path, newPath);
            return new Path(newPath);
        }
        LOG.error("Invalid path {}", path);
        return null;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        lfsFileSystem = new LakeFSFileSystem();
        lfsFileSystem.initialize(name, conf);
        ObjectLocation loc = lfsFileSystem.pathToObjectLocation(new Path(name));
        lfsPathPrefix = ObjectLocation.formatPath(loc.getScheme(), loc.getRepository(), loc.getRef());

        String tracerWorkingDir = conf.get(TRACER_WORKING_DIR);
        if (tracerWorkingDir == null) {
            throw new IOException("tracerWorkingDir is null");
        }
        s3aPathPrefix = String.format("%s://%s", S3_URI_SCHEME, tracerWorkingDir);
        Path s3aPath = new Path(s3aPathPrefix);
        s3AFileSystem = s3aPath.getFileSystem(conf);

        useLakeFSFileSystemResults = conf.getBoolean(USE_LAKEFS_OUTPUT, false);
        LOG.trace("Initialization finished, fs.lakefs.tracer.use_lakefs_output: {}", useLakeFSFileSystemResults);
    }

    @Override
    public URI getUri() {
        LOG.trace("getUri");

        URI lakefsRes = lfsFileSystem.getUri();
        URI s3aRes = s3AFileSystem.getUri();
        LOG.trace("{}[getUri] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public Path makeQualified(Path path) {
        LOG.trace("makeQualified");

        Path lakefsRes = lfsFileSystem.makeQualified(path);
        Path s3aRes = s3AFileSystem.makeQualified(translateLakeFSPathToS3APath(path));
        LOG.trace("{}[makeQualified] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return translateS3APathToLakeFSPath(s3aRes);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        LOG.trace("open(Path {}, bufferSize {})", f, bufferSize);
        FSDataInputStream lakefsRes = null;
        FSDataInputStream s3aRes = null;
        IOException lakeFSException = null;
        IOException s3aException = null;
        Path s3aPath = translateLakeFSPathToS3APath(f);
        try {
            lakefsRes = lfsFileSystem.open(f, bufferSize);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[open] Can't open {} with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.open(translateLakeFSPathToS3APath(f), bufferSize);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[open] Can't open {} with S3AFileSystem, exception {}", s3aPath, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[open] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[open] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[open] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        LOG.trace("create(Path {}, permission {}, overwrite {}, bufferSize {}, replication {}, blockSize {}, progress {})",
                f, permission, overwrite, bufferSize, replication, blockSize, progress);
        FSDataOutputStream lakeFSStream = null;
        FSDataOutputStream s3aStream = null;
        IOException lakeFSException = null;
        IOException s3aException = null;
        Path s3aPath = translateLakeFSPathToS3APath(f);
        try {
            lakeFSStream = lfsFileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[create] Can't create {} with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aStream = s3AFileSystem.create(s3aPath, permission, overwrite, bufferSize,
                replication, blockSize, progress);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[create] Can't create {} with S3AFileSystem, exception {}", s3aPath, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[create] exception by lakeFSFileSystem");
            if (s3aStream != null) {
                s3aStream.close();
            }
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[create] exception by S3AFileSystem");
            if (lakeFSStream != null) {
                lakeFSStream.close();
            }
            throw s3aException;
        }

        LOG.trace("{}[create] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakeFSStream, s3aStream);
        TracerOutputTStream tOutputStream = new TracerOutputTStream(lakeFSStream, s3aStream);
        return new FSDataOutputStream(tOutputStream, null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        LOG.trace("append(f {}, bufferSize {}, progress {})", f, bufferSize, progress);

        FSDataOutputStream lakefsRes = lfsFileSystem.append(f, bufferSize, progress);
        FSDataOutputStream s3aRes = s3AFileSystem.append(translateLakeFSPathToS3APath(f), bufferSize, progress);
        LOG.trace("{}[append] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.trace("rename(src {}, dst {})", src, dst);
        boolean lakefsRes = false;
        boolean s3aRes = false;
        IOException lakeFSException = null;
        IOException s3aException = null;
        try {
            lakefsRes = lfsFileSystem.rename(src, dst);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[rename] Can't rename {} to {} with lakeFSFileSystem, exception {}", src, dst, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.rename(translateLakeFSPathToS3APath(src), translateLakeFSPathToS3APath(dst));
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[rename] Can't rename {} to {} with S3AFileSystem, exception {}", src, dst, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[rename] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[rename] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[rename] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        LOG.trace("delete(f {}, recursive {})", f, recursive);
        boolean lakefsRes = false;
        boolean s3aRes = false;
        IOException lakeFSException = null;
        IOException s3aException = null;
        try {
            lakefsRes = delete(f, recursive);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[delete] Can't delete {} with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.delete(translateLakeFSPathToS3APath(f), recursive);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[delete] Can't delete {} to {} with S3AFileSystem, exception {}", f, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[delete] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[delete] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[delete] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        LOG.trace("listStatus(f {})", f);

        FileStatus[] lakefsRes = null;
        FileStatus[] s3aRes = null;
        IOException lakeFSException = null;
        IOException s3aException = null;
        Path s3aPath = translateLakeFSPathToS3APath(f);
        try {
            lakefsRes = lfsFileSystem.listStatus(f);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[listStatus] Can't list the status of {} with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.listStatus(s3aPath);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[listStatus] Can't list the status of {} with S3AFileSystem, exception {}", s3aPath, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[listStatus] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[listStatus] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[listStatus] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        for (FileStatus stat : s3aRes) {
            Path filePath = stat.getPath();
            Path lfsPath = translateS3APathToLakeFSPath(filePath);
            stat.setPath(lfsPath);
        }
        return s3aRes;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        LOG.trace("setWorkingDirectory(new_dir {})", newDir);

        lfsFileSystem.setWorkingDirectory(newDir);
        s3AFileSystem.setWorkingDirectory(translateLakeFSPathToS3APath(newDir));
    }

    @Override
    public Path getWorkingDirectory() {
        LOG.trace("getWorkingDirectory()");

        Path lakefsRes = lfsFileSystem.getWorkingDirectory();
        Path s3aRes = s3AFileSystem.getWorkingDirectory();
        LOG.trace("{}[getWorkingDirectory] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);

        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        LOG.trace("mkdirs(f {}, permission {})", f, permission);
        boolean lakefsRes = false;
        boolean s3aRes = false;
        IOException lakeFSException = null;
        IOException s3aException = null;
        try {
            lakefsRes = lfsFileSystem.mkdirs(f, permission);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[mkdirs] Can't mkdir {} with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.mkdirs(translateLakeFSPathToS3APath(f), permission);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[mkdirs] Can't mkdir {} to {} with S3AFileSystem, exception {}", f, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[mkdirs] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[mkdirs] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[mkdirs] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
        }
        return s3aRes;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LOG.trace("getFileStatus(f {})", f);

        FileStatus lakefsRes = null;
        FileStatus s3aRes = null;
        IOException lakeFSException = null;
        IOException s3aException = null;
        Path s3aPath = translateLakeFSPathToS3APath(f);
        try {
            lakefsRes = lfsFileSystem.getFileStatus(f);
        } catch (IOException e) {
            lakeFSException = e;
            LOG.error("[getFileStatus] Can't get {} file status with lakeFSFileSystem, exception {}", f, e.getMessage());
        }
        try {
            s3aRes = s3AFileSystem.getFileStatus(s3aPath);
        } catch (IOException e) {
            s3aException = e;
            LOG.error("[getFileStatus] Can't get {} file status with S3AFileSystem, exception {}", s3aPath, e.getMessage());
        }

        if (useLakeFSFileSystemResults && lakeFSException != null) {
            LOG.trace("[getFileStatus] exception by lakeFSFileSystem");
            throw lakeFSException;
        }
        if (!useLakeFSFileSystemResults && s3aException != null) {
            LOG.trace("[getFileStatus] exception by S3AFileSystem");
            throw s3aException;
        }

        LOG.trace("{}[getFileStatus] lakefs: {}, s3a: {}", RESULTS_COMPARISON, lakefsRes, s3aRes);
        if (useLakeFSFileSystemResults) {
            return lakefsRes;
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

        public TracerOutputTStream(FSDataOutputStream lakeFSStream, FSDataOutputStream s3aStream) throws IOException {
            this.lakeFSStream = lakeFSStream;
            this.s3aStream = s3aStream;
        }

        @Override
        public void write(int b) throws IOException {
            if (lakeFSStream != null) {
                lakeFSStream.write(b);
            }
            if (s3aStream != null) {
                s3aStream.write(b);
            }
        }

        @Override
        public void write(@NotNull byte[] b) throws IOException {
            if (lakeFSStream != null) {
                lakeFSStream.write(b);
            }
            if (s3aStream != null) {
                s3aStream.write(b);
            }
        }

        @Override
        public void write(@NotNull byte[] b, int off, int len) throws IOException {
            if (lakeFSStream != null) {
                lakeFSStream.write(b, off, len);
            }
            if (s3aStream != null) {
                s3aStream.write(b, off, len);
            }
        }

        @Override
        public void flush() throws IOException {
            if (lakeFSStream != null) {
                lakeFSStream.flush();
            }
            if (s3aStream != null) {
                s3aStream.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (lakeFSStream != null) {
                lakeFSStream.close();
            }
            if (s3aStream != null) {
                s3aStream.close();
            }
        }
    }
}
