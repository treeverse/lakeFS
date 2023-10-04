package io.lakefs;

import static io.lakefs.Constants.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.lakefs.Constants.AccessMode;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.BranchesApi;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.model.ObjectCopyCreation;
import io.lakefs.clients.api.model.ObjectErrorList;
import io.lakefs.clients.api.model.ObjectStageCreation;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Pagination;
import io.lakefs.clients.api.model.PathList;
import io.lakefs.clients.api.model.Repository;
import io.lakefs.clients.api.model.StorageConfig;
import io.lakefs.storage.CreateOutputStreamParams;
import io.lakefs.storage.PhysicalAddressTranslator;
import io.lakefs.storage.PresignedStorageAccessStrategy;
import io.lakefs.storage.SimpleStorageAccessStrategy;
import io.lakefs.storage.StorageAccessStrategy;
import io.lakefs.utils.ObjectLocation;

/**
 * A dummy implementation of the core lakeFS Filesystem.
 * This class implements a {@link LakeFSFileSystem} that can be registered to
 * Spark and support limited write and read actions.
 * <p>
 * Configure Spark to use lakeFS filesystem by property:
 * spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem.
 * <p>
 * Configure the application or the filesystem application by properties:
 * fs.lakefs.endpoint=http://localhost:8000/api/v1
 * fs.lakefs.access.key=AKIAIOSFODNN7EXAMPLE
 * fs.lakefs.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
 */
public class LakeFSFileSystem extends FileSystem {
    public static final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystem.class);
    public static final Logger OPERATIONS_LOG = LoggerFactory.getLogger(LakeFSFileSystem.class + "[OPERATION]");
    public static final String LAKEFS_DELETE_BULK_SIZE = "fs.lakefs.delete.bulk_size";

    private Configuration conf;
    private URI uri;
    private Path workingDirectory = new Path(Constants.SEPARATOR);
    private ClientFactory clientFactory;
    private LakeFSClient lfsClient;
    private int listAmount;
    private FileSystem fsForConfig;
    private boolean failedFSForConfig = false;
    private PhysicalAddressTranslator physicalAddressTranslator;
    private StorageAccessStrategy storageAccessStrategy;
    private AccessMode accessMode;
    private static File emptyFile = new File("/dev/null");

    // Currently bulk deletes *must* receive a single-threaded executor!
    private ExecutorService deleteExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });
    
    @Override
    public URI getUri() {
        return uri;
    }

    public interface ClientFactory {
        LakeFSClient newClient() throws IOException;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        initializeWithClientFactory(name, conf, new ClientFactory() {
                public LakeFSClient newClient() throws IOException { return new LakeFSClient(name.getScheme(), conf); }
            });
    }

    void initializeWithClientFactory(URI name, Configuration conf, ClientFactory clientFactory) throws IOException {
        super.initialize(name, conf);
        this.uri = name;
        this.conf = conf;
        this.clientFactory = clientFactory;
        this.lfsClient = clientFactory.newClient();

        String host = name.getHost();
        if (host == null) {
            throw new IOException("Invalid repository specified");
        }
        setConf(conf);

        listAmount = FSConfiguration.getInt(conf, uri.getScheme(), LIST_AMOUNT_KEY_SUFFIX, DEFAULT_LIST_AMOUNT);
        String accessModeConf = FSConfiguration.get(conf, uri.getScheme(), ACCESS_MODE_KEY_SUFFIX);
        accessMode = AccessMode.valueOf(StringUtils.defaultIfBlank(accessModeConf, AccessMode.SIMPLE.toString()).toUpperCase());
        if (accessMode == AccessMode.PRESIGNED) {
            storageAccessStrategy = new PresignedStorageAccessStrategy(this, lfsClient);
        } else if (accessMode == AccessMode.SIMPLE) {
            // setup address translator for simple storage access strategy
            try {
                StorageConfig storageConfig = lfsClient.getConfigApi().getStorageConfig();
                physicalAddressTranslator = new PhysicalAddressTranslator(storageConfig.getBlockstoreType(),
                        storageConfig.getBlockstoreNamespaceValidityRegex());
            } catch (ApiException e) {
                throw new IOException("Failed to get lakeFS blockstore type", e);
            }
            storageAccessStrategy = new SimpleStorageAccessStrategy(this, lfsClient, conf, physicalAddressTranslator);
        } else {
            throw new IOException("Invalid access mode: " + accessMode);
        }
    }

    private synchronized FileSystem getFSForConfig() {
        if (fsForConfig != null) {
            return fsForConfig;
        }
        if (failedFSForConfig || accessMode == AccessMode.PRESIGNED || physicalAddressTranslator == null) {
            return null;
        }
        Path path = new Path(uri);
        ObjectLocation objectLoc = pathToObjectLocation(path);
        RepositoriesApi repositoriesApi = lfsClient.getRepositoriesApi();
        try {
            Repository repository = repositoriesApi.getRepository(objectLoc.getRepository());
            String storageNamespace = repository.getStorageNamespace();
            URI storageURI = URI.create(storageNamespace);
            Path physicalPath = physicalAddressTranslator.translate(storageNamespace);
            fsForConfig = physicalPath.getFileSystem(conf);
        } catch (ApiException | URISyntaxException | IOException e) {
            failedFSForConfig = true;
            LOG.warn("get underlying filesystem for {}: {} (use default values)", path, e);
        }
        return fsForConfig;
    }

    @FunctionalInterface
    private interface BiFunctionWithIOException<U, V, R> {
        R apply(U u, V v) throws IOException;
    }

    @Override
    public long getDefaultBlockSize(Path path) {
        if (getFSForConfig() != null) {
            return getFSForConfig().getDefaultBlockSize(path);
        }
        return Constants.DEFAULT_BLOCK_SIZE;
    }

    @Override
    public long getDefaultBlockSize() {
        if (getFSForConfig() != null) {
            return getFSForConfig().getDefaultBlockSize();
        }
        return Constants.DEFAULT_BLOCK_SIZE;
    }

    @Override
    public FSDataInputStream open(Path path, int bufSize) throws IOException {
        OPERATIONS_LOG.trace("open({})", path);
        try {                        
            ObjectLocation objectLoc = pathToObjectLocation(path);
            return storageAccessStrategy.createDataInputStream(objectLoc, bufSize);
        } catch (ApiException e) {
            throw translateException("open: " + path, e);
        }
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
        OPERATIONS_LOG.trace("list_files({}), recursive={}", f, recursive);
        return new RemoteIterator<LocatedFileStatus>() {
            private final ListingIterator iterator = new ListingIterator(f, recursive, listAmount);

            @Override
            public boolean hasNext() throws IOException {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LakeFSFileStatus status = iterator.next();
                BlockLocation[] locations = status.isFile()
                        ? getFileBlockLocations(status, 0, status.getLen())
                        : new BlockLocation[0];
                return new LocatedFileStatus(status, locations);
            }
        };
    }

    /**
     * {@inheritDoc}
     * Called on a file write Spark/Hadoop action. This method writes the content of the file in path into stdout.
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        OPERATIONS_LOG.trace("create({})", path);
        try {
            LakeFSFileStatus status = getFileStatus(path);
            if (status.isDirectory()) {
                throw new FileAlreadyExistsException(path + " is a directory");
            }
            if (!overwrite) {
                throw new FileAlreadyExistsException(path + " already exists");
            }
        } catch (FileNotFoundException ignored) {
        }
        try {
            ObjectLocation objectLoc = pathToObjectLocation(path);
            return storageAccessStrategy.createDataOutputStream(objectLoc,
                    new CreateOutputStreamParams()
                            .bufferSize(bufferSize)
                            .blockSize(blockSize)
                            .progress(progress)
                    );
        } catch (ApiException e) {
            throw new IOException("staging.getPhysicalAddress: " + e.getResponseBody(), e);
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("Append is not supported by LakeFSFileSystem");
    }

    /**
     * Rename, behaving similarly to the POSIX "mv" command, but non-atomically.
     * 1. Rename is only supported for uncommitted data on the same branch.
     * 2. The following rename scenarios are supported:
     *   file -> existing-file-name: rename(src.txt, existing-dst.txt) -> existing-dst.txt, existing-dst.txt is overridden
     *   file -> existing-directory-name: rename(src.txt, existing-dstdir) -> existing-dstdir/src.txt
     *   file -> non-existing dst: in case of non-existing rename target, false is return. note that empty directory is
     *   considered an existing directory and rename will move the directory/file into that folder.
     *   directory -> existing directory: rename(srcDir(containing srcDir/a.txt), existing-dstdir) -> existing-dstdir/a.txt
     * 3. Rename dst path can be an uncommitted file, that will be overridden as a result of the rename operation.
     * 4. The 'mtime' of the src object is not preserved.
     *
     * @throws IOException
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        OPERATIONS_LOG.trace("rename {} to {}", src, dst);
        ObjectLocation srcObjectLoc = pathToObjectLocation(src);
        ObjectLocation dstObjectLoc = pathToObjectLocation(dst);
        // Same as s3a https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L460
        if (srcObjectLoc.getPath().isEmpty()) {
            LOG.error("rename: src {} is root directory", src);
            return false;
        }
        if (dstObjectLoc.getPath().isEmpty()) {
            LOG.error("rename: dst {} is root directory", dst);
            return false;
        }

        if (srcObjectLoc.equals(dstObjectLoc)) {
            LOG.debug("rename: src and dst refer to the same lakefs object location: {}", dst);
            return true;
        }

        if (!srcObjectLoc.onSameBranch(dstObjectLoc)) {
            LOG.error("rename: src {} and dst {} are not on the same branch. rename outside this scope is unsupported "
                    + "by lakefs.", src, dst);
            return false;
        }

        // Return false when src does not exist. mimics s3a's behaviour in
        // https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L468
        LakeFSFileStatus srcStatus;
        try {
            srcStatus = getFileStatus(src);
        } catch (FileNotFoundException ignored) {
            return false;
        }
        boolean result;
        if (srcStatus.isDirectory()) {
            result = renameDirectory(src, dst);
        } else {
            result = renameFile(srcStatus, dst);
        }
        if (!src.getParent().equals(dst.getParent())) {
            deleteEmptyDirectoryMarkers(dst.getParent());
            createDirectoryMarkerIfNotExists(src.getParent());
        }
        return result;
    }


    /**
     * Recursively rename objects under src dir.
     *
     * @return true if all objects under src renamed successfully, false otherwise.
     */
    private boolean renameDirectory(Path src, Path dst) throws IOException {
        try {
            // May be unnecessary with https://github.com/treeverse/lakeFS/issues/1691
            LakeFSFileStatus dstFileStatus = getFileStatus(dst);
            if (!dstFileStatus.isDirectory()) {
                // Same as https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L482
                LOG.debug("renameDirectory: rename src {} to dst {}: src is a directory and dst is a file", src, dst);
                return false;
            }
            // lakefsFs only has non-empty directories. Therefore, if the destination is an existing directory we consider
            // it to be non-empty. The behaviour is same as https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L482
            if (!dstFileStatus.isEmptyDirectory()) {
                LOG.debug("renameDirectory: rename src {} to dst {}: dst is a non-empty directory.", src, dst);
                return false;
            }
            // delete empty directory marker from destination
            // based on the same behaviour https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L549
            deleteHelper(pathToObjectLocation(dst).toDirectory());
        } catch (FileNotFoundException e) {
            LOG.debug("renameDirectory: dst {} does not exist", dst);
            // Ensure parent directory exists
            if (!isDirectory(dst.getParent())) {
                return false;
            }
        }

        ListingIterator iterator = new ListingIterator(src, true, listAmount);
        iterator.setRemoveDirectory(false);
        while (iterator.hasNext()) {
            // TODO (Tals): parallelize objects rename process.
            LakeFSFileStatus fileStatus = iterator.next();
            Path objDst = buildObjPathOnNonExistingDestinationDir(fileStatus.getPath(), src, dst);
            try {
                renameObject(fileStatus, objDst);
            } catch (IOException e) {
                // Rename dir operation in non-transactional. if one object rename failed we will end up in an
                // intermediate state. TODO: consider adding a cleanup similar to
                // https://github.com/apache/hadoop/blob/2960d83c255a00a549f8809882cd3b73a6266b6d/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/impl/RenameOperation.java#L191
                throw new IOException("renameDirectory: failed to rename src directory " + src, e);
            }
        }
        return true;
    }

    /**
     * Sample input and output
     * input:
     * renamedObj: lakefs://repo/main/dir1/file1.txt
     * srcDirPath: lakefs://repo/main/dir1
     * dstDirPath: lakefs://repo/main/dir2
     * output:
     * lakefs://repo/main/dir2/file1.txt
     */
    private Path buildObjPathOnNonExistingDestinationDir(Path renamedObj, Path srcDir, Path dstDir) {
        String renamedPath = renamedObj.toUri().getPath();
        String srcPath = srcDir.toUri().getPath();
        if (srcPath.length() == renamedPath.length()) {
            // we rename a directory
            return new Path(dstDir.toUri());
        }
        String renamedObjName = renamedPath.substring(srcPath.length() + 1);
        String newObjPath = dstDir.toUri() + SEPARATOR + renamedObjName;
        return new Path(newObjPath);
    }

    /**
     * Sample input and output
     * input:
     * srcObj: lakefs://repo/main/file1.txt
     * dstDir: lakefs://repo/main/dir1
     * output:
     * lakefs://repo/main/dir1/file1.txt
     * <p>
     * input:
     * srcObj: lakefs://repo/main/dir1/file1.txt
     * dstDir: lakefs://repo/main/dir2/file2.txt
     * output:
     * lakefs://repo/main/dir2/file2.txt
     */
    private Path buildObjPathOnExistingDestinationDir(Path srcObj, Path dstDir) {
        Path srcParent = srcObj.getParent();
        String filename = srcObj.toString().substring(srcParent.toString().length() + SEPARATOR.length());
        return new Path(dstDir + SEPARATOR + filename);
    }

    private boolean renameFile(LakeFSFileStatus srcStatus, Path dst) throws IOException {
        LakeFSFileStatus dstFileStatus;
        try {
            dstFileStatus = getFileStatus(dst);
            LOG.debug("renameFile: dst {} exists and is a {}", dst, dstFileStatus.isDirectory() ? "directory" : "file");
            if (dstFileStatus.isDirectory()) {
                dst = buildObjPathOnExistingDestinationDir(srcStatus.getPath(), dst);
                LOG.debug("renameFile: use {} to create dst {}", srcStatus.getPath(), dst);
            }
        } catch (FileNotFoundException e) {
            LOG.debug("renameFile: dst does not exist, renaming src {} to a file called dst {}",
                    srcStatus.getPath(), dst);
            // Ensure parent directory exists
            if (!isDirectory(dst.getParent())) {
                return false;
            }
        }
        return renameObject(srcStatus, dst);
    }

    /**
     * fallbackToStage determines whether the old StageObject API should be use,
     * turn true when CopyObject API is not supported.
     */
    private boolean fallbackToStage = false;

    /**
     * Non-atomic rename operation.
     *
     * @return true if rename succeeded, false otherwise
     */
    private boolean renameObject(LakeFSFileStatus srcStatus, Path dst) throws IOException {
        ObjectLocation srcObjectLoc = pathToObjectLocation(srcStatus.getPath());
        ObjectLocation dstObjectLoc = pathToObjectLocation(dst);
        if (srcStatus.isEmptyDirectory()) {
            srcObjectLoc = srcObjectLoc.toDirectory();
            dstObjectLoc = dstObjectLoc.toDirectory();
        }

        ObjectsApi objects = lfsClient.getObjectsApi();
        //TODO (Tals): Can we add metadata? we currently don't have an API to get the metadata of an object.

        if (!fallbackToStage) {
            try {
                ObjectCopyCreation creationReq = new ObjectCopyCreation()
                        .srcRef(srcObjectLoc.getRef())
                        .srcPath(srcObjectLoc.getPath());
                objects.copyObject(dstObjectLoc.getRepository(), dstObjectLoc.getRef(), dstObjectLoc.getPath(),
                        creationReq);
            } catch (ApiException e) {
                if (e.getCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR ||
                        e.getResponseBody() == null ||
                        !e.getResponseBody().contains("invalid API endpoint")) {
                    throw translateException("renameObject: src:" + srcStatus.getPath() + ", dst: " + dst + ", failed to copy object", e);
                }

                LOG.warn("Copy API doesn't exist, falling back to stageObject");
                fallbackToStage = true;
            }
        }

        if (fallbackToStage) {
            ObjectStageCreation stageCreationReq = new ObjectStageCreation()
                    .checksum(srcStatus.getChecksum())
                    .sizeBytes(srcStatus.getLen())
                    .physicalAddress(srcStatus.getPhysicalAddress());
            try {
                objects.stageObject(dstObjectLoc.getRepository(), dstObjectLoc.getRef(), dstObjectLoc.getPath(),
                        stageCreationReq);
            } catch (ApiException e) {
                throw translateException("renameObject: src:" + srcStatus.getPath() + ", dst: " + dst +
                        ", failed to stage object", e);
            }
        }

        // delete src path
        try {
            objects.deleteObject(srcObjectLoc.getRepository(), srcObjectLoc.getRef(), srcObjectLoc.getPath());
        } catch (ApiException e) {
            throw translateException("renameObject: src:" + srcStatus.getPath() + ", dst: " + dst +
                    ", failed to delete src", e);
        }
        return true;
    }

    /**
     * Translate {@link ApiException} to an {@link IOException}.
     *
     * @param msg the message describing the exception
     * @param e   the exception to translate
     * @return an IOException that corresponds to the translated API exception
     */
    private IOException translateException(String msg, ApiException e) {
        int code = e.getCode();
        switch (code) {
            case HttpStatus.SC_NOT_FOUND:
                return (FileNotFoundException) new FileNotFoundException(msg).initCause(e);
            case HttpStatus.SC_FORBIDDEN:
                return (AccessDeniedException) new AccessDeniedException(msg).initCause(e);
            default:
                return new IOException(msg, e);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        OPERATIONS_LOG.trace("delete({}), recursive={}", path, recursive);
        LakeFSFileStatus status;
        try {
            status = getFileStatus(path);
        } catch (FileNotFoundException ignored) {
            return false;
        }

        boolean deleted = true;
        ObjectLocation loc = pathToObjectLocation(path);
        if (status.isDirectory()) {
            if (!recursive && !status.isEmptyDirectory()) {
                throw new IOException("Path is a non-empty directory: " + path);
            }

            if (status.isEmptyDirectory()) {
                loc = loc.toDirectory();
                deleted = deleteHelper(loc);
            } else {
                ObjectLocation location = pathToObjectLocation(path);
                try (BulkDeleter deleter = newDeleter(location.getRepository(), location.getRef())) {
                    ListingIterator iterator = new ListingIterator(path, true, listAmount);
                    iterator.setRemoveDirectory(false);
                    while (iterator.hasNext()) {
                        LakeFSFileStatus fileStatus = iterator.next();
                        ObjectLocation fileLoc = pathToObjectLocation(fileStatus.getPath());
                        if (fileStatus.isDirectory()) {
                            fileLoc = fileLoc.toDirectory();
                        }
                        deleter.add(fileLoc.getPath());
                    }
                } catch (BulkDeleter.DeleteFailuresException e) {
                    LOG.error("delete(%s, %b): %s", path, recursive, e.toString());
                    deleted = false;
                }
            }
        } else {
            deleted = deleteHelper(loc);
        }

        createDirectoryMarkerIfNotExists(path.getParent());
        return deleted;
    }

    private BulkDeleter newDeleter(String repository, String branch) throws IOException {
        // Use a different client -- a different thread waits for its calls,
        // *late*.
        ObjectsApi objectsApi = clientFactory.newClient().getObjectsApi();
        return new BulkDeleter(deleteExecutor, new BulkDeleter.Callback() {
                public ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException {
                    return objectsApi.deleteObjects(repository, branch, pathList);
                }
            }, repository, branch, conf.getInt(LAKEFS_DELETE_BULK_SIZE, 0));
    }

    private boolean deleteHelper(ObjectLocation loc) throws IOException {
        try {
            ObjectsApi objectsApi = lfsClient.getObjectsApi();
            objectsApi.deleteObject(loc.getRepository(), loc.getRef(), loc.getPath());
        } catch (ApiException e) {
            // This condition mimics s3a behaviour in
            // https://github.com/apache/hadoop/blob/874c055e73293e0f707719ebca1819979fb211d8/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L619
            if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
                LOG.error("Could not delete: {}, reason: {}", loc, e.getResponseBody());
                return false;
            }
            throw new IOException("deleteObject", e);
        }
        return true;
    }

    /**
     * Delete parent directory markers from path until root.
     * Assumption: the caller has created an object under the path, so the empty
     * directory markers are no longer necessary.
     * Based on the S3AFileSystem implementation.
     * Note: there is a race here if this is called on a path which mkdir is trying to create.
     * 
     * @param f path to start for empty directory markers
     */
    void deleteEmptyDirectoryMarkers(Path f) {
        while (true) {
            try {
                ObjectLocation objectLocation = pathToObjectLocation(f);
                if (!objectLocation.isValidPath()) {
                    break;
                }

                LakeFSFileStatus status = getFileStatus(f);
                if (status.isDirectory() && status.isEmptyDirectory()) {
                    deleteHelper(objectLocation.toDirectory());
                } else {
                    // not an empty directory, so the parent cannot be empty either
                    break;
                }
            } catch (IOException ignored) {
            }

            if (f.isRoot()) {
                break;
            }

            f = f.getParent();
        }
    }

    /**
     * create marker object for empty directory
     * @param f path to check if empty directory marker is needed
     * @throws IOException any issue with lakeFS or underlying filesystem
     */
    private void createDirectoryMarkerIfNotExists(Path f) throws IOException {
        ObjectLocation objectLocation = pathToObjectLocation(f).toDirectory();
        if (!objectLocation.isValidPath()) {
            LOG.warn("Cannot create directory marker for invalid path {}", f.toString());
            // Safe to do nothing, because directory markers are mostly
            // useless.  This happens when the path inside the branch is
            // empty -- and cannot be created.  If the repo or branch names
            // are empty this also happens but then the actual operation
            // will fail.
            return;
        }
        try {
            ObjectsApi objects = lfsClient.getObjectsApi();
            objects.uploadObject(objectLocation.getRepository(), objectLocation.getRef(), objectLocation.getPath(), null, "*", emptyFile);
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.SC_PRECONDITION_FAILED) {
                LOG.trace("createDirectoryMarkerIfNotExists: Ignore {} response, marker exists");
                return;
            }
            throw new IOException("createDirectoryMarkerIfNotExists", e);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        OPERATIONS_LOG.trace("list_status({})", path);
        ObjectLocation objectLoc = pathToObjectLocation(path);
        ObjectsApi objectsApi = lfsClient.getObjectsApi();
        try {
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false);
            LakeFSFileStatus fileStatus = convertObjectStatsToFileStatus(objectLoc, objectStat);
            return new FileStatus[]{fileStatus};
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        // list directory content
        List<FileStatus> fileStatuses = new ArrayList<>();
        ListingIterator iterator = new ListingIterator(path, false, listAmount);
        while (iterator.hasNext()) {
            LakeFSFileStatus fileStatus = iterator.next();
            fileStatuses.add(fileStatus);
        }
        if (!fileStatuses.isEmpty()) {
            return fileStatuses.toArray(new FileStatus[0]);
        }
        // nothing to list - check if it is an empty directory marker
        try {
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(),
                    objectLoc.getPath() + SEPARATOR, false, false);
            LakeFSFileStatus fileStatus = convertObjectStatsToFileStatus(objectLoc, objectStat);
            if (fileStatus.isEmptyDirectory()) {
                return new FileStatus[0];
            }
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        throw new FileNotFoundException("No such file or directory: " + path);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDirectory = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDirectory;
    }

    /**
     * Make the given path and all non-existent parents into directories.
     * We use the same technic as S3A implementation, an object size 0, without a name with delimiter ('/') that
     * keeps the directory exists.
     * When we write an object into the directory - we can delete the marker.
     * @param path path to create
     * @param fsPermission to apply (passing to the underlying filesystem)
     * @return an IOException that corresponds to the translated API exception
     */
    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        OPERATIONS_LOG.trace("mkdirs({})", path);
        try {
            // Check that path is not already a directory
            FileStatus fileStatus = getFileStatus(path);
            if (fileStatus.isDirectory()) {
                return true;
            }
            throw new FileAlreadyExistsException("Path is a file: " + path);
        } catch (FileNotFoundException e) {
            // check if part of path is a file already
            ObjectLocation objectLocation = pathToObjectLocation(path);
            Path branchRoot = new Path(objectLocation.toRefString());
            Path currentPath = path;
            do {
                try {
                    FileStatus fileStatus = getFileStatus(currentPath);
                    if (fileStatus.isFile()) {
                        throw new FileAlreadyExistsException(String.format(
                                "Can't make directory for path '%s' since it is a file.",
                                currentPath));
                    }
                } catch (FileNotFoundException ignored) {
                }
                currentPath = currentPath.getParent();
            } while (currentPath != null && !currentPath.equals(branchRoot));

            createDirectoryMarker(path);
            return true;
        }
    }

    private void createDirectoryMarker(Path path) throws IOException {
        try {
            ObjectLocation objectLoc = pathToObjectLocation(path).toDirectory();
            OutputStream out = storageAccessStrategy.createDataOutputStream(objectLoc, null);
            out.close();
        } catch (io.lakefs.clients.api.ApiException e) {
            throw new IOException("createDirectoryMarker: " + e.getResponseBody(), e);
        }
    }

    /**
     * Return a file status object that represents the path.
     * @param path to a file or directory
     * @return a LakeFSFileStatus object
     * @throws java.io.FileNotFoundException when the path does not exist;
     *         IOException API call or underlying filesystem exceptions
     */
    @Override
    public LakeFSFileStatus getFileStatus(Path path) throws IOException {
        OPERATIONS_LOG.trace("get_file_status({})", path);
        ObjectLocation objectLoc = pathToObjectLocation(path);
        if (objectLoc.getPath().isEmpty()) {
            if (isBranchExists(objectLoc.getRepository(), objectLoc.getRef())) {
                return new LakeFSFileStatus.Builder(path).isdir(true).build();
            }
            throw new FileNotFoundException(path + " not found");
        }
        ObjectsApi objectsApi = lfsClient.getObjectsApi();
        // get object status on path
        try {
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false);
            return convertObjectStatsToFileStatus(objectLoc, objectStat);
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        // get object status on path + "/" for directory marker directory
        try {
            ObjectStats objectStat = objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath() + SEPARATOR, false, false);
            return convertObjectStatsToFileStatus(objectLoc, objectStat);
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("statObject", e);
            }
        }
        // not found as a file or directory marker; check if path is a "directory", i.e. a prefix.
        ListingIterator iterator = new ListingIterator(path, true, 1);
        iterator.setRemoveDirectory(false);
        if (iterator.hasNext()) {
            Path filePath = new Path(objectLoc.toString());
            return new LakeFSFileStatus.Builder(filePath).isdir(true).build();
        }
        throw new FileNotFoundException(path + " not found");
    }

    @Nonnull
    private LakeFSFileStatus convertObjectStatsToFileStatus(ObjectLocation objectLocation, ObjectStats objectStat) throws IOException {
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
        Path filePath = new Path(ObjectLocation.formatPath(objectLocation.getScheme(), objectLocation.getRepository(),
                objectLocation.getRef(), objectStat.getPath()));
        String physicalAddress = objectStat.getPhysicalAddress();
        boolean isDir = isDirectory(objectStat);
        boolean isEmptyDirectory = isDir && objectStat.getPathType() == ObjectStats.PathTypeEnum.OBJECT;
        long blockSize = isDir
                ? 0
                : getDefaultBlockSize();
        LakeFSFileStatus.Builder builder =
                new LakeFSFileStatus.Builder(filePath)
                        .length(length)
                        .isdir(isDir)
                        .isEmptyDirectory(isEmptyDirectory)
                        .blockSize(blockSize)
                        .mTime(modificationTime)
                        .checksum(objectStat.getChecksum())
                        .physicalAddress(physicalAddress);
        return builder.build();
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return lakefs scheme
     */
    @Override
    public String getScheme() {
        return this.uri.getScheme();
    }

    @Override
    public boolean exists(Path path) throws IOException {
        OPERATIONS_LOG.trace("exists({})", path);
        ObjectLocation objectLoc = pathToObjectLocation(path);
        // no path - check if branch exists
        if (objectLoc.getPath().isEmpty()) {
            return isBranchExists(objectLoc.getRepository(), objectLoc.getRef());
        }

        ObjectsApi objects = lfsClient.getObjectsApi();
        /*
         * path "exists" in Hadoop if one of these holds:
         *
         *    - path exists on lakeFS (regular file)
         *    - path + "/" exists (directory marker)
         *    - path + "/" + <something> exists (a nonempty directory that has no
         *      directory marker for some reason; perhaps it was not created by
         *      Spark).
         */

        String directoryPath = objectLoc.toDirectory().getPath();
        // List a small number of objects after path.  If either path or
        // path + "/" + <something> are there, then path exists.  Pick the
        // number of objects so that it costs about the same to list that
        // many objects as it does to list 1.
        try {
            // TODO(ariels,itaiad200): configure the "5" to the right value.
            // "Right" being: the number of objects that costs about the same to list as 1.
            // 5 is a good guess for now since that in DynamoDB backends listing 5 objects cost the same as 1.
            ObjectStatsList osl = objects.listObjects(objectLoc.getRepository(), objectLoc.getRef(), false, false, "", 5, "", objectLoc.getPath());
            List<ObjectStats> l = osl.getResults();
            if (l.isEmpty()) {
                // No object with any name that starts with objectLoc.
                return false;
            }
            ObjectStats first = l.get(0);
            if (first.getPath().equals(objectLoc.getPath())) {
                // objectLoc itself points at the object, it's a regular object!
                return true;
            }
            for (ObjectStats stat : l) {
                if (stat.getPath().startsWith(directoryPath)) {
                    // path exists as a directory containing this object.
                    // Also handles the case where this object is a directory marker.
                    return true;
                }
                if (stat.getPath().compareTo(directoryPath) > 0) {
                    // This object is after path + "/" and does not start
                    // with it: there can be no object under path + "/".
                    return false;
                }
            }
            if (!osl.getPagination().getHasMore()) {
                // Scanned all files with prefix path and did not find
                // anything with path or path + "/".
                return false;
            }
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
                // Repository or ref do not exist.
                return false;
            } else {
                throw new IOException("exists", e);
            }
        }

        // The initial listing did not even reach path+"/".  We know path
        // does not exist (it would have been first in that listing), so
        // just check if path+"/" or something below it exist.

        try {
            ObjectStatsList osl = objects.listObjects(objectLoc.getRepository(), objectLoc.getRef(), false, false, "", 1, "", directoryPath);
            List<ObjectStats> l = osl.getResults();
            return ! l.isEmpty();
        } catch (ApiException e) {
            // Repo and ref exist!
            throw new IOException("exists", e);
        }
    }

    private boolean isBranchExists(String repository, String branch) throws IOException {
        try {
            BranchesApi branches = lfsClient.getBranchesApi();
            branches.getBranch(repository, branch);
            return true;
        } catch (ApiException e) {
            if (e.getCode() != HttpStatus.SC_NOT_FOUND) {
                throw new IOException("getBranch", e);
            }
            return false;
        }
    }

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     *
     * @param path to extract information from.
     * @return lakeFS Location with repository, ref and path
     */
    @Nonnull
    public ObjectLocation pathToObjectLocation(Path path) {
        return ObjectLocation.pathToObjectLocation(this.workingDirectory, path);
    }

    class ListingIterator implements RemoteIterator<LakeFSFileStatus> {
        private final ObjectLocation objectLocation;
        private final String delimiter;
        private final int amount;
        private boolean removeDirectory;
        private String nextOffset;
        private boolean last;
        private List<ObjectStats> chunk;
        private int pos;

        /**
         * Returns iterator for files under path.
         * When recursive is set, the iterator will list all files under the target path (delimiter is ignored).
         * Parameter amount controls the limit for each request for listing.
         *
         * @param path      the location to list
         * @param recursive boolean for recursive listing
         * @param amount    buffer size to fetch listing
         */
        public ListingIterator(Path path, boolean recursive, int amount) {
            this.removeDirectory = recursive;
            this.chunk = Collections.emptyList();
            // we assume that 'path' is a directory
            this.objectLocation = pathToObjectLocation(path).toDirectory();
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
            String listingPath = this.objectLocation.getPath();
            do {
                try {
                    ObjectsApi objectsApi = lfsClient.getObjectsApi();
                    ObjectStatsList resp = objectsApi.listObjects(objectLocation.getRepository(), objectLocation.getRef(), false, false, nextOffset, amount, delimiter, objectLocation.getPath());
                    chunk = resp.getResults();
                    pos = 0;
                    Pagination pagination = resp.getPagination();
                    nextOffset = pagination.getNextOffset();
                    if (!pagination.getHasMore()) {
                        last = true;
                    }
                } catch (ApiException e) {
                    throw new IOException("listObjects", e);
                }
                chunk = chunk.stream().filter(item -> {
                    // filter directories if needed
                    if (this.removeDirectory && isDirectory(item)) {
                        return false;
                    }
                    // filter out the marker object of the path we list
                    return !item.getPath().equals(listingPath);
                }).collect(Collectors.toList());
                // loop until we have something or last chunk
            } while (chunk.isEmpty() && !last);
        }

        public boolean isRemoveDirectory() {
            return removeDirectory;
        }

        public void setRemoveDirectory(boolean removeDirectory) {
            this.removeDirectory = removeDirectory;
        }

        @Override
        public LakeFSFileStatus next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries");
            }
            ObjectStats objectStats = chunk.get(pos++);
            return convertObjectStatsToFileStatus(
                    objectLocation,
                    objectStats);
        }
    }

    private static boolean isDirectory(ObjectStats stat) {
        return stat.getPath().endsWith(SEPARATOR) || stat.getPathType() == ObjectStats.PathTypeEnum.COMMON_PREFIX;
    }
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        Path parent = path.getParent();
        if (parent != null && !this.getFileStatus(parent).isDirectory()) {
            throw new FileAlreadyExistsException("Not a directory: " + parent);
        } else {
            return this.create(path, permission, flags.contains(CreateFlag.OVERWRITE), bufferSize, replication, blockSize, progress);
        }
    }
}
