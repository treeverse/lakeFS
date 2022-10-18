package io.lakefs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.ObjectErrorList;
import io.lakefs.clients.api.model.PathList;

class BulkDeleter implements Closeable {
    private static final int defaultBulkSize = 1000;

    private final ExecutorService executor;
    private final Callback callback;
    private final String repository;
    private final String branch;
    private final int bulkSize;

    private PathList pathList;
    // TODO(ariels): Configure this!
    private final int concurrency = 1;
    private Queue<Future<ObjectErrorList>> deletions = new ArrayDeque<>();

    public static interface Callback {
        ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException;
    }

    public static class DeleteFailuresException extends IOException {
        public DeleteFailuresException(ObjectErrorList errorList) {
            super("failed to delete: " + errorList.toString());
        }
    }

    /**
     * Construct a BulkDeleter to bulk-delete objects on branch in repository,
     * using callback on executor.
     */
    BulkDeleter(ExecutorService executor, Callback callback, String repository, String branch, int bulkSize) {
        this.executor = executor;
        this.callback = callback;
        this.repository = repository;
        this.branch = branch;
        if (bulkSize <= 0) {
            bulkSize = defaultBulkSize;
        }
        this.bulkSize = bulkSize;
    }

    BulkDeleter(ExecutorService executor, Callback callback, String repository, String branch) {
        this(executor, callback, repository, branch, defaultBulkSize);
    }

    /**
     * Add another key to be deleted.  If a bulk is ready, delete it.  Any
     * errors thrown may be related to previously-added keys.
     */
    public synchronized void add(String key) throws IOException, DeleteFailuresException {
        if (pathList == null) {
            pathList = new PathList();
        }
        pathList.addPathsItem(key);
        if (pathList.getPaths().size() >= bulkSize) {
            startDeletingUnlocked();
        }
    }

    /**
     * Close this BulkDeleter, possibly performing one last deletion.
     *
     * @throws DeleteFailuresException if last deletion did not (entirely) succeed.
     */
    @Override
    public synchronized void close() throws IOException, DeleteFailuresException {
        if (pathList != null && !pathList.getPaths().isEmpty()) {
            startDeletingUnlocked();
        }
        drainDeletionsUnlocked();
    }

    /**
     * Start deleting everything in pathList and empty it.  Must call locked.
     */
    private void startDeletingUnlocked() throws IOException, DeleteFailuresException {
        maybeWaitForDeletionUnlocked();
        PathList toDelete = pathList;
        pathList = null;
        deletions.add(executor.submit(new Callable() {
                @Override
                public ObjectErrorList call() throws ApiException, InterruptedException, DeleteFailuresException {
                    ObjectErrorList ret = callback.apply(repository, branch, toDelete);
                    return ret;
                }
            }));
    }

    /**
     * Wait for deletion callbacks to end until deletions has space.  Must
     * call locked.
     *
     * @throws DeleteFailuresException if deletion did not (entirely) succeed.
     */
    private void maybeWaitForDeletionUnlocked() throws DeleteFailuresException, IOException {
        while (deletions.size() >= concurrency) {
            waitForOneDeletionUnlocked();
        }
    }

    /**
     * Wait for deletion callbacks to end until deletions has space.  Must
     * call locked.
     *
     * @throws DeleteFailuresException if deletion did not (entirely) succeed.
     */
    private void drainDeletionsUnlocked() throws DeleteFailuresException, IOException {
        while (!deletions.isEmpty()) {
            waitForOneDeletionUnlocked();
        }
    }

    private void waitForOneDeletionUnlocked() throws DeleteFailuresException, IOException {
        try {
            Future<ObjectErrorList> deletion = deletions.poll();
            if (deletion == null) return;

            ObjectErrorList errors = deletion.get();
            if (errors != null && errors.getErrors() != null && !errors.getErrors().isEmpty()) {
                throw new DeleteFailuresException(errors);
            }
        } catch (ExecutionException e) {
            // Unwrap and re-throw e (usually)
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException)cause;
            } else if (cause instanceof Error) {
                // Don't wrap serious errors.
                throw (Error)cause;
            } else {
                throw new IOException("failed to wait for bulk delete", cause);
            }
        } catch (InterruptedException ie) {
            throw new IOException("wait for deletion", ie);
        }
    }
}
