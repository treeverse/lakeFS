package io.lakefs;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectErrorList;
import io.lakefs.clients.api.model.PathList;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.HashSet;
import java.util.Set;

class BulkDeleter implements Closeable {
  private static final int defaultBulkSize = 1000;

  private final ExecutorService executor;
  private final Callback callback;
  private final String repository;
  private final String branch;
  private final int bulkSize;

  private PathList pathList;
  private Future<ObjectErrorList> deletion;

  public static interface Callback {
    ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException;
  }

  public static class DeleteFailuresException extends IOException {
    private final ObjectErrorList errorList;

    public DeleteFailuresException(ObjectErrorList errorList) {
      super("failed to delete: " + errorList.toString());
      this.errorList = errorList;
    }
  }

  /**
   * Construct a BulkDeleter to bulk-delete objects on branch in repository,
   * using objectsApi on executor (which should be single-threaded for
   * correctness).
   */
  BulkDeleter(ExecutorService executor, Callback callback, String repository, String branch, int bulkSize) {
    System.out.printf("[DEBUG] start for %s %s bulk %d\n", repository, branch, bulkSize);
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
  public synchronized void close() throws IOException {
    if (pathList != null && !pathList.getPaths().isEmpty()) {
      startDeletingUnlocked();
    }
    maybeWaitForDeletionUnlocked();
  }

  /**
   * Start deleting everything in pathList and empty it.  Must call locked.
   */
  private void startDeletingUnlocked() throws IOException, DeleteFailuresException {
    maybeWaitForDeletionUnlocked();
    PathList toDelete = pathList;
    pathList = null;
    deletion = executor.submit(new Callable() {
        @Override
        public ObjectErrorList call() throws ApiException, InterruptedException, DeleteFailuresException {
          ObjectErrorList ret = callback.apply(repository, branch, toDelete);
          return ret;
        }
      });
  }

  /**
   * Wait for deletion callback to end if it is non-null, then make it null.
   * Must call locked.
   *
   * @throws DeleteFailuresException if deletion did not (entirely) succeed.
   */
  private void maybeWaitForDeletionUnlocked() throws DeleteFailuresException, IOException {
    try {
      if (deletion != null) {
        ObjectErrorList errors = deletion.get();
        deletion = null;
        if (errors != null && errors.getErrors() != null && !errors.getErrors().isEmpty()) {
          throw new DeleteFailuresException(errors);
        }
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof DeleteFailuresException) {
        throw (DeleteFailuresException)cause;
      } else if (cause instanceof IOException) {
        throw (IOException)cause;
      } else {
        throw new IOException("failed to wait for bulk delete", cause);
      }
    } catch (InterruptedException ie) {
      throw new IOException("wait for deletion", ie);
    }
  }
}
