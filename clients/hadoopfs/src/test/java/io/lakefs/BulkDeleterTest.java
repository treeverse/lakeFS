package io.lakefs;

import java.io.IOException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.ObjectErrorList;
import io.lakefs.clients.api.model.PathList;

public class BulkDeleterTest {
    protected ExecutorService executorService = Executors.newFixedThreadPool(3);

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    @Test
    public void nothing() throws IOException {
        BulkDeleter deleter = new BulkDeleter(executorService, new BulkDeleter.Callback() {
                public ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException {
                    throw new ApiException("failed for testing");
                }
            },
            "repo", "branch", 50);
        deleter.close();
    }

    class Callback implements BulkDeleter.Callback {
        private int bulkSize;
        private Set<String> expected;

        Callback(int bulkSize, Set<String> expected) {
            this.bulkSize = bulkSize;
            this.expected = new HashSet(expected);
        }

        public ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException {
            Assert.assertNotNull(pathList);
            Assert.assertTrue(String.format("expected at most %d paths but got %d", bulkSize, pathList.getPaths().size()),
                              pathList.getPaths().size() <= bulkSize);
            synchronized(expected) {
                for(String p: pathList.getPaths()) {
                    Assert.assertTrue(expected.remove(p));
                }
            }
            return new ObjectErrorList();
        }

        public void verify() {
            Assert.assertEquals(java.util.Collections.emptySet(), expected);
        }
    }

    protected void goodBulkCase(int bulkSize, int numPaths) throws IOException {
        Set<String> toDelete = new HashSet<>();
        for (int i = 0; i < numPaths; i++) {
            toDelete.add(String.format("%d", i));
        }
        Callback callback = new Callback(bulkSize, toDelete);

        BulkDeleter deleter = new BulkDeleter(executorService, callback, "repo", "branch", 50);
        for (int i = 0; i < numPaths; i++) {
            deleter.add(String.format("%d", i));
        }

        deleter.close();
        callback.verify();
    }

    @Test
    public void exactGoodBatches() throws IOException {
        goodBulkCase(50, 100);
    }

    @Test
    public void inexactGoodBatches() throws IOException {
        goodBulkCase(50, 103);
    }

    @Test
    public void exactGoodSingleBatch() throws IOException {
        goodBulkCase(50, 50);
    }

    @Test
    public void inexactGoodSingleBatch() throws IOException {
        goodBulkCase(50, 47);
    }

    @Test
    public void exactGoodManyBatches() throws IOException {
        goodBulkCase(50, 500);
    }

    @Test
    public void inexactGoodManyBatches() throws IOException {
        goodBulkCase(50, 493);
    }
}
