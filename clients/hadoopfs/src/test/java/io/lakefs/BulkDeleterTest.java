package io.lakefs;

import io.lakefs.clients.api.*;
import io.lakefs.clients.api.model.*;

import org.junit.Assert;
import org.junit.Test;
import org.hamcrest.CustomTypeSafeMatcher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;

public class BulkDeleterTest {
    protected ExecutorService executorService = Executors.newSingleThreadExecutor();

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
        private int i = 0;

        private int bulkSize;
        private int numPaths;

        Callback(int bulkSize, int numPaths) {
            this.bulkSize = bulkSize;
            this.numPaths = numPaths;
        }

        public ObjectErrorList apply(String repository, String branch, PathList pathList) throws ApiException {
            Assert.assertNotNull(pathList);
            Assert.assertThat(pathList.getPaths().size(),
                              new CustomTypeSafeMatcher<Integer>(String.format("has at most %d elements", bulkSize)) {
                                  public boolean matchesSafely(Integer size) {
                                      return size <= bulkSize;
                                  }
                              });
            for(String p: pathList.getPaths()) {
                Assert.assertEquals(String.format("%d", i++), p);
            }
            return new ObjectErrorList();
        }
        public int getCount() { return i; }
    }

    protected void goodBulkCase(int bulkSize, int numPaths) throws IOException {
        Callback callback = new Callback(bulkSize, numPaths);
        BulkDeleter deleter = new BulkDeleter(executorService, callback, "repo", "branch", 50);

        for (int i = 0; i < numPaths; i++) {
            deleter.add(String.format("%d", i));
        }
        deleter.close();
        Assert.assertEquals(numPaths, callback.getCount());
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
