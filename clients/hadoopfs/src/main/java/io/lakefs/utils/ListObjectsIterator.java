package io.lakefs.utils;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;

import org.apache.hadoop.fs.BatchedRemoteIterator;

import java.io.IOException;

public class ListObjectsIterator extends BatchedRemoteIterator<String, ObjectStats> {
    public static int REQUEST_AMOUNT = 1000;

    private final ObjectsApi objects;
    private final String repository;
    private final String ref;
    private final String prefix;
    private final String delimiter;

    public ListObjectsIterator(ObjectsApi objects, String repository, String ref, String prefix, String delimiter) {
        super("");
        this.objects = objects;
        this.repository = repository;
        this.ref = ref;
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    private static class ListObjectsBatch implements BatchedEntries<ObjectStats> {
        private final ObjectStatsList list;

        public ListObjectsBatch(ObjectStatsList list) {
            this.list = list;
        }

        @Override
        public ObjectStats get(int i) {
            return list.getResults().get(i);
        }

        @Override
        public int size() {
            return list.getResults().size();
        }

        @Override
        public boolean hasMore() {
            return list.getPagination().getHasMore();
        }
    }

    public BatchedEntries<ObjectStats> makeRequest(String prevKey) throws IOException {
        try {
            return new ListObjectsBatch(objects.listObjects(repository, ref, true, prevKey, REQUEST_AMOUNT, delimiter, prefix));
        } catch (ApiException e) {
            throw new IOException(String.format("Failed to list on {}:{} prefix {} delimiter {} starting at {}",
                                                repository, ref, prefix, delimiter, prevKey),
                                  e);
        }
    }

    public String elementToPrevKey(ObjectStats stats) {
        return stats.getPath();
    }
}
