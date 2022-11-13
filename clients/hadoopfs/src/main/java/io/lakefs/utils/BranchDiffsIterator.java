package io.lakefs.utils;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.BranchesApi;
import io.lakefs.clients.api.model.Diff;
import io.lakefs.clients.api.model.DiffList;

import org.apache.hadoop.fs.BatchedRemoteIterator;

import java.io.IOException;

public class BranchDiffsIterator extends BatchedRemoteIterator<String, Diff> {
    public static int REQUEST_AMOUNT = 1000;

    final private BranchesApi branches;
    final private String repository;
    final private String branch;
    final private String prefix;
    final private String delimiter;

    public BranchDiffsIterator(BranchesApi branches, String repository, String branch, String prefix, String delimiter) {
        super("");
        this.branches = branches;
        this.repository = repository;
        this.branch = branch;
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    private static class DiffListBatch implements BatchedEntries<Diff> {
        private final DiffList diffs;

        public DiffListBatch(DiffList diffs) {
            this.diffs = diffs;
        }

        @Override
        public Diff get(int i) {
            return diffs.getResults().get(i);
        }

        @Override
        public int size() {
            return diffs.getResults().size();
        }

        @Override
        public boolean hasMore() {
            return diffs.getPagination().getHasMore();
        }
    }

    public BatchedRemoteIterator.BatchedEntries<Diff> makeRequest(String prevKey) throws IOException {
        try {
            return new DiffListBatch(branches.diffBranch(repository, branch, prevKey, REQUEST_AMOUNT, prefix, delimiter));
        } catch (ApiException e) {
            throw new IOException(String.format("Failed to list uncommitted diffs on {}:{} prefix {} delimiter {} starting at {}",
                                                repository, branch, prefix, delimiter, prevKey),
                                  e);
        }
    }

    public String elementToPrevKey(Diff diff) {
        return diff.getPath();
    }
}
