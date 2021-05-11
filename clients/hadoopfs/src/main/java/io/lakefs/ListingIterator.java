package io.lakefs;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Pagination;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static io.lakefs.Constants.SEPARATOR;

public class ListingIterator implements RemoteIterator<LocatedFileStatus> {
    public static final int AMOUNT = 1000;
    private final LakeFSFileSystem lakeFSFileSystem;
    private final ObjectLocation objectLocation;
    private final String delimiter;
    private String nextOffset;
    private boolean last;
    private List<ObjectStats> chunk;
    private int pos;

    public ListingIterator(LakeFSFileSystem lakeFSFileSystem, ObjectLocation objectLocation, boolean recursive) {
        this.lakeFSFileSystem = lakeFSFileSystem;
        this.chunk = Collections.emptyList();
        this.objectLocation = objectLocation;
        this.delimiter = recursive ? "" : SEPARATOR;
        this.last = false;
        this.pos = 0;
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
        try {
            ObjectsApi objectsApi = new ObjectsApi(lakeFSFileSystem.getApiClient());
            ObjectStatsList resp = objectsApi.listObjects(objectLocation.getRepository(), objectLocation.getRef(), objectLocation.getPath(), this.nextOffset, AMOUNT, this.delimiter);
            this.chunk = resp.getResults();
            this.pos = 0;
            Pagination pagination = resp.getPagination();
            if (pagination != null) {
                this.nextOffset = pagination.getNextOffset();
                if (!pagination.getHasMore()) {
                    this.last = true;
                }
            } else if (this.chunk.size() == 0) {
                this.last = true;
            }
        } catch (ApiException e) {
            throw new IOException("listObjects", e);
        }
    }

    @Override
    public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException("No more entries");
        }
        ObjectStats objectStats = this.chunk.get(this.pos++);
        FileStatus fileStatus = LakeFSFileSystem.convertObjectStatsToFileStatus(objectStats);
        return new LocatedFileStatus(fileStatus, null);
    }
}
