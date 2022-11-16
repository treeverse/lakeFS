package io.lakefs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A class that extends LocatedFileStatus with the actual LakeFSFileStatus.
 * Needed because LocatedFileStatus slices FileStatus to derive from it,
 * losing the further details available on LakeFSFileStatus.
 *
 * LakeFSFileStatus.listFiles returns a LakeFSLocatedFileStatus for all its
 * values; downcast to acces the original status.
 */
public class LakeFSLocatedFileStatus extends LocatedFileStatus {
    private LakeFSFileStatus stat;

    public LakeFSLocatedFileStatus(LakeFSFileStatus stat, BlockLocation[] locations) throws IOException {
        super(stat, locations);
        this.stat = stat;
    }

    public LakeFSFileStatus getStatus() {
        return stat;
    }
}
