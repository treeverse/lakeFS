package io.lakefs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;

import java.io.IOException;

public class LakeFSLocatedFileStatus extends LocatedFileStatus {

    private final String checksum;
    private final String physicalAddress;
    private final boolean isEmptyDirectory;

    public LakeFSLocatedFileStatus(LakeFSFileStatus status, BlockLocation[] locations) throws IOException {
        super(status, locations);
        this.checksum = status.getChecksum();
        this.physicalAddress = status.getPhysicalAddress();
        this.isEmptyDirectory = status.isEmptyDirectory();
    }

    public LakeFSFileStatus toLakeFSFileStatus() {
        return new LakeFSFileStatus.Builder(getPath())
                .length(getLen())
                .isdir(isDirectory())
                .isEmptyDirectory(isEmptyDirectory)
                .blockSize(getBlockSize())
                .mTime(getModificationTime())
                .checksum(checksum)
                .physicalAddress(physicalAddress)
                .build();
    }
}
