package io.lakefs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;

import java.io.IOException;

public class LakeFSLocatedFileStatus extends LocatedFileStatus {

    private String checksum;
    private String physicalAddress;

    public LakeFSLocatedFileStatus(LakeFSFileStatus status, BlockLocation[] locations) throws IOException {
        super(status, locations);
        this.checksum = status.getChecksum();
        this.physicalAddress = status.getPhysicalAddress();
    }

    public LakeFSFileStatus toLakeFSFileStatus() {
        return new LakeFSFileStatus.Builder(getPath()).length(getLen())
                        .isdir(isDirectory()).blocksize(getBlockSize()).mTime(getModificationTime())
                        .checksum(checksum).physicalAddress(physicalAddress).build();
    }
}
