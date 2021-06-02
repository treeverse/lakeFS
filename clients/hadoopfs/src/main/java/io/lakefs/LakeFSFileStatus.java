package io.lakefs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * A class that represents the Lakefs status of a path. A path can be
 * {@link io.lakefs.clients.api.model.ObjectStats.PathTypeEnum#OBJECT} or a
 * {@link io.lakefs.clients.api.model.ObjectStats.PathTypeEnum#COMMON_PREFIX}.
 * For lakefs objects, an instance of this class encapsulates its {@link io.lakefs.clients.api.model.ObjectStats}, and
 * for common-prefix it acts as a markup that a common-prefix considered a directory.
 */
public class LakeFSFileStatus extends FileStatus {

    private String checksum;
    private String physicalAddress;

    private LakeFSFileStatus(Builder builder) {
        super(builder.length, builder.isdir, builder.blockReplication, builder.blockSize, builder.mTime, builder.path);
        this.checksum = builder.checksum;
        this.physicalAddress = builder.physicalAddress;
    }

    public String getChecksum() {
        return checksum;
    }

    public String getPhysicalAddress() {
        return physicalAddress;
    }

    public static class Builder {
        private Path path;
        private long length;
        private boolean isdir;
        private short blockReplication;
        private long blockSize;
        private long mTime;
        private String checksum;
        private String physicalAddress;

        public Builder(Path path) {
            this.path = path;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder isdir(boolean isdir) {
            this.isdir = isdir;
            return this;
        }

        public Builder blockReplication(short blockReplication) {
            this.blockReplication = blockReplication;
            return this;
        }

        public Builder blocksize(long blocksize) {
            this.blockSize = blocksize;
            return this;
        }

        public Builder mTime(long mTime) {
            this.mTime = mTime;
            return this;
        }

        public Builder checksum(String checksum) {
            this.checksum = checksum;
            return this;
        }

        public Builder physicalAddress(String physicalAddress) {
            this.physicalAddress = physicalAddress;
            return this;
        }

        public LakeFSFileStatus build() {
            LakeFSFileStatus lakeFSFileStatus =  new LakeFSFileStatus(this);
            return lakeFSFileStatus;
        }
    }
}
