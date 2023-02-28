package io.lakefs.storage;

import org.apache.hadoop.util.Progressable;

public class CreateOutputStreamParams {
    boolean overwrite;
    int bufferSize;
    short replication;
    long blockSize;
    Progressable progress;

    public CreateOutputStreamParams overwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public CreateOutputStreamParams bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public CreateOutputStreamParams replication(short replication) {
        this.replication = replication;
        return this;
    }

    public CreateOutputStreamParams blockSize(long blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public CreateOutputStreamParams progress(Progressable progress) {
        this.progress = progress;
        return this;
    }
}
