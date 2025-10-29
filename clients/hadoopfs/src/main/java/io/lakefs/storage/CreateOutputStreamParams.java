package io.lakefs.storage;

import org.apache.hadoop.util.Progressable;

public class CreateOutputStreamParams {
    int bufferSize;
    long blockSize;
    Progressable progress;

    public CreateOutputStreamParams bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
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
