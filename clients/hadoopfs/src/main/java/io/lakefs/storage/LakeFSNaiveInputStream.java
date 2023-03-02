package io.lakefs.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.InputBuffer;

// TODO this class is only for POC
public class LakeFSNaiveInputStream extends FSInputStream {
    private boolean closed;
    final private InputBuffer inputBuffer;
    final private byte[] content;
    
    public LakeFSNaiveInputStream(InputStream in, int contentLength) throws IOException {
        content = new byte[contentLength];
        IOUtils.readFully(in, content);
        this.inputBuffer = new InputBuffer();
        this.inputBuffer.reset(content, contentLength);
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
                + " " + pos);
          }
        inputBuffer.reset(content, (int)pos, content.length - (int)pos);
    }

    @Override
    public synchronized long getPos() throws IOException {
        return inputBuffer.getPosition();
    }

    public synchronized int available() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        return inputBuffer.getLength() - inputBuffer.getPosition();
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {        
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        return inputBuffer.read();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        inputBuffer.close();
    }
}
