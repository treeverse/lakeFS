package io.lakefs.storage;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.InputBuffer;

// TODO this class is only for POC
public class LakeFSFileSystemInputStream  extends FSInputStream {
    private InputBuffer in;
    private byte[] content;
    
    public LakeFSFileSystemInputStream(InputStream in, int contentLength) throws IOException {
        content = new byte[contentLength];
        IOUtils.readFully(in, content);
        this.in = new InputBuffer();
        this.in.reset(content, contentLength);
    }

    @Override
    public void seek(long pos) throws IOException {
        in.reset(content, (int)pos, content.length);
    }

    @Override
    public long getPos() throws IOException {
        return in.getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {        
        return false;
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
  
}
