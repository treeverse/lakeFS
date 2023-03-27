package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.lakefs.LakeFSLinker;

/**
 * Wraps a FSDataOutputStream to link file on staging when done writing
 */
class LinkOnCloseOutputStream extends OutputStream {
    private final URI physicalUri;
    private final MetadataClient metadataClient;
    private final OutputStream out;
    private final AtomicBoolean isLinked = new AtomicBoolean(false);
    private final LakeFSLinker linker;

    /**
     * @param physicalUri translated physical location of object data for underlying FileSystem.
     * @param metadataClient client used to request metadata information from the underlying FileSystem.
     * @param out stream on underlying filesystem to wrap.
     * @param linker {@link LakeFSLinker} for the given object.
     */
    LinkOnCloseOutputStream(URI physicalUri, MetadataClient metadataClient, OutputStream out,
            LakeFSLinker linker) {
        this.physicalUri = physicalUri;
        this.metadataClient = metadataClient;
        this.out = out;
        this.linker = linker;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void close() throws IOException {
        out.close();
        // Now the object is on the underlying store, find its parameters (sadly lost by
        // the underlying Hadoop FileSystem) so we can link it on lakeFS.
        if (!this.isLinked.getAndSet(true)) {
            ObjectMetadata objectMetadata = metadataClient.getObjectMetadata(physicalUri);
            linker.link(objectMetadata.getETag(), objectMetadata.getContentLength());
        }
    }
}
