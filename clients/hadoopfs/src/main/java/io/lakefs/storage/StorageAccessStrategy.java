package io.lakefs.storage;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import io.lakefs.clients.api.ApiException;
import io.lakefs.utils.ObjectLocation;

public interface StorageAccessStrategy {

    static URI translateUri(String blockstoreType, URI uri) throws java.net.URISyntaxException {
        switch (blockstoreType) {
            case "s3":
                return new URI("s3a", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(),
                        uri.getFragment());
            case "azure":
                // TODO(johnnyaug) - translate https:// style to abfs://
            default:
                throw new RuntimeException(String.format("lakeFS has unsupported blockstore type %s", blockstoreType));
        }
    }

    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation, CreateOutputStreamParams params)
            throws ApiException, IOException;

    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation) throws ApiException, IOException;
}
