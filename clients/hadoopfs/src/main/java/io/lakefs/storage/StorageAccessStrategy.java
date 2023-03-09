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
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation, CreateOutputStreamParams params)
            throws ApiException, IOException;

    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation) throws ApiException, IOException;
}
