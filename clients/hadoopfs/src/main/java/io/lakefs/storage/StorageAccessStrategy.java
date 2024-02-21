package io.lakefs.storage;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import io.lakefs.clients.sdk.ApiException;
import io.lakefs.utils.ObjectLocation;

public interface StorageAccessStrategy {
    default FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation,
                                                     CreateOutputStreamParams params) throws ApiException, IOException {
        return createDataOutputStream(objectLocation, params, true);
    }

    FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation, CreateOutputStreamParams params, boolean overwrite)
            throws ApiException, IOException;

    FSDataInputStream createDataInputStream(ObjectLocation objectLocation, int bufSize) throws ApiException, IOException;
}
