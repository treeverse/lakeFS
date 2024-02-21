package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.LakeFSLinker;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.ObjectsApi;
import io.lakefs.clients.sdk.StagingApi;
import io.lakefs.clients.sdk.model.ObjectStats;
import io.lakefs.clients.sdk.model.StagingLocation;
import io.lakefs.utils.ObjectLocation;

public class PresignedStorageAccessStrategy implements StorageAccessStrategy {

    private final LakeFSFileSystem lakeFSFileSystem;
    private final LakeFSClient lfsClient;

    public PresignedStorageAccessStrategy(LakeFSFileSystem lakeFSFileSystem,
            LakeFSClient lfsClient) {
        this.lakeFSFileSystem = lakeFSFileSystem;
        this.lfsClient = lfsClient;
    }
    
    @Override
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation,
            CreateOutputStreamParams params, boolean overwrite) throws ApiException, IOException {
        StagingApi stagingApi = lfsClient.getStagingApi();
        StagingLocation stagingLocation =
            stagingApi.getPhysicalAddress(objectLocation.getRepository(),
                                          objectLocation.getRef(), objectLocation.getPath())
            .presign(true).execute();
        URL presignedUrl = new URL(stagingLocation.getPresignedUrl());
        HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/octet-stream");
        connection.setRequestMethod("PUT");
        LakeFSLinker linker = new LakeFSLinker(lakeFSFileSystem, lfsClient, objectLocation, stagingLocation, overwrite);
        OutputStream out = new LakeFSFileSystemOutputStream(connection, linker);
        // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
        return new FSDataOutputStream(out, null);
    }

    @Override
    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation, int bufSize)
            throws ApiException, IOException {
        ObjectsApi objectsApi = lfsClient.getObjectsApi();
        ObjectStats stats = objectsApi.statObject(objectLocation.getRepository(),
                                                  objectLocation.getRef(),
                                                  objectLocation.getPath())
            .userMetadata(false).presign(true)
            .execute();
        return new FSDataInputStream(new HttpRangeInputStream(stats.getPhysicalAddress(), bufSize));
    }
}
