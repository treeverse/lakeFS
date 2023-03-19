package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.utils.ObjectLocation;

public class PresignedStorageAccessStrategy implements StorageAccessStrategy {

    private LakeFSFileSystem lakeFSFileSystem;
    private LakeFSClient lfsClient;

    public PresignedStorageAccessStrategy(LakeFSFileSystem lakeFSFileSystem,
            LakeFSClient lfsClient) {
        this.lakeFSFileSystem = lakeFSFileSystem;
        this.lfsClient = lfsClient;
    }

    @Override
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation,
            CreateOutputStreamParams params) throws ApiException, IOException {
        // TODO(johnnyaug) respect params
        StagingApi stagingApi = lfsClient.getStagingApi();
        StagingLocation stagingLocation =
                stagingApi.getPhysicalAddress(objectLocation.getRepository(),
                        objectLocation.getRef(), objectLocation.getPath(), true);
        URL presignedUrl = new URL(stagingLocation.getPresignedUrl());
        HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/octet-stream");
        connection.setRequestMethod("PUT");
        OutputStream out = new LakeFSFileSystemOutputStream(lakeFSFileSystem, lfsClient, connection,
                objectLocation, stagingLocation);
        // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
        return new FSDataOutputStream(out, null);
    }

    @Override
    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation, int bufSize)
            throws ApiException, MalformedURLException, IOException {
        ObjectsApi objectsApi = lfsClient.getObjectsApi();
        ObjectStats stats = objectsApi.statObject(objectLocation.getRepository(),
                objectLocation.getRef(), objectLocation.getPath(), false, true);
        return new FSDataInputStream(new HttpRangeInputStream(stats.getPhysicalAddress(), bufSize));
    }
}
