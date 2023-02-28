package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.utils.ObjectLocation;

public class PresignedStorageAccessStrategy implements StorageAccessStrategy {
    
    private LakeFSFileSystem lakeFSFileSystem;
    private LakeFSClient lfsClient;
    
    @Override
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation, CreateOutputStreamParams params) throws ApiException, IOException {
        StagingApi stagingApi = lfsClient.getStagingApi();
        StagingLocation stagingLocation = stagingApi.getPhysicalAddress(objectLocation.getRepository(), objectLocation.getRef(),
                objectLocation.getPath(), true);
        URL presignedUrl = new URL(stagingLocation.getPresignedUrl());
        System.out.println("presignedUrl: " + presignedUrl);
        HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/octet-stream"); // TODO be better than this
        connection.setRequestMethod("PUT");
        OutputStream out = new LakeFSFileSystemOutputStream(lakeFSFileSystem, lfsClient, connection, objectLocation, stagingLocation);
        // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
        return new FSDataOutputStream(out, null);
    }

    @Override
    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createDataInputStream'");
    }

}
