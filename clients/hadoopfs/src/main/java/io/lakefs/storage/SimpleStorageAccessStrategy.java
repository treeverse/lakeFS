package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.MetadataClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.StagingLocation;
import io.lakefs.utils.ObjectLocation;

public class SimpleStorageAccessStrategy implements StorageAccessStrategy {
    private PhysicalAddressTranslator physicalAddressTranslator;
    private LakeFSFileSystem lakeFSFileSystem;
    private LakeFSClient lfsClient;
    private Configuration conf;

    public SimpleStorageAccessStrategy(LakeFSFileSystem lakeFSFileSystem, LakeFSClient lfsClient, Configuration conf, PhysicalAddressTranslator physicalAddressTranslator) {
        this.lakeFSFileSystem = lakeFSFileSystem;
        this.lfsClient = lfsClient;
        this.conf = conf;
        this.physicalAddressTranslator = physicalAddressTranslator;
    }

    @Override
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation, CreateOutputStreamParams params)
            throws ApiException, IOException {
        StagingApi staging = lfsClient.getStagingApi();
        StagingLocation stagingLocation = staging.getPhysicalAddress(objectLocation.getRepository(),
                objectLocation.getRef(), objectLocation.getPath(), false);
        URI physicalUri;
        try {
            physicalUri = physicalAddressTranslator
                    .translate(new URI(Objects.requireNonNull(stagingLocation.getPhysicalAddress())));
        } catch (URISyntaxException e) {
            throw new IOException("Failed to parse object phystical address", e);
        }
        Path physicalPath = new Path(physicalUri.toString());
        FileSystem physicalFs = physicalPath.getFileSystem(conf);
        OutputStream physicalOut;
        if (params != null) {
            physicalOut = physicalFs.create(physicalPath, params.overwrite, params.bufferSize, params.replication,
                    params.blockSize);
        } else {
            physicalOut = physicalFs.create(physicalPath);
        }
        MetadataClient metadataClient = new MetadataClient(physicalFs);
        LinkOnCloseOutputStream out = new LinkOnCloseOutputStream(lakeFSFileSystem, lfsClient,
                stagingLocation, objectLocation, physicalUri, metadataClient, physicalOut);
        // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
        return new FSDataOutputStream(out, null);
    }

    @Override
    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation) throws ApiException, IOException {
        ObjectsApi objects = lfsClient.getObjectsApi();
        ObjectStats stats = objects.statObject(objectLocation.getRepository(), objectLocation.getRef(),
                objectLocation.getPath(), false, false);
        URI physicalUri;
        try {
            physicalUri = physicalAddressTranslator
                    .translate(new URI(Objects.requireNonNull(stats.getPhysicalAddress())));
        } catch (URISyntaxException e) {
            throw new IOException("Failed to parse object phystical address", e);
        }
        Path physicalPath = new Path(physicalUri.toString());
        FileSystem physicalFs = physicalPath.getFileSystem(conf);
        return physicalFs.open(physicalPath);
    }
}
