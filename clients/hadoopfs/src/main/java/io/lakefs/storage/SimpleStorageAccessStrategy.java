package io.lakefs.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.lakefs.LakeFSClient;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.LakeFSLinker;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.ObjectsApi;
import io.lakefs.clients.sdk.StagingApi;
import io.lakefs.clients.sdk.model.ObjectStats;
import io.lakefs.clients.sdk.model.StagingLocation;
import io.lakefs.utils.ObjectLocation;

public class SimpleStorageAccessStrategy implements StorageAccessStrategy {
    private final PhysicalAddressTranslator physicalAddressTranslator;
    private final LakeFSFileSystem lakeFSFileSystem;
    private final LakeFSClient lfsClient;
    private final Configuration conf;

    public SimpleStorageAccessStrategy(LakeFSFileSystem lakeFSFileSystem, LakeFSClient lfsClient,
            Configuration conf, PhysicalAddressTranslator physicalAddressTranslator) {
        this.lakeFSFileSystem = lakeFSFileSystem;
        this.lfsClient = lfsClient;
        this.conf = conf;
        this.physicalAddressTranslator = physicalAddressTranslator;
    }
    
    @Override
    public FSDataOutputStream createDataOutputStream(ObjectLocation objectLocation,
            CreateOutputStreamParams params, boolean overwrite) throws ApiException, IOException {
        StagingApi staging = lfsClient.getStagingApi();
        StagingLocation stagingLocation =
            staging.getPhysicalAddress(objectLocation.getRepository(),
                                       objectLocation.getRef(), objectLocation.getPath())
            .presign(false)
            .execute();
        Path physicalPath;
        try {
            physicalPath = physicalAddressTranslator.translate(Objects.requireNonNull(stagingLocation.getPhysicalAddress()));
        } catch (URISyntaxException e) {
            throw new IOException("Failed to parse object phystical address", e);
        }
        FileSystem physicalFs = physicalPath.getFileSystem(conf);
        OutputStream physicalOut;
        if (params != null) {
            physicalOut = physicalFs.create(physicalPath, false, params.bufferSize,
                    physicalFs.getDefaultReplication(physicalPath), params.blockSize);
        } else {
            physicalOut = physicalFs.create(physicalPath);
        }
        MetadataClient metadataClient = new MetadataClient(physicalFs);
        LakeFSLinker linker = new LakeFSLinker(lakeFSFileSystem, lfsClient, objectLocation, stagingLocation, overwrite);
        LinkOnCloseOutputStream out = new LinkOnCloseOutputStream(physicalPath.toUri(), metadataClient, physicalOut, linker);
        // TODO(ariels): add fs.FileSystem.Statistics here to keep track.
        return new FSDataOutputStream(out, null);
    }
    
    @Override
    public FSDataInputStream createDataInputStream(ObjectLocation objectLocation, int bufSize)
            throws ApiException, IOException {
        ObjectsApi objects = lfsClient.getObjectsApi();
        ObjectStats stats = objects.statObject(objectLocation.getRepository(),
                                               objectLocation.getRef(), objectLocation.getPath())
            .userMetadata(false).presign(false)
            .execute();
        Path physicalPath;
        try {
            physicalPath = physicalAddressTranslator.translate(Objects.requireNonNull(stats.getPhysicalAddress()));
        } catch (URISyntaxException e) {
            throw new IOException("Failed to parse object physical address", e);
        }
        FileSystem physicalFs = physicalPath.getFileSystem(conf);
        return physicalFs.open(physicalPath, bufSize);
    }
}
