package io.lakefs;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.StagingApi;
import io.lakefs.clients.sdk.model.StagingLocation;
import io.lakefs.clients.sdk.model.StagingMetadata;
import io.lakefs.utils.ObjectLocation;

public class LakeFSLinker {
    private final StagingLocation stagingLocation;
    private final ObjectLocation objectLoc;
    private final LakeFSFileSystem lfs;
    private final LakeFSClient lakeFSClient;
    private final boolean overwrite;

    public LakeFSLinker(LakeFSFileSystem lfs, LakeFSClient lfsClient,
            ObjectLocation objectLoc, StagingLocation stagingLocation, boolean overwrite) {
        this.objectLoc = objectLoc;
        this.stagingLocation = stagingLocation;
        this.lfs = lfs;
        this.lakeFSClient = lfsClient;
        this.overwrite = overwrite;
    }

    public void link(String eTag, long byteSize) throws IOException {
        StagingApi staging = lakeFSClient.getStagingApi();
        StagingMetadata stagingMetadata =
                new StagingMetadata().checksum(eTag).sizeBytes(byteSize).staging(stagingLocation);
        try {
            StagingApi.APIlinkPhysicalAddressRequest request = 
                    staging.linkPhysicalAddress(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), stagingMetadata);
            if (!overwrite) {
                request.ifNoneMatch("*");
            }
            request.execute();
        } catch (ApiException e) {
            throw new IOException("link lakeFS path to physical address", e);
        }
        lfs.deleteEmptyDirectoryMarkers(new Path(objectLoc.toString()).getParent());
    }
}
