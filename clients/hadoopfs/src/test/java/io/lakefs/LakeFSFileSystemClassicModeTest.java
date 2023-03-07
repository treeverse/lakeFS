package io.lakefs;

import static io.lakefs.LakeFSFileSystemTest.UNUSED_CHECKSUM;
import static io.lakefs.LakeFSFileSystemTest.UNUSED_MTIME;
import static org.mockito.Mockito.when;
import org.apache.hadoop.fs.Path;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.api.model.StagingLocation;

public class LakeFSFileSystemClassicModeTest extends LakeFSFileSystemTest {

    @Override
    void initConfiguration() {}

    @Override
    StagingLocation mockGetPhysicalAddress(String repo, String branch, String key,
            String physicalKey) throws ApiException {
        StagingLocation stagingLocation =
                new StagingLocation().token("foo").physicalAddress(s3Url("/" + physicalKey));
        when(stagingApi.getPhysicalAddress(repo, branch, key, false)).thenReturn(stagingLocation);
        return stagingLocation;
    }

    @Override
    void mockStatObject(String repo, String branch, String key, String physicalKey, Long sizeBytes)
            throws ApiException {
        Path p = new Path("lakefs://" + repo + "/" + branch + "/" + key);
        when(objectsApi.statObject(repo, branch, key, false, false))
                .thenReturn(new ObjectStats().path(p.toString()).pathType(PathTypeEnum.OBJECT)
                        .physicalAddress(s3Url(physicalKey)).checksum(UNUSED_CHECKSUM).mtime(UNUSED_MTIME)
                        .sizeBytes((long) sizeBytes));
    }
}
