package io.lakefs;

import static org.mockito.Mockito.when;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.api.model.StagingLocation;

public class LakeFSFileSystemPresignedModeTest extends LakeFSFileSystemTest {

    void initConfiguration() {
        conf.set("fs.lakefs.presigned.mode", "true");
    }

    StagingLocation mockGetPhysicalAddress(String repo, String branch, String key,
            String physicalKey) throws ApiException {
        URL url =
                s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(s3Bucket, physicalKey)
                        .withMethod(HttpMethod.PUT).withExpiration(
                                new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))));
        StagingLocation stagingLocation = new StagingLocation().token("foo")
                .physicalAddress(s3Url("/" + physicalKey)).presignedUrl(url.toString());
        when(stagingApi.getPhysicalAddress(repo, branch, key, true)).thenReturn(stagingLocation);
        return stagingLocation;
    }

    @Override
    void mockStatObject(String repo, String branch, String key, String physicalKey, Long sizeBytes)
            throws ApiException {
        URL url =
                s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(s3Bucket, physicalKey)
                        .withMethod(HttpMethod.GET).withExpiration(
                                new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))));
        when(objectsApi.statObject(repo, branch, key, false, true))
                .thenReturn(new ObjectStats().path("lakefs://" + repo + "/" + branch + "/" + key)
                        .pathType(PathTypeEnum.OBJECT).physicalAddress(url.toString())
                        .checksum(UNUSED_CHECKSUM).mtime(UNUSED_MTIME).sizeBytes((long) sizeBytes));
    }
}
