package io.lakefs;

import org.mockito.Answers;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.api.model.StagingLocation;

public class LakeFSFileSystemSimpleModeTest extends LakeFSFileSystemTest {
    @Override
    void initConfiguration() {}

    @Override
    StagingLocation mockGetPhysicalAddress(String repo, String branch, String key,
            String physicalKey) throws ApiException {
        StagingLocation stagingLocation =
                new StagingLocation().token("foo").physicalAddress(s3Url("/" + physicalKey));
        when(stagingApi.getPhysicalAddress(repo, branch, key)).thenAnswer(invocation -> {
                StagingApi.APIgetPhysicalAddressRequest req = mock(StagingApi.APIgetPhysicalAddressRequest.class, Answers.RETURNS_SMART_NULLS);
                when(req.presign(any())).thenReturn(req);
                when(req.execute()).thenReturn(stagingLocation);
                verify(req.presign(false));
                return req;
            });
        return stagingLocation;
    }

    @Override
    void mockStatObject(String repo, String branch, String key, String physicalKey, long sizeBytes)
            throws ApiException {
        ObjectStats stats = new ObjectStats()
            .path("lakefs://" + repo + "/" + branch + "/" + key)
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress(s3Url(physicalKey))
            .sizeBytes((long) sizeBytes)
            .checksum(UNUSED_CHECKSUM).mtime(UNUSED_MTIME);

        when(objectsApi.statObject(repo, branch, key)).thenAnswer(invocation -> {
                ObjectsApi.APIstatObjectRequest req = mock(ObjectsApi.APIstatObjectRequest.class, Answers.RETURNS_SMART_NULLS);
                when(req.userMetadata(any())).thenReturn(req);
                when(req.presign(any())).thenReturn(req);
                when(req.execute()).thenReturn(stats);
                verify(req.userMetadata(false));
                verify(req.presign(false));
                return req;
            });
    }
}
