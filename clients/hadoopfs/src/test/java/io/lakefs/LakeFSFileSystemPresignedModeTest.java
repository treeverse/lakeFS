// package io.lakefs;

// import org.mockito.Answers;
// import org.mockito.stubbing.Answer;
// import org.mockito.invocation.InvocationOnMock;
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.Mockito.*;

// import java.net.URL;
// import java.util.Date;
// import java.util.concurrent.TimeUnit;
// import com.amazonaws.HttpMethod;
// import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
// import io.lakefs.clients.sdk.ApiException;
// import io.lakefs.clients.sdk.ObjectsApi;
// import io.lakefs.clients.sdk.StagingApi;
// import io.lakefs.clients.sdk.model.ObjectStats;
// import io.lakefs.clients.sdk.model.ObjectStats.PathTypeEnum;
// import io.lakefs.clients.sdk.model.StagingLocation;

// public class LakeFSFileSystemPresignedModeTest extends LakeFSFileSystemTest {
//     @Override
//     void initConfiguration() {
//         conf.set("fs.lakefs.access.mode", "presigned");
//     }

//     @Override
//     StagingLocation mockGetPhysicalAddress(String repo, String branch, String key,
//             String physicalKey) throws ApiException {
//         URL url =
//                 s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(s3Bucket, physicalKey)
//                         .withMethod(HttpMethod.PUT).withExpiration(
//                                 new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))));
//         StagingLocation stagingLocation = new StagingLocation().token("foo")
//                 .physicalAddress(s3Url("/" + physicalKey)).presignedUrl(url.toString());
//         when(stagingApi.getPhysicalAddress(repo, branch, key)).thenAnswer(new Answer() {
//                 public Object answer(InvocationOnMock invocation) throws ApiException {
//                     StagingApi.APIgetPhysicalAddressRequest req = mock(StagingApi.APIgetPhysicalAddressRequest.class, Answers.RETURNS_SMART_NULLS);
//                     when(req.presign(true)).thenReturn(req);
//                     when(req.execute()).thenReturn(stagingLocation);
//                     return req;
//                 }
//             });
//         return stagingLocation;
//     }

//     @Override
//     void mockStatObject(String repo, String branch, String key, String physicalKey, long sizeBytes)
//             throws ApiException {
//         Date expiration = new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
//         URL url =
//             s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(s3Bucket, physicalKey)
//                                           .withMethod(HttpMethod.GET)
//                                           .withExpiration(expiration));
//         ObjectStats stats = new ObjectStats()
//             .path("lakefs://" + repo + "/" + branch + "/" + key)
//             .pathType(PathTypeEnum.OBJECT)
//             .physicalAddress(url.toString())
//             .sizeBytes((long) sizeBytes)
//             .checksum(UNUSED_CHECKSUM).mtime(UNUSED_MTIME);
//         when(objectsApi.statObject(repo, branch, key)).thenAnswer(new Answer() {
//                 public Object answer(InvocationOnMock invocation) throws ApiException {
//                     ObjectsApi.APIstatObjectRequest req = mock(ObjectsApi.APIstatObjectRequest.class);
//                     when(req.userMetadata(false)).thenReturn(req);
//                     when(req.presign(true)).thenReturn(req);
//                     when(req.execute()).thenReturn(stats);
//                     return req;
//                 }
//             });
//     }
// }
