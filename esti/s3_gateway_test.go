package esti

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/go-openapi/swag"
	"github.com/thanhpk/randstr"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	gtwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type GetCredentials = func(id, secret, token string) *credentials.Credentials

const (
	numUploads           = 100
	randomDataPathLength = 1020
	gatewayTestPrefix    = mainBranch + "/data/"
)

func newMinioClient(t *testing.T, getCredentials GetCredentials) *minio.Client {
	t.Helper()
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	endpointSecure := viper.GetBool("s3_endpoint_secure")
	creds := getCredentials(accessKeyID, secretAccessKey, "")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: endpointSecure,
	})
	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}
	return client
}

func TestS3UploadToReadOnlyRepoError(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3uploadobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	minioClient := newMinioClient(t, credentials.NewStaticV4)
	const tenMibi = 10 * 1024 * 1024
	reader := NewZeroReader(tenMibi)

	_, err := minioClient.PutObject(ctx, readOnlyRepo, gatewayTestPrefix+"/test", reader, tenMibi, minio.PutObjectOptions{
		// this prevents minio from reading the entire file before sending the request
		SendContentMd5: false,
	})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "read-only")

	// The read-only check should occur before we read the file.
	// To ensure that, we're asserting that the file was not read entirely.
	// (The minio client reads at least one chunk of the file before sending the request,
	// so `NumBytesRead` is probably not 0, but must be < 10MB.)
	require.Less(t, reader.NumBytesRead, tenMibi)
}

func TestS3DeleteFromReadOnlyRepoError(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3deleteobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	minioClient := newMinioClient(t, credentials.NewStaticV4)
	content := "some random data"
	contentReader := strings.NewReader(content)

	path := gatewayTestPrefix + "test"
	_, uploadErr := client.UploadObjectWithBodyWithResponse(ctx, readOnlyRepo, mainBranch, &apigen.UploadObjectParams{
		Path:  path,
		Force: swag.Bool(true),
	}, "application/octet-stream", contentReader)
	require.Nil(t, uploadErr)

	t.Run("existing object", func(t *testing.T) {
		err := minioClient.RemoveObject(ctx, readOnlyRepo, path, minio.RemoveObjectOptions{})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "read-only")
	})
	t.Run("non existing object", func(t *testing.T) {
		err := minioClient.RemoveObject(ctx, readOnlyRepo, path+"not-existing", minio.RemoveObjectOptions{})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "read-only")
	})
}

func TestS3UploadAndDownload(t *testing.T) {
	const parallelism = 10

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	sigs := []struct {
		Name           string
		GetCredentials GetCredentials
	}{
		{Name: "V2", GetCredentials: credentials.NewStaticV2},
		{Name: "V4", GetCredentials: credentials.NewStaticV4},
	}

	for _, sig := range sigs {
		t.Run("Sig"+sig.Name, func(t *testing.T) {
			// Use the same sequence of path names to test each sig.
			r := rand.New(rand.NewSource(17))

			type Object struct {
				Path, Content string
			}

			var (
				wg      sync.WaitGroup
				objects = make(chan Object, parallelism*2)
			)

			client := newMinioClient(t, sig.GetCredentials)
			wg.Add(parallelism)
			for i := 0; i < parallelism; i++ {
				go func() {
					defer wg.Done()
					for o := range objects {
						_, err := client.PutObject(ctx, repo, o.Path, strings.NewReader(o.Content), int64(len(o.Content)), minio.PutObjectOptions{})
						if err != nil {
							t.Errorf("minio.Client.PutObject(%s): %s", o.Path, err)
							continue
						}

						download, err := client.GetObject(ctx, repo, o.Path, minio.GetObjectOptions{})
						if err != nil {
							t.Errorf("minio.Client.GetObject(%s): %s", o.Path, err)
							continue
						}
						contents := bytes.NewBuffer(nil)
						_, err = io.Copy(contents, download)
						if err != nil {
							t.Errorf("download %s: %s", o.Path, err)
							continue
						}
						if strings.Compare(contents.String(), o.Content) != 0 {
							t.Errorf("Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), o.Content)
						}
					}
				}()
			}

			for i := 0; i < numUploads; i++ {
				objects <- Object{
					Content: testutil.RandomString(r, randomDataContentLength),
					// lakeFS supports _any_ path, even if its
					// byte sequence is not legal UTF-8 string.
					Path: gatewayTestPrefix + testutil.RandomString(r, randomDataPathLength-len(gatewayTestPrefix)),
				}
			}
			close(objects)
			wg.Wait()
		})
	}
}

func TestMultipartUploadIfNoneMatch(t *testing.T) {
	ctx, log, repo := setupTest(t)
	defer tearDownTest(repo)
	s3Endpoint := viper.GetString("s3_endpoint")
	s3Client := createS3Client(s3Endpoint, t)
	testCases := []struct {
		Name          string
		Path          string
		IfNoneMatch   string
		ExpectedError string
	}{
		{
			Name: "sanity",
			Path: "main/object1",
		},
		{
			Name:          "object exists",
			Path:          "main/object1",
			IfNoneMatch:   "*",
			ExpectedError: gtwerrors.ErrPreconditionFailed.Error(),
		},
		{
			Name:        "object doesn't exist",
			Path:        "main/object2",
			IfNoneMatch: "*",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			input := &s3.CreateMultipartUploadInput{
				Bucket: aws.String(repo),
				Key:    aws.String(tt.Path),
			}

			resp, err := s3Client.CreateMultipartUpload(ctx, input)
			require.NoError(t, err, "failed to create multipart upload")

			parts := make([][]byte, multipartNumberOfParts)
			for i := 0; i < multipartNumberOfParts; i++ {
				parts[i] = randstr.Bytes(multipartPartSize + i)
			}

			completedParts := uploadMultipartParts(t, ctx, s3Client, log, resp, parts, 0)
			completeInput := &s3.CompleteMultipartUploadInput{
				Bucket:   resp.Bucket,
				Key:      resp.Key,
				UploadId: resp.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: completedParts,
				},
			}
			_, err = s3Client.CompleteMultipartUpload(ctx, completeInput, s3.WithAPIOptions(setIfNonMatchHeader(tt.IfNoneMatch)))
			if tt.ExpectedError != "" {
				require.ErrorContains(t, err, tt.ExpectedError)
			} else {
				require.NoError(t, err, "expected no error but got: %w", err)
			}
		})
	}
}

func TestS3IfNoneMatch(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	s3Endpoint := viper.GetString("s3_endpoint")
	s3Client := createS3Client(s3Endpoint, t)
	testCases := []struct {
		Name          string
		Path          string
		IfNoneMatch   string
		ExpectedError string
	}{
		{
			Name: "sanity",
			Path: "main/object1",
		},
		{
			Name:          "object exists",
			Path:          "main/object1",
			IfNoneMatch:   "*",
			ExpectedError: gtwerrors.ErrPreconditionFailed.Error(),
		},
		{
			Name:        "object doesn't exist",
			Path:        "main/object2",
			IfNoneMatch: "*",
		},
		{
			Name:          "unsupported value",
			Path:          "main/object3",
			IfNoneMatch:   "unsupported string",
			ExpectedError: gtwerrors.ErrNotImplemented.Error(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			input := &s3.PutObjectInput{
				Bucket: aws.String(repo),
				Key:    aws.String(tt.Path),
			}
			_, err := s3Client.PutObject(ctx, input, s3.WithAPIOptions(setIfNonMatchHeader(tt.IfNoneMatch)))
			if tt.ExpectedError != "" {
				require.ErrorContains(t, err, tt.ExpectedError)
			} else {
				require.NoError(t, err, "expected no error but got: %w", err)
			}
		})
	}
}

func setIfNonMatchHeader(ifNoneMatch string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("AddIfNoneMatchHeader", func(
			ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
		) (
			middleware.BuildOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.Header.Set("If-None-Match", ifNoneMatch)
			}
			return next.HandleBuild(ctx, in)
		}), middleware.Before)
	}
}

func TestListMultipartUploads(t *testing.T) {
	blockStoreType := viper.GetString(ViperBlockstoreType)
	if blockStoreType != "s3" {
		return
	}
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	s3Endpoint := viper.GetString("s3_endpoint")
	s3Client := createS3Client(s3Endpoint, t)
	multipartNumberOfParts := 3
	multipartPartSize := 5 * 1024 * 1024

	// create two objects for two mpus
	obj1 := "object1"
	obj2 := "object2"
	keysPrefix := "main/"
	key1 := keysPrefix + obj1
	key2 := keysPrefix + obj2

	input1 := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(key1),
	}
	input2 := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(key2),
	}
	// create first mpu
	resp1, err := s3Client.CreateMultipartUpload(ctx, input1)
	require.NoError(t, err, "failed to create multipart upload")
	parts := make([][]byte, multipartNumberOfParts)
	for i := 0; i < multipartNumberOfParts; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i)
	}

	completedParts1 := uploadMultipartParts(t, ctx, s3Client, logger, resp1, parts, 0)

	completeInput1 := &s3.CompleteMultipartUploadInput{
		Bucket:   resp1.Bucket,
		Key:      resp1.Key,
		UploadId: resp1.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts1,
		},
	}
	// check first mpu appears
	output, err := s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: resp1.Bucket})
	require.NoError(t, err, "error listing multiparts")
	keys := extractUploadKeys(output)
	require.Contains(t, keys, obj1)

	// create second mpu check both appear
	_, err = s3Client.CreateMultipartUpload(ctx, input2)
	require.NoError(t, err, "failed to create multipart upload")
	output, err = s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: resp1.Bucket})
	keys = extractUploadKeys(output)
	require.Contains(t, keys, obj1)
	require.Contains(t, keys, obj2)

	// testing maxuploads - only first upload should return
	maxUploads := aws.Int32(1)
	output, err = s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: resp1.Bucket, MaxUploads: maxUploads})
	require.NoError(t, err, "failed to list multipart uploads")
	keys = extractUploadKeys(output)
	require.Contains(t, keys, obj1)
	require.NotContains(t, keys, obj2)

	// testing key marker and upload id marker for pagination. only records after marker should return
	keyMarker := output.NextKeyMarker
	uploadIDMarker := output.NextUploadIdMarker
	output, err = s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: resp1.Bucket, MaxUploads: maxUploads, KeyMarker: keyMarker, UploadIdMarker: uploadIDMarker})
	require.NoError(t, err, "failed to list multipart uploads")
	keys = extractUploadKeys(output)
	require.NotContains(t, keys, obj1)
	require.Contains(t, keys, obj2)

	// finish first mpu check only second appear
	_, err = s3Client.CompleteMultipartUpload(ctx, completeInput1)
	require.NoError(t, err, "failed to complete multipart upload")
	output, err = s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: resp1.Bucket})
	require.NoError(t, err, "error listing multiparts")
	keys = extractUploadKeys(output)
	require.NotContains(t, keys, obj1)
	require.Contains(t, keys, obj2)

}

func TestListMultipartUploadsUnsupported(t *testing.T) {
	blockStoreType := viper.GetString(ViperBlockstoreType)
	if blockStoreType != "s3" {
		return
	}
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	s3Endpoint := viper.GetString("s3_endpoint")
	s3Client := createS3Client(s3Endpoint, t)
	Bucket := aws.String(repo)

	delimiter := aws.String("/")
	prefix := aws.String("prefix")
	encodingType := types.EncodingTypeUrl

	t.Run("Delimiter", func(t *testing.T) {
		_, err := s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: Bucket, Delimiter: delimiter})
		require.Error(t, err)
		require.Contains(t, err.Error(), "NotImplemented")
	})

	t.Run("Prefix", func(t *testing.T) {
		_, err := s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: Bucket, Prefix: prefix})
		require.Error(t, err)
		require.Contains(t, err.Error(), "NotImplemented")
	})

	t.Run("EncodingType", func(t *testing.T) {
		_, err := s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: Bucket, EncodingType: encodingType})
		require.Error(t, err)
		require.Contains(t, err.Error(), "NotImplemented")
	})
}

func extractUploadKeys(output *s3.ListMultipartUploadsOutput) []string {
	if output == nil {
		return nil
	}
	keys := make([]string, 0, len(output.Uploads))
	for _, upload := range output.Uploads {
		if upload.Key != nil {
			keys = append(keys, *upload.Key)
		}
	}
	return keys
}

func verifyObjectInfo(t *testing.T, got minio.ObjectInfo, expectedSize int) {
	if got.Err != nil {
		t.Errorf("%s: %s", got.Key, got.Err)
	}
	if got.Size != int64(expectedSize) {
		t.Errorf("Got size %d != expected size %d", got.Size, int64(expectedSize))
	}
}

func TestS3ReadObject(t *testing.T) {
	const (
		contents = "the quick brown fox jumps over the lazy dog"
		goodPath = "main/exists"
		badPath  = "main/does/not/exist"
	)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// Upload an object
	minioClient := newMinioClient(t, credentials.NewStaticV2)

	_, err := minioClient.PutObject(ctx, repo, goodPath, strings.NewReader(contents), int64(len(contents)), minio.PutObjectOptions{})
	if err != nil {
		t.Errorf("PutObject(%s, %s): %s", repo, goodPath, err)
	}

	t.Run("get_exists", func(t *testing.T) {
		res, err := minioClient.GetObject(ctx, repo, goodPath, minio.GetObjectOptions{})
		if err != nil {
			t.Errorf("GetObject(%s, %s): %s", repo, goodPath, err)
		}
		defer func() { _ = res.Close() }()
		info, err := res.Stat()
		if err != nil {
			t.Errorf("GetObject(%s, %s) get: %s", repo, goodPath, err)
		}
		verifyObjectInfo(t, info, len(contents))
		got, err := io.ReadAll(res)
		if err != nil {
			t.Errorf("Read: %s", err)
		}
		if string(got) != contents {
			t.Errorf("Got contents \"%s\" but expected \"%s\"", string(got), contents)
		}
	})

	t.Run("get_exists_presigned", func(t *testing.T) {
		// using presigned URL, so we can check the headers
		// We expect the Content-Length header to be set
		preSignedURL, err := minioClient.Presign(ctx, http.MethodGet, repo, goodPath, time.Second*60, url.Values{})
		if err != nil {
			t.Fatalf("minioClientPresign(%s, %s): %s", repo, goodPath, err)
		}

		resp, err := http.Get(preSignedURL.String())
		if err != nil {
			t.Fatalf("http.Get %s: %s", preSignedURL.String(), err)
		}
		defer func() { _ = resp.Body.Close() }()
		contentLength := resp.Header.Get("Content-Length")
		if contentLength == "" {
			t.Errorf("Expected Content-Length header to be set")
		}
	})

	t.Run("get_no_physical_object", func(t *testing.T) {
		blockStoreType := viper.GetString(ViperBlockstoreType)
		if blockStoreType != "s3" {
			t.Skip("Skipping test - blockstore type is not s3")
		}
		awsAccessKeyID := viper.GetString("aws_access_key_id")
		awsSecretAccessKey := viper.GetString("aws_secret_access_key")
		if awsAccessKeyID == "" || awsSecretAccessKey == "" {
			t.Skip("Skipping test - AWS credentials not set")
		}

		// prepare entry with no physical object under it
		const (
			objPath   = "no-physical-object"
			objS3Path = "main/" + objPath
		)
		_, err = minioClient.PutObject(ctx, repo, objS3Path, strings.NewReader(""), int64(0), minio.PutObjectOptions{})
		if err != nil {
			t.Errorf("PutObject(%s, %s): %s", repo, objS3Path, err)
		}
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{
			Path:    objPath,
			Presign: apiutil.Ptr(false),
		})
		if err != nil {
			t.Fatalf("StatObject(%s, %s): %s", repo, objPath, err)
		}
		if statResp.JSON200 == nil {
			t.Fatalf("StatObject(%s, %s): got nil response, status: %s", repo, objPath, statResp.Status())
		}
		physicalAddress := statResp.JSON200.PhysicalAddress

		// setup s3 client and delete the physical object
		s3BlockstoreClient, err := minio.New("s3.amazonaws.com", &minio.Options{
			Creds:  credentials.NewStaticV4(awsAccessKeyID, awsSecretAccessKey, ""),
			Secure: true,
		})
		if err != nil {
			t.Fatalf("failed to create s3 blockstore client: %s", err)
		}

		// access the underlying storage directly to delete the object
		s3PhysicalURL, err := url.Parse(physicalAddress)
		if err != nil {
			t.Fatalf("failed to parse physical address %s, %v", physicalAddress, err)
		}
		if s3PhysicalURL.Scheme != "s3" {
			t.Fatalf("physical address %s is not an s3 address", physicalAddress)
		}
		s3PhysicalKey := strings.TrimLeft(s3PhysicalURL.Path, "/")
		err = s3BlockstoreClient.RemoveObject(ctx, s3PhysicalURL.Host, s3PhysicalKey, minio.RemoveObjectOptions{})
		if err != nil {
			t.Fatalf("RemoveObject(%s, %s): %s", repo, objPath, err)
		}

		// try to read the object - should fail
		const s3ObjPath = "main/" + objPath
		res, err := minioClient.GetObject(ctx, repo, s3ObjPath, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("GetObject(%s, %s): %s", repo, s3ObjPath, err)
		}
		defer func() { _ = res.Close() }()
		got, err := io.ReadAll(res)
		if err == nil {
			t.Fatalf("Successfully read \"%s\" from nonexistent path %s", got, s3ObjPath)
		}
		s3ErrorResponse := minio.ToErrorResponse(err)
		expectedErrorCode := gtwerrors.Codes[gtwerrors.ErrNoSuchVersion].Code
		if s3ErrorResponse.Code != expectedErrorCode {
			t.Errorf("Got %+v [%d] on reading when expecting code %s", s3ErrorResponse, s3ErrorResponse.StatusCode, expectedErrorCode)
		}
	})

	t.Run("get_not_exists", func(t *testing.T) {
		res, err := minioClient.GetObject(ctx, repo, badPath, minio.GetObjectOptions{})
		if err != nil {
			t.Errorf("GetObject(%s, %s): %s", repo, badPath, err)
		}
		defer func() { _ = res.Close() }()
		got, err := io.ReadAll(res)
		if err == nil {
			t.Errorf("Successfully read \"%s\" from nonexistent path %s", got, badPath)
		}
		s3ErrorResponse := minio.ToErrorResponse(err)
		if s3ErrorResponse.StatusCode != 404 {
			t.Errorf("Got %+v [%d] on reading when expecting Not Found [404]",
				s3ErrorResponse, s3ErrorResponse.StatusCode)
		}
	})

	t.Run("head_exists", func(t *testing.T) {
		info, err := minioClient.StatObject(ctx, repo, goodPath, minio.StatObjectOptions{})
		if err != nil {
			t.Errorf("StatObject(%s, %s): %s", repo, goodPath, err)
		}
		verifyObjectInfo(t, info, len(contents))
	})

	t.Run("head_not_exists", func(t *testing.T) {
		info, err := minioClient.StatObject(ctx, repo, badPath, minio.StatObjectOptions{})
		if err == nil {
			t.Errorf("StatObject(%s, %s): expected an error but got %+v", repo, badPath, info)
		}

		s3ErrorResponse := minio.ToErrorResponse(err)
		if s3ErrorResponse.StatusCode != 404 {
			t.Errorf("Got %+v [%d] on reading when expecting Not Found [404]",
				s3ErrorResponse, s3ErrorResponse.StatusCode)
		}
	})
}

func TestS3HeadBucket(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	badRepo := repo + "-nonexistent"

	// Upload an object
	client := newMinioClient(t, credentials.NewStaticV2)

	t.Run("existing", func(t *testing.T) {
		ok, err := client.BucketExists(ctx, repo)
		if err != nil {
			t.Errorf("BucketExists(%s) failed: %s", repo, err)
		}
		if !ok {
			t.Errorf("Got that good bucket %s does not exist", repo)
		}
	})

	t.Run("not existing", func(t *testing.T) {
		ok, err := client.BucketExists(ctx, badRepo)
		if err != nil {
			t.Errorf("BucketExists(%s) failed: %s", badRepo, err)
		}
		if ok {
			t.Errorf("Got that bad bucket %s exists", badRepo)
		}
	})
}

// getOrCreatePathToLargeObject returns a configured existing large
// (largeDataContentLength, 6MiB) object, or creates a new one under
// testPrefix.
func getOrCreatePathToLargeObject(t *testing.T, ctx context.Context, s3lakefsClient *minio.Client, repo, branch string) (string, int64) {
	t.Helper()

	path := "source-file"
	s3Path := fmt.Sprintf("%s/source-file", branch)

	if physicalAddress := viper.GetString("large_object_path"); physicalAddress != "" {
		res, err := client.LinkPhysicalAddressWithResponse(ctx, repo, branch, &apigen.LinkPhysicalAddressParams{Path: path}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum: "dont-care",
			// TODO(ariels): Check actual length of object!
			SizeBytes: largeDataContentLength,
			Staging: apigen.StagingLocation{
				PhysicalAddress: &physicalAddress,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.JSON200)
		return s3Path, largeDataContentLength
	}

	// content
	r := rand.New(rand.NewSource(17))
	objContent := testutil.NewRandomReader(r, largeDataContentLength)

	// upload data
	_, err := s3lakefsClient.PutObject(ctx, repo, s3Path, objContent, largeDataContentLength,
		minio.PutObjectOptions{})
	require.NoError(t, err)
	return s3Path, largeDataContentLength
}

func TestS3CopyObjectMultipart(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// additional repository for copy between repos
	const destRepoName = "tests3copyobjectmultipartdest"
	destRepo := createRepositoryByName(ctx, t, destRepoName)
	defer DeleteRepositoryIfAskedTo(ctx, destRepoName)

	s3lakefsClient := newMinioClient(t, credentials.NewStaticV4)

	srcPath, objectLength := getOrCreatePathToLargeObject(t, ctx, s3lakefsClient, repo, mainBranch)
	destPath := gatewayTestPrefix + "dest-file"

	dest := minio.CopyDestOptions{
		Bucket: destRepo,
		Object: destPath,
	}

	srcs := []minio.CopySrcOptions{
		{
			Bucket:     repo,
			Object:     srcPath,
			MatchRange: true,
			Start:      0,
			End:        minDataContentLengthForMultipart - 1,
		}, {
			Bucket:     repo,
			Object:     srcPath,
			MatchRange: true,
			Start:      minDataContentLengthForMultipart,
			End:        objectLength - 1,
		},
	}

	ui, err := s3lakefsClient.ComposeObject(ctx, dest, srcs...)
	if err != nil {
		t.Fatalf("minio.Client.ComposeObject from(%+v) to(%+v): %s", srcs, dest, err)
	}

	if ui.Size != objectLength {
		t.Errorf("Copied %d bytes when expecting %d", ui.Size, objectLength)
	}

	// Comparing 2 readers is too much work.  Instead just hash them.
	// This will fail for malicious bad S3 gateways, but otherwise is
	// fine.
	uploadedReader, err := s3lakefsClient.GetObject(ctx, repo, srcPath, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("Get uploaded object: %s", err)
	}
	defer uploadedReader.Close()
	uploadedCRC, err := testutil.ChecksumReader(uploadedReader)
	if err != nil {
		t.Fatalf("Read uploaded object: %s", err)
	}
	if uploadedCRC == 0 {
		t.Fatal("Impossibly bad luck: uploaded data with CRC64 == 0!")
	}

	copiedReader, err := s3lakefsClient.GetObject(ctx, repo, srcPath, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("Get copied object: %s", err)
	}
	defer copiedReader.Close()
	copiedCRC, err := testutil.ChecksumReader(copiedReader)
	if err != nil {
		t.Fatalf("Read copied object: %s", err)
	}

	if uploadedCRC != copiedCRC {
		t.Fatal("Copy not equal")
	}
}

func TestS3CopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// additional repository for copy between repos
	const destRepoName = "tests3copyobjectdest"
	destRepo := createRepositoryByName(ctx, t, destRepoName)
	defer DeleteRepositoryIfAskedTo(ctx, destRepoName)

	// content
	r := rand.New(rand.NewSource(17))
	objContent := testutil.RandomString(r, randomDataContentLength)
	srcPath := gatewayTestPrefix + "source-file"
	destPath := gatewayTestPrefix + "dest-file"
	userMetadata := map[string]string{"X-Amz-Meta-Key1": "value1", "X-Amz-Meta-Key2": "value2"}

	// upload data
	s3lakefsClient := newMinioClient(t, credentials.NewStaticV2)
	_, err := s3lakefsClient.PutObject(ctx, repo, srcPath, strings.NewReader(objContent), int64(len(objContent)),
		minio.PutObjectOptions{
			UserMetadata: userMetadata,
		})
	require.NoError(t, err)

	t.Run("same_branch", func(t *testing.T) {
		// copy the object to the same repository
		_, err = s3lakefsClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket: repo,
				Object: destPath,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: srcPath,
			})
		if err != nil {
			t.Fatalf("minio.Client.CopyObject from(%s) to(%s): %s", srcPath, destPath, err)
		}

		download, err := s3lakefsClient.GetObject(ctx, repo, destPath, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("minio.Client.GetObject(%s): %s", destPath, err)
		}
		defer func() { _ = download.Close() }()

		// compere files content
		var content bytes.Buffer
		_, err = io.Copy(&content, download)
		if err != nil {
			t.Fatalf("Download '%s' failed: %s", destPath, err)
		}
		require.Equal(t, objContent, content.String())

		resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: "data/source-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: "data/dest-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		// assert that the physical addresses of the objects are the same
		sourceObjectStats := resp.JSON200
		destObjectStats := resp.JSON200
		require.Equal(t, sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress, "source and dest physical address should match")
		require.Equal(t, userMetadata, destObjectStats.Metadata.AdditionalProperties, "source and dest metadata should match")
	})

	t.Run("different_repo", func(t *testing.T) {
		// copy the object to different repository. should create another version of the file
		userMetadataReplace := map[string]string{"X-Amz-Meta-Key1": "value1Replace", "X-Amz-Meta-Key2": "value2Replace"}

		_, err := s3lakefsClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket:          destRepo,
				Object:          destPath,
				UserMetadata:    userMetadataReplace,
				ReplaceMetadata: true,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: srcPath,
			})
		if err != nil {
			t.Fatalf("minio.Client.CopyObjectFrom(%s)To(%s): %s", srcPath, destPath, err)
		}

		download, err := s3lakefsClient.GetObject(ctx, destRepo, destPath, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("minio.Client.GetObject(%s): %s", destPath, err)
		}
		defer func() { _ = download.Close() }()
		var contents bytes.Buffer
		_, err = io.Copy(&contents, download)
		require.NoError(t, err)

		// compere files content
		require.Equal(t, contents.String(), objContent)

		resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: "data/source-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		sourceObjectStats := resp.JSON200

		resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &apigen.StatObjectParams{Path: "data/dest-file"})
		if err != nil {
			t.Fatalf("client.StatObject(%s): %s", destPath, err)
		}
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		destObjectStats := resp.JSON200

		// assert that the physical addresses of the objects are not the same
		require.NotEqual(t, sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
		require.Equal(t, userMetadataReplace, destObjectStats.Metadata.AdditionalProperties, "dest metadata should be replaced")
	})
}

func TestS3PutObjectTagging(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	srcPath := gatewayTestPrefix + "source-file"
	s3lakefsClient := newMinioClient(t, credentials.NewStaticV2)

	tag, err := tags.NewTags(map[string]string{"tag1": "value1"}, true)
	require.NoError(t, err)

	err = s3lakefsClient.PutObjectTagging(ctx, repo, srcPath, tag, minio.PutObjectTaggingOptions{})
	require.Error(t, err)

	errResponse := minio.ToErrorResponse(err)
	require.Equal(t, "ERRLakeFSNotSupported", errResponse.Code)
	require.Equal(t, "This operation is not supported in LakeFS", errResponse.Message)
}

func TestS3CopyObjectErrors(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3copyobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	RequireBlockstoreType(t, block.BlockstoreTypeS3)
	destPath := gatewayTestPrefix + "dest-file"

	// upload data
	s3lakefsClient := newMinioClient(t, credentials.NewStaticV2)

	t.Run("malformed dest", func(t *testing.T) {
		// copy the object to a non-existent repo - tests internal lakeFS error
		_, err := s3lakefsClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket: "wrong-repo",
				Object: destPath,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: "main/data/not-found",
			})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "The specified bucket does not exist")
	})

	t.Run("source not found", func(t *testing.T) {
		// Create object in lakeFS with wrong physical address
		getResp, err := client.GetPhysicalAddressWithResponse(ctx, repo, mainBranch, &apigen.GetPhysicalAddressParams{
			Path: "data/not-found",
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.JSON200)
		linkResp, err := client.LinkPhysicalAddressWithResponse(ctx, repo, mainBranch, &apigen.LinkPhysicalAddressParams{
			Path: "data/not-found",
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "12345",
			SizeBytes: 10,
			Staging: apigen.StagingLocation{
				PhysicalAddress: getResp.JSON200.PhysicalAddress,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, linkResp.JSON200)

		// copy the object to the same repository
		_, err = s3lakefsClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket: repo,
				Object: destPath,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: "main/data/not-found",
			})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "NoSuchKey")
	})

	t.Run("readonly repo from non-existing source", func(t *testing.T) {
		_, err := s3lakefsClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket: readOnlyRepo,
				Object: destPath,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: "not-a-branch/data/not-found",
			})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "read-only")
	})
}

func TestS3ReadObjectRedirect(t *testing.T) {
	RequireBlockstoreType(t, block.BlockstoreTypeS3)
	const (
		contents = "the quick brown fox jumps over the lazy dog"
		goodPath = "main/exists.txt"
	)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// Upload an object
	minioClient := newMinioClient(t, credentials.NewStaticV4)

	_, err := minioClient.PutObject(ctx, repo, goodPath, strings.NewReader(contents), int64(len(contents)), minio.PutObjectOptions{})
	if err != nil {
		t.Errorf("PutObject(%s, %s): %s", repo, goodPath, err)
	}

	t.Run("get_exists", func(t *testing.T) {
		opts := minio.GetObjectOptions{}
		opts.Set("User-Agent", "client with s3RedirectionSupport set")
		res, err := minioClient.GetObject(ctx, repo, goodPath, opts)
		require.NoError(t, err)

		// Verify we got redirect
		_, err = io.ReadAll(res)
		require.Contains(t, err.Error(), "307 Temporary Redirect")
	})
}

func createS3Client(endpoint string, t *testing.T) *s3.Client {
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	s3Client, err := testutil.SetupTestS3Client(endpoint, accessKeyID, secretAccessKey, true)
	require.NoError(t, err, "failed creating s3 client")
	return s3Client
}

func TestPossibleAPIEndpointError(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	t.Run("use_open_api_for_client_endpoint", func(t *testing.T) {
		s3Client := createS3Client(endpointURL+apiutil.BaseURL, t)
		_, listErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("not-exists")})
		require.ErrorContains(t, listErr, gtwerrors.ErrNoSuchBucketPossibleAPIEndpoint.Error())
	})

	t.Run("use_proper_client_endpoint", func(t *testing.T) {
		s3Client := createS3Client(endpointURL, t)
		_, listErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("not-exists")})
		require.ErrorContains(t, listErr, gtwerrors.ErrNoSuchBucket.Error())
	})
}
