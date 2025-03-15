package esti

import (
	"bytes"
	"context"
	randread "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/go-openapi/swag"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
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
	branch               = "main"
	gatewayTestPrefix    = branch + "/data/"
)

func newMinioClient(t *testing.T, getCredentials GetCredentials) *minio.Client {
	t.Helper()
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	endpointSecure := viper.GetBool("s3_endpoint_secure")
	creds := getCredentials(accessKeyID, secretAccessKey, "")

	clt, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: endpointSecure,
	})
	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}
	return clt
}

func setHttpHeader(key, value string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("IDoEVaultGrant", func(
			ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
		) (
			middleware.BuildOutput, middleware.Metadata, error,
		) {
			switch v := in.Request.(type) {
			case *smithyhttp.Request:
				v.Header.Add(key, value)
			}
			return next.HandleBuild(ctx, in)
		}), middleware.Before)
	}
}

func createS3Client(endpoint string, t *testing.T, opts ...func(*s3.Options)) *s3.Client {
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	s3Client, err := testutil.SetupTestS3Client(endpoint, accessKeyID, secretAccessKey, true, opts...)
	require.NoError(t, err, "failed creating s3 client")
	return s3Client
}

func TestS3UploadToReadOnlyRepoError(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3uploadobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	const tenMibi = 10 * 1024 * 1024
	reader := NewZeroReader(tenMibi)

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &readOnlyRepo,
		Key:           aws.String(gatewayTestPrefix + "test"),
		Body:          reader,
		ContentLength: aws.Int64(tenMibi),
	})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "read-only")

	// The read-only check should occur before we read the file.
	// To ensure that, we're asserting that the file was not read entirely.
	// (The minio client reads at least one chunk of the file before sending the request,
	// so `NumBytesRead` is probably not 0, but must be < 10MB.)
	require.Less(t, reader.NumBytesRead, int64(tenMibi))
}

func TestS3DeleteFromReadOnlyRepoError(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3deleteobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	content := "some random data"
	contentReader := strings.NewReader(content)

	path := gatewayTestPrefix + "test"
	_, uploadErr := client.UploadObjectWithBodyWithResponse(ctx, readOnlyRepo, mainBranch, &apigen.UploadObjectParams{
		Path:  path,
		Force: swag.Bool(true),
	}, "application/octet-stream", contentReader)
	require.Nil(t, uploadErr)

	t.Run("existing object", func(t *testing.T) {
		_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(readOnlyRepo),
			Key:    aws.String(path),
		})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "read-only")
	})
	t.Run("non existing object", func(t *testing.T) {
		_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(readOnlyRepo),
			Key:    aws.String(path + "not-existing"),
		})
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

			// Using a minio client only to cover the V2 signing scenario. As V2 signing is already deprecated in AWS it is no longer
			// supported in its SDK. We should consider removing this test case altogether at some point
			clt := newMinioClient(t, sig.GetCredentials)
			wg.Add(parallelism)
			for i := 0; i < parallelism; i++ {
				go func() {
					defer wg.Done()
					for o := range objects {
						_, err := clt.PutObject(ctx, repo, o.Path, strings.NewReader(o.Content), int64(len(o.Content)), minio.PutObjectOptions{})
						if err != nil {
							t.Errorf("minio.Client.PutObject(%s): %s", o.Path, err)
							continue
						}

						download, err := clt.GetObject(ctx, repo, o.Path, minio.GetObjectOptions{})
						if err != nil {
							t.Errorf("GetObject(%s): %s", o.Path, err)
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

func TestS3ReadObject(t *testing.T) {
	const (
		contents = "the quick brown fox jumps over the lazy dog"
		goodPath = "main/exists"
		badPath  = "main/does/not/exist"
	)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// Upload an object
	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(repo),
		Key:           aws.String(goodPath),
		Body:          strings.NewReader(contents),
		ContentLength: aws.Int64(int64(len(contents))),
	})
	require.NoError(t, err)

	t.Run("get_exists", func(t *testing.T) {
		res, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(goodPath),
		})
		require.NoError(t, err)
		defer func() { _ = res.Body.Close() }()
		got, err := io.ReadAll(res.Body)
		require.NoError(t, err, "failure reading object")
		require.Equal(t, contents, string(got), "contents mismatch")
	})

	t.Run("get_exists_presigned", func(t *testing.T) {
		// using presigned URL, so we can check the headers
		// We expect the Content-Length header to be set
		presignClient := s3.NewPresignClient(svc)
		preSignedURL, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(goodPath),
		})
		require.NoError(t, err)

		resp, err := http.Get(preSignedURL.URL)
		require.NoError(t, err)
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
		_, err = svc.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(repo),
			Key:           aws.String(objS3Path),
			Body:          strings.NewReader(""),
			ContentLength: aws.Int64(0),
		})
		require.NoError(t, err)
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{
			Path:    objPath,
			Presign: apiutil.Ptr(false),
		})
		require.NoError(t, err)
		require.NotNil(t, statResp.JSON200)
		physicalAddress := statResp.JSON200.PhysicalAddress

		// setup s3 client and delete the physical object
		s3BlockstoreClient, err := testutil.SetupTestS3Client("s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, true)
		require.NoError(t, err)

		// access the underlying storage directly to delete the object
		s3PhysicalURL, err := url.Parse(physicalAddress)
		require.NoError(t, err, "failed to parse physical address %s, %v", physicalAddress, err)
		require.Equal(t, "s3", s3PhysicalURL.Scheme)
		s3PhysicalKey := strings.TrimLeft(s3PhysicalURL.Path, "/")
		_, err = s3BlockstoreClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s3PhysicalURL.Host),
			Key:    aws.String(s3PhysicalKey),
		})
		require.NoError(t, err)

		// try to read the object - should fail
		const s3ObjPath = "main/" + objPath
		_, err = svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(s3ObjPath),
		})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "NoSuchVersion")
	})

	t.Run("get_not_exists", func(t *testing.T) {
		_, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(badPath),
		})
		var ae smithy.APIError
		require.ErrorAs(t, err, &ae)
		require.Equal(t, "NoSuchKey", ae.ErrorCode())
	})

	t.Run("head_exists", func(t *testing.T) {
		info, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(goodPath),
		})
		require.NoError(t, err)
		require.Equal(t, int64(len(contents)), *info.ContentLength)
	})

	t.Run("head_not_exists", func(t *testing.T) {
		_, err = svc.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(badPath),
		})
		var ae smithy.APIError
		require.ErrorAs(t, err, &ae)
		require.Equal(t, "NotFound", ae.ErrorCode())
	})
}

func TestS3HeadBucket(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	badRepo := repo + "-nonexistent"

	// Upload an object
	t.Run("existing", func(t *testing.T) {
		_, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(repo),
		})
		require.NoError(t, err)
	})

	t.Run("not existing", func(t *testing.T) {
		_, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(badRepo),
		})
		require.Contains(t, err.Error(), "NotFound")
	})
}

// getOrCreatePathToLargeObject returns a configured existing large
// (largeDataContentLength, 6MiB) object, or creates a new one under
// testPrefix.
func getOrCreatePathToLargeObject(t *testing.T, ctx context.Context, s3lakefsClient *s3.Client, repo, branch string) (string, int64) {
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
		return s3Path, *res.JSON200.SizeBytes
	}

	// content
	token := make([]byte, largeDataContentLength)
	_, err := randread.Read(token)
	require.NoError(t, err)
	objContent := bytes.NewReader(token)

	// upload data
	_, err = s3lakefsClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(repo),
		Key:           aws.String(s3Path),
		Body:          objContent,
		ContentLength: aws.Int64(largeDataContentLength),
	})
	require.NoError(t, err)
	return s3Path, largeDataContentLength
}

func TestS3CopyObjectMultipart(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// additional repository for copy between repos
	destRepo := createRepositoryUnique(ctx, t)
	defer DeleteRepositoryIfAskedTo(ctx, destRepo)

	srcPath, objectLength := getOrCreatePathToLargeObject(t, ctx, svc, repo, branch)
	destPath := gatewayTestPrefix + "dest-file"

	mpu, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destRepo),
		Key:    aws.String(destPath),
	})
	require.NoError(t, err)

	var parts []types.CompletedPart
	part, err := svc.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:          aws.String(destRepo),
		Key:             aws.String(destPath),
		CopySource:      aws.String(repo + "/" + srcPath),
		PartNumber:      aws.Int32(1),
		UploadId:        mpu.UploadId,
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", 0, minDataContentLengthForMultipart-1)),
	})
	require.NoError(t, err)
	parts = append(parts, types.CompletedPart{
		PartNumber: aws.Int32(1),
		ETag:       part.CopyPartResult.ETag,
	})
	part, err = svc.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:          aws.String(destRepo),
		Key:             aws.String(destPath),
		CopySource:      aws.String(repo + "/" + srcPath),
		PartNumber:      aws.Int32(2),
		UploadId:        mpu.UploadId,
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", minDataContentLengthForMultipart, objectLength-1)),
	})
	require.NoError(t, err)
	parts = append(parts, types.CompletedPart{
		PartNumber: aws.Int32(2),
		ETag:       part.CopyPartResult.ETag,
	})
	_, err = svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(destRepo),
		Key:             aws.String(destPath),
		UploadId:        mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	require.NoError(t, err)

	// Comparing 2 readers is too much work. Instead, just hash them.
	// This will fail for malicious bad S3 gateways, but otherwise is fine.
	uploadedReader, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(repo),
		Key:    aws.String(srcPath),
	})
	require.NoError(t, err)
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(uploadedReader.Body)
	expectedContent := make([]byte, objectLength)
	_, err = uploadedReader.Body.Read(expectedContent)
	require.NoError(t, err)
	copiedReader, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destRepo),
		Key:    aws.String(destPath),
	})
	require.NoError(t, err)
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(copiedReader.Body)
	actualContent := make([]byte, objectLength)
	_, err = copiedReader.Body.Read(actualContent)
	require.NoError(t, err)
	require.Equal(t, expectedContent, actualContent)
}

func verifyMetadata(t testing.TB, expected, actual map[string]string) {
	require.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		actualKey := "X-Amz-Meta-" + k
		actualValue, ok := actual[actualKey]
		require.True(t, ok, "missing key %v", actualKey)
		require.Equal(t, v, actualValue)
	}
}

func TestS3CopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// additional repository for copy between repos
	destRepo := createRepositoryUnique(ctx, t)
	defer DeleteRepositoryIfAskedTo(ctx, destRepo)

	// content
	r := rand.New(rand.NewSource(17))
	objContent := testutil.RandomString(r, randomDataContentLength)
	srcPath := gatewayTestPrefix + "source-file"
	destPath := gatewayTestPrefix + "dest-file"
	userMetadata := map[string]string{"Key1": "value1", "Key2": "value2"}

	// upload data
	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(repo),
		Key:           aws.String(srcPath),
		Body:          strings.NewReader(objContent),
		ContentLength: aws.Int64(int64(len(objContent))),
		Metadata:      userMetadata,
	})
	require.NoError(t, err)

	t.Run("same_branch", func(t *testing.T) {
		// copy the object to the same repository
		_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(repo),
			CopySource: aws.String(repo + "/" + srcPath),
			Key:        aws.String(destPath),
		})
		require.NoError(t, err)

		download, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(destPath),
		})
		require.NoError(t, err)
		defer func() { _ = download.Body.Close() }()

		// compere files content
		var content bytes.Buffer
		_, err = io.Copy(&content, download.Body)
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
		verifyMetadata(t, userMetadata, destObjectStats.Metadata.AdditionalProperties)
	})

	t.Run("different_repo", func(t *testing.T) {
		// copy the object to different repository. should create another version of the file
		userMetadataReplace := map[string]string{"Key1": "value1Replace", "Key2": "value2Replace"}

		_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(destRepo),
			CopySource:        aws.String(repo + "/" + srcPath),
			Key:               aws.String(destPath),
			Metadata:          userMetadataReplace,
			MetadataDirective: "REPLACE",
		})
		require.NoError(t, err)

		download, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(destRepo),
			Key:    aws.String(destPath),
		})
		require.NoError(t, err)
		defer func() { _ = download.Body.Close() }()
		var contents bytes.Buffer
		_, err = io.Copy(&contents, download.Body)
		require.NoError(t, err)

		// compere files content
		require.Equal(t, contents.String(), objContent)

		resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &apigen.StatObjectParams{Path: "data/source-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		sourceObjectStats := resp.JSON200

		resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &apigen.StatObjectParams{Path: "data/dest-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		destObjectStats := resp.JSON200

		// assert that the physical addresses of the objects are not the same
		require.NotEqual(t, sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)

		verifyMetadata(t, userMetadataReplace, destObjectStats.Metadata.AdditionalProperties)
	})
}

func TestS3PutObjectTagging(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	srcPath := gatewayTestPrefix + "source-file"

	_, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  aws.String(repo),
		Key:     aws.String(srcPath),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("tag1"), Value: aws.String("value1")}}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERRLakeFSNotSupported: This operation is not supported in LakeFS")
}

func TestS3CopyObjectErrors(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	readOnlyRepo := createReadOnlyRepositoryByName(ctx, t, "tests3copyobjectdestreadonly")
	defer DeleteRepositoryIfAskedTo(ctx, readOnlyRepo)

	RequireBlockstoreType(t, block.BlockstoreTypeS3)
	destPath := gatewayTestPrefix + "dest-file"

	// upload data\
	t.Run("malformed dest", func(t *testing.T) {
		// copy the object to a non-existent repo - tests internal lakeFS error
		_, err := svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String("wrong-repo"),
			CopySource: aws.String(repo + "/main/data/not-found"),
			Key:        aws.String(destPath),
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
		_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(repo),
			CopySource: aws.String(repo + "/main/data/not-found"),
			Key:        aws.String(destPath),
		})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "NoSuchKey")
	})

	t.Run("readonly repo from non-existing source", func(t *testing.T) {
		_, err := svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(readOnlyRepo),
			CopySource: aws.String(repo + "/not-a-branch/data/not-found"),
			Key:        aws.String(destPath),
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
	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(repo),
		Key:           aws.String(goodPath),
		Body:          strings.NewReader(contents),
		ContentLength: aws.Int64(int64(len(contents))),
	})
	if err != nil {
		t.Errorf("PutObject(%s, %s): %s", repo, goodPath, err)
	}

	t.Run("get_exists", func(t *testing.T) {
		opts := s3.WithAPIOptions(setHttpHeader("User-Agent", "client with s3RedirectionSupport set"))
		s3Client := createS3Client(viper.GetString("s3_endpoint"), t, opts)
		_, err = s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(repo),
			Key:    aws.String(goodPath),
		})
		// AWS GO SDK does not respect http.ErrUseLastResponse
		var ae smithy.APIError
		require.ErrorAs(t, err, &ae)
		require.Equal(t, "TemporaryRedirect", ae.ErrorCode())
	})
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
		s3Endpoint := viper.GetString("s3_endpoint")
		s3Client := createS3Client(s3Endpoint, t)
		_, listErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("not-exists")})
		require.ErrorContains(t, listErr, gtwerrors.ErrNoSuchBucket.Error())
	})
}

func TestDeleteObjects(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	const numOfObjects = 10
	identifiers := make([]types.ObjectIdentifier, 0, numOfObjects)

	for i := 1; i <= numOfObjects; i++ {
		file := strconv.Itoa(i) + ".txt"
		identifiers = append(identifiers, types.ObjectIdentifier{
			Key: aws.String(mainBranch + "/" + file),
		})
		_, _ = UploadFileRandomData(ctx, t, repo, mainBranch, file, nil)
	}

	listOut, err := svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Contents, numOfObjects)

	deleteOut, err := svc.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(repo),
		Delete: &types.Delete{
			Objects: identifiers,
		},
	})

	assert.NoError(t, err)
	assert.Len(t, deleteOut.Deleted, numOfObjects)

	listOut, err = svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})

	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, 0)
}

// TestDeleteObjects_Viewer verify we can't delete with read only user
func TestDeleteObjects_Viewer(t *testing.T) {
	t.SkipNow()
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// setup data
	const filename = "delete-me"
	_, _ = UploadFileRandomData(ctx, t, repo, mainBranch, filename, nil)

	// setup user with only view rights - create user, add to group, generate credentials
	uid := "del-viewer"
	resCreateUser, err := client.CreateUserWithResponse(ctx, apigen.CreateUserJSONRequestBody{
		Id: uid,
	})
	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	resAssociateUser, err := client.AddGroupMembershipWithResponse(ctx, "Viewers", "del-viewer")
	require.NoError(t, err, "Failed to add user to Viewers group")
	require.Equal(t, http.StatusCreated, resAssociateUser.StatusCode(), "AddGroupMembershipWithResponse unexpectedly status code")

	resCreateCreds, err := client.CreateCredentialsWithResponse(ctx, "del-viewer")
	require.NoError(t, err, "Failed to create credentials")
	require.NotNil(t, resCreateCreds.JSON201, "CreateCredentials unexpectedly empty response")

	// client with viewer user credentials
	key := resCreateCreds.JSON201.AccessKeyId
	secret := resCreateCreds.JSON201.SecretAccessKey
	s3Endpoint := viper.GetString("s3_endpoint")
	forcePathStyle := viper.GetBool("force_path_style")
	s3Client, err := testutil.SetupTestS3Client(s3Endpoint, key, secret, forcePathStyle)
	require.NoError(t, err)

	// delete objects using viewer
	deleteOut, err := s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(repo),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String(mainBranch + "/" + filename)},
			},
		},
	})
	// make sure we got an error we fail to delete the file
	assert.NoError(t, err)
	assert.Len(t, deleteOut.Errors, 1, "error we fail to delete")
	assert.Len(t, deleteOut.Deleted, 0, "no file should be deleted")

	// verify that viewer can't delete the file
	listOut, err := svc.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(repo),
		Prefix: aws.String(mainBranch + "/"),
	})
	assert.NoError(t, err)
	assert.Len(t, listOut.Contents, 1, "list should find 'delete-me' file")
	assert.Equal(t, aws.ToString(listOut.Contents[0].Key), mainBranch+"/"+filename)
}
