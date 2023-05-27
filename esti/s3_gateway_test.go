package esti

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type GetCredentials = func(id, secret, token string) *credentials.Credentials

var (
	sigV2 = credentials.NewStaticV2
	sigV4 = credentials.NewStaticV4
	sigs  = []struct {
		Name           string
		GetCredentials GetCredentials
	}{
		{"V2", sigV2},
		{"V4", sigV4},
	}
)

const (
	numUploads           = 100
	randomDataPathLength = 1020
	gatewayTestPrefix    = "main/data/"
)

func newClient(t *testing.T, getCredentials GetCredentials) *minio.Client {
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

func TestS3UploadAndDownload(t *testing.T) {
	const parallelism = 10

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	opts := minio.PutObjectOptions{}

	for _, sig := range sigs {
		t.Run("Sig"+sig.Name, func(t *testing.T) {
			// Use same sequence of pathnames to test each sig.
			r := rand.New(rand.NewSource(17))

			type Object struct {
				Path, Content string
			}

			var (
				wg      sync.WaitGroup
				objects = make(chan Object, parallelism*2)
			)

			for i := 0; i < parallelism; i++ {
				client := newClient(t, sig.GetCredentials)

				wg.Add(1)
				go func() {
					for o := range objects {
						_, err := client.PutObject(
							ctx, repo, o.Path, strings.NewReader(o.Content), int64(len(o.Content)), opts)
						if err != nil {
							t.Errorf("minio.Client.PutObject(%s): %s", o.Path, err)
						}

						download, err := client.GetObject(
							ctx, repo, o.Path, minio.GetObjectOptions{})
						if err != nil {
							t.Errorf("minio.Client.GetObject(%s): %s", o.Path, err)
						}
						contents := bytes.NewBuffer(nil)
						_, err = io.Copy(contents, download)
						if err != nil {
							t.Errorf("download %s: %s", o.Path, err)
						}
						if strings.Compare(contents.String(), o.Content) != 0 {
							t.Errorf(
								"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), o.Content)
						}
					}
					wg.Done()
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
	client := newClient(t, sigV2)

	_, err := client.PutObject(ctx, repo, goodPath, strings.NewReader(contents), int64(len(contents)), minio.PutObjectOptions{})
	if err != nil {
		t.Errorf("client.PutObject(%s, %s): %s", repo, goodPath, err)
	}

	t.Run("GetObject", func(t *testing.T) {
		t.Run("Exists", func(t *testing.T) {
			res, err := client.GetObject(ctx, repo, goodPath, minio.GetObjectOptions{})
			if err != nil {
				t.Errorf("client.GetObject(%s, %s): %s", repo, goodPath, err)
			}
			defer res.Close()
			info, err := res.Stat()
			if err != nil {
				t.Errorf("client.GetObject(%s, %s) get: %s", repo, goodPath, err)
			}
			verifyObjectInfo(t, info, len(contents))
			got, err := io.ReadAll(res)
			if err != nil {
				t.Errorf("client.Read: %s", err)
			}
			if string(got) != contents {
				t.Errorf("Got contents \"%s\" but expected \"%s\"", string(got), contents)
			}
		})
		t.Run("Doesn't exist", func(t *testing.T) {
			res, err := client.GetObject(ctx, repo, badPath, minio.GetObjectOptions{})
			if err != nil {
				t.Errorf("client.GetObject(%s, %s): %s", repo, badPath, err)
			}
			defer res.Close()
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
	})

	t.Run("HeadObject", func(t *testing.T) {
		t.Run("Exists", func(t *testing.T) {
			info, err := client.StatObject(ctx, repo, goodPath, minio.StatObjectOptions{})
			if err != nil {
				t.Errorf("client.StatObject(%s, %s): %s", repo, goodPath, err)
			}
			verifyObjectInfo(t, info, len(contents))
		})
		t.Run("Doesn't exist", func(t *testing.T) {
			info, err := client.StatObject(ctx, repo, badPath, minio.StatObjectOptions{})
			if err == nil {
				t.Errorf("client.StatObject(%s, %s): expected an error but got %+v", repo, badPath, info)
			}

			s3ErrorResponse := minio.ToErrorResponse(err)
			if s3ErrorResponse.StatusCode != 404 {
				t.Errorf("Got %+v [%d] on reading when expecting Not Found [404]",
					s3ErrorResponse, s3ErrorResponse.StatusCode)
			}
		})
	})
}

func TestS3HeadBucket(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	badRepo := repo + "-nonexistent"

	// Upload an object
	client := newClient(t, sigV2)

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

func TestS3CopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// additional repository for copy between repos
	const destRepoName = "tests3copyobjectdest"
	destRepo := createRepositoryByName(ctx, t, destRepoName)
	defer deleteRepositoryIfAskedTo(ctx, destRepoName)

	// content
	r := rand.New(rand.NewSource(17))
	objContent := testutil.RandomString(r, randomDataContentLength)
	srcPath := gatewayTestPrefix + "source-file"
	destPath := gatewayTestPrefix + "dest-file"

	// upload data
	minioClient := newClient(t, sigV2)
	_, err := minioClient.PutObject(ctx, repo, srcPath, strings.NewReader(objContent), int64(len(objContent)),
		minio.PutObjectOptions{})
	require.NoError(t, err)

	t.Run("same_branch", func(t *testing.T) {
		// copy object to the same repository
		_, err = minioClient.CopyObject(ctx,
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

		download, err := minioClient.GetObject(ctx, repo, destPath, minio.GetObjectOptions{})
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

		resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		// assert that the physical addresses of the objects are the same
		sourceObjectStats := resp.JSON200
		destObjectStats := resp.JSON200
		require.Equal(t, sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress, "source and dest physical address should match")
	})

	t.Run("different_repo", func(t *testing.T) {
		// copy object to different repository. should create another version of the file
		_, err := minioClient.CopyObject(ctx,
			minio.CopyDestOptions{
				Bucket: destRepo,
				Object: destPath,
			},
			minio.CopySrcOptions{
				Bucket: repo,
				Object: srcPath,
			})
		if err != nil {
			t.Fatalf("minio.Client.CopyObjectFrom(%s)To(%s): %s", srcPath, destPath, err)
		}

		download, err := minioClient.GetObject(ctx, destRepo, destPath, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("minio.Client.GetObject(%s): %s", destPath, err)
		}
		defer func() { _ = download.Close() }()
		var contents bytes.Buffer
		_, err = io.Copy(&contents, download)
		require.NoError(t, err)

		// compere files content
		require.Equal(t, contents.String(), objContent)

		resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		sourceObjectStats := resp.JSON200

		resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
		if err != nil {
			t.Fatalf("client.StatObject(%s): %s", destPath, err)
		}
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		destObjectStats := resp.JSON200

		// assert that the physical addresses of the objects are not the same
		require.NotEqual(t, sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	})
}
