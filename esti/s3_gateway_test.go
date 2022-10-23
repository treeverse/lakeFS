package esti

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/api"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var sigs = []struct {
	Name           string
	GetCredentials func(id, secret, token string) *credentials.Credentials
}{
	{"V2", credentials.NewStaticV2},
	{"V4", credentials.NewStaticV4},
}

const (
	numUploads           = 100
	randomDataPathLength = 1020
	prefix               = "main/data/"
)

func TestS3UploadAndDownload(t *testing.T) {
	SkipTestIfAskedTo(t)
	const parallelism = 10

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	endpointSecure := viper.GetBool("s3_endpoint_secure")
	opts := minio.PutObjectOptions{}

	for _, sig := range sigs {
		t.Run("Sig"+sig.Name, func(t *testing.T) {
			// Use same sequence of pathnames to test each sig.
			r := rand.New(rand.NewSource(17))

			creds := sig.GetCredentials(accessKeyID, secretAccessKey, "")

			type Object struct {
				Path, Content string
			}

			var (
				wg      sync.WaitGroup
				objects = make(chan Object, parallelism*2)
			)

			for i := 0; i < parallelism; i++ {
				client, err := minio.New(endpoint, &minio.Options{
					Creds:  creds,
					Secure: endpointSecure,
				})
				if err != nil {
					t.Fatalf("minio.New: %s", err)
				}

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
					Path: prefix + testutil.RandomString(r, randomDataPathLength-len(prefix)),
				}
			}
			close(objects)
			wg.Wait()
		})
	}
}

func TestS3GetObject(t *testing.T) {
	SkipTestIfAskedTo(t)

	const (
		contents = "the quick brown fox jumps over the lazy dog"
		goodPath = "main/exists"
		badPath  = "main/does/not/exist"
	)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	endpointSecure := viper.GetBool("s3_endpoint_secure")

	// Upload an object
	creds := sigs[0].GetCredentials(accessKeyID, secretAccessKey, "")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: endpointSecure,
	})
	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}

	_, err = client.PutObject(ctx, repo, goodPath, strings.NewReader(contents), int64(len(contents)), minio.PutObjectOptions{})
	if err != nil {
		t.Errorf("client.PutObject(%s, %s): %s", repo, goodPath, err)
	}

	t.Run("Exists", func(t *testing.T) {
		res, err := client.GetObject(ctx, repo, goodPath, minio.GetObjectOptions{})
		if err != nil {
			t.Errorf("client.GetObject(%s, %s): %s", repo, goodPath, err)
		}
		defer res.Close()
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
}

func TestS3CopyObject(t *testing.T) {
	SkipTestIfAskedTo(t)
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	destRepoName := "tests3copyobjectdest"
	destRepo := createRepositoryByName(ctx, t, destRepoName)
	defer deleteRepositoryIfAskedTo(context.Background(), destRepoName)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	endpointSecure := viper.GetBool("s3_endpoint_secure")
	opts := minio.PutObjectOptions{}
	r := rand.New(rand.NewSource(17))

	creds := sigs[0].GetCredentials(accessKeyID, secretAccessKey, "")

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: endpointSecure,
	})
	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}

	Content := testutil.RandomString(r, randomDataContentLength)
	SourcePath := prefix + "source-file"
	DestPath := prefix + "dest-file"

	_, err = minioClient.PutObject(
		ctx, repo, SourcePath, strings.NewReader(Content), int64(len(Content)), opts)
	if err != nil {
		t.Errorf("minio.Client.PutObject(%s): %s", SourcePath, err)
	}
	// copy object to the same repository
	_, err = minioClient.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: repo,
			Object: DestPath,
		},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: SourcePath,
		})

	if err != nil {
		t.Errorf("minio.Client.CopyObjectFrom(%s)To(%s): %s", SourcePath, DestPath, err)
	}

	download, err := minioClient.GetObject(
		ctx, repo, DestPath, minio.GetObjectOptions{})
	if err != nil {
		t.Errorf("minio.Client.GetObject(%s): %s", DestPath, err)
	}
	// compere files content
	contents := bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download)
	if err != nil {
		t.Fatalf("download %s: %s", DestPath, err)
	}
	if strings.Compare(contents.String(), Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), Content)
	}

	resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", SourcePath, err)
	}
	sourceObjectStats := resp.JSON200
	require.Equal(t, http.StatusOK, resp.StatusCode())

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", DestPath, err)
	}
	destObjectStats := resp.JSON200
	require.Equal(t, http.StatusOK, resp.StatusCode())

	// assert that the physical addresses of the objects are the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) != 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}

	// copy object to different repository- create another version of the file
	_, err = minioClient.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: destRepo,
			Object: DestPath,
		},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: SourcePath,
		})

	if err != nil {
		t.Errorf("minio.Client.CopyObjectFrom(%s)To(%s): %s", SourcePath, DestPath, err)
	}

	download, err = minioClient.GetObject(
		ctx, destRepo, DestPath, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("minio.Client.GetObject(%s): %s", DestPath, err)
	}
	contents = bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download)
	if err != nil {
		t.Errorf("download %s: %s", DestPath, err)
	}
	// compere files content
	if strings.Compare(contents.String(), Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), Content)
	}

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", SourcePath, err)
	}
	sourceObjectStats = resp.JSON200
	require.Equal(t, http.StatusOK, resp.StatusCode())

	resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Fatalf("client.StatObject(%s): %s", DestPath, err)
	}
	destObjectStats = resp.JSON200
	require.Equal(t, http.StatusOK, resp.StatusCode())

	// assert that the physical addresses of the objects are not the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) == 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}
}
