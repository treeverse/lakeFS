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
	const parallelism = 10

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	opts := minio.PutObjectOptions{}

	for _, sig := range sigs {
		t.Run("Sig"+sig.Name, func(t *testing.T) {
			// Use same sequence of pathnames to test each sig.
			rand := rand.New(rand.NewSource(17))

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
					Secure: false,
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
					Content: testutil.RandomString(rand, randomDataContentLength),
					// lakeFS supports _any_ path, even if its
					// byte sequence is not legal UTF-8 string.
					Path: prefix + testutil.RandomString(rand, randomDataPathLength-len(prefix)),
				}
			}
			close(objects)
			wg.Wait()
		})
	}
}

func TestS3CopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	destRepoName := "tests3copyobjectdest"
	destRepo := createRepositoryByName(ctx, t, destRepoName)
	defer deleteRepositoryIfAskedTo(context.Background(), destRepoName)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	opts := minio.PutObjectOptions{}
	rand := rand.New(rand.NewSource(17))

	creds := sigs[0].GetCredentials(accessKeyID, secretAccessKey, "")

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}

	Content := testutil.RandomString(rand, randomDataContentLength)
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
			Object: DestPath},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: SourcePath})

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
			Object: DestPath},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: SourcePath})

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
