package nessie

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"

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
	destRepo := createRepositoryByName(ctx, t, "tests3copyobjectdest")

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	opts := minio.PutObjectOptions{}
	rand := rand.New(rand.NewSource(17))

	creds := sigs[0].GetCredentials(accessKeyID, secretAccessKey, "")

	type Object struct {
		SourcePath, DestPath, Content string
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
	})

	if err != nil {
		t.Fatalf("minio.New: %s", err)
	}

	o := Object{
		Content:    testutil.RandomString(rand, randomDataContentLength),
		SourcePath: prefix + "source-file",
		DestPath:   prefix + "dest-file",
	}

	_, err = minioClient.PutObject(
		ctx, repo, o.SourcePath, strings.NewReader(o.Content), int64(len(o.Content)), opts)
	if err != nil {
		t.Errorf("minio.Client.PutObject(%s): %s", o.SourcePath, err)
	}
	// copy object to the same repository
	_, err = minioClient.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: repo,
			Object: o.DestPath},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: o.SourcePath})

	if err != nil {
		t.Errorf("minio.Client.CopyObjectFrom(%s)To(%s): %s", o.SourcePath, o.DestPath, err)
	}

	download, err := minioClient.GetObject(
		ctx, repo, o.DestPath, minio.GetObjectOptions{})
	if err != nil {
		t.Errorf("minio.Client.GetObject(%s): %s", o.DestPath, err)
	}
	// compere files content
	contents := bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download)
	if err != nil {
		t.Errorf("download %s: %s", o.DestPath, err)
	}
	if strings.Compare(contents.String(), o.Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), o.Content)
	}

	resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Errorf("client.StatObject(%s): %s", o.SourcePath, err)
	}
	sourceObjectStats := resp.JSON200

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Errorf("client.StatObject(%s): %s", o.DestPath, err)
	}
	destObjectStats := resp.JSON200

	// assert that the physical addresses of the objects are the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) != 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}

	// copy object to different repository- create another version of the file
	_, err = minioClient.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: destRepo,
			Object: o.DestPath},
		minio.CopySrcOptions{
			Bucket: repo,
			Object: o.SourcePath})

	if err != nil {
		t.Errorf("minio.Client.CopyObjectFrom(%s)To(%s): %s", o.SourcePath, o.DestPath, err)
	}

	download, err = minioClient.GetObject(
		ctx, destRepo, o.DestPath, minio.GetObjectOptions{})
	if err != nil {
		t.Errorf("minio.Client.GetObject(%s): %s", o.DestPath, err)
	}
	contents = bytes.NewBuffer(nil)
	_, err = io.Copy(contents, download)
	if err != nil {
		t.Errorf("download %s: %s", o.DestPath, err)
	}
	// compere files content
	if strings.Compare(contents.String(), o.Content) != 0 {
		t.Errorf(
			"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), o.Content)
	}

	resp, err = client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: "data/source-file"})
	if err != nil {
		t.Errorf("client.StatObject(%s): %s", o.SourcePath, err)
	}
	sourceObjectStats = resp.JSON200

	resp, err = client.StatObjectWithResponse(ctx, destRepo, mainBranch, &api.StatObjectParams{Path: "data/dest-file"})
	if err != nil {
		t.Errorf("client.StatObject(%s): %s", o.DestPath, err)
	}
	destObjectStats = resp.JSON200

	// assert that the physical addresses of the objects are not the same
	if strings.Compare(sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress) == 0 {
		t.Errorf(
			"Source object address: %s Source destination address: %s", sourceObjectStats.PhysicalAddress, destObjectStats.PhysicalAddress)
	}
}
