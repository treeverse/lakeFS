package nessie

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
