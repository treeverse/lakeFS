package nessie

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
)

var sigs = []struct {
	Name           string
	GetCredentials func(id, secret, token string) *credentials.Credentials
}{
	{"V2", credentials.NewStaticV2},
	{"V4", credentials.NewStaticV4},
}

const (
	numUploads = 1000
	//
	randomDataPathLength = 250
)

// randomRune returns a random Unicode rune from rand, weighting at least
// num out of den runes to be ASCII.
func randomRune(rand *rand.Rand, num, den int) rune {
	if rand.Intn(den) < num {
		return rune(rand.Intn(utf8.RuneSelf))
	}
	for {
		r := rune(rand.Intn(utf8.MaxRune))
		// Almost all chars are legal, so this usually
		// returns immediately.
		if utf8.ValidRune(r) {
			return r
		}
	}
}

// randomString returns a random UTF-8 string of size or almost size bytes
// from rand.  It is weighted towards using many ASCII characters.
func randomString(rand *rand.Rand, size int) string {
	sb := strings.Builder{}
	for sb.Len() <= size {
		sb.WriteRune(randomRune(rand, 2, 5))
	}
	ret := sb.String()
	_, lastRuneSize := utf8.DecodeLastRuneInString(ret)
	return ret[0 : len(ret)-lastRuneSize]
}

func TestS3UploadAndDownload(t *testing.T) {
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
			client, err := minio.New(endpoint, &minio.Options{
				Creds:  creds,
				Secure: false,
			})
			if err != nil {
				t.Error(fmt.Sprintf("minio.New: %s", err))
			}

			for i := 0; i < numUploads; i++ {
				objContent := randomString(rand, randomDataContentLength)
				// lakeFS supports _any_ path, even if its
				// byte sequence is not legal UTF-8 string.
				objPath := "main/data/" + randomString(rand, randomDataPathLength)
				_, err := client.PutObject(
					ctx, repo, objPath, strings.NewReader(objContent), int64(len(objContent)), opts)
				if err != nil {
					t.Error(fmt.Sprintf("minio.Client.PutObject(%s): %s", objPath, err))
				}

				download, err := client.GetObject(
					ctx, repo, objPath, minio.GetObjectOptions{})
				if err != nil {
					t.Error(fmt.Sprintf("minio.Client.GetObject(%s): %s", objPath, err))
				}
				contents := bytes.NewBuffer(nil)
				_, err = io.Copy(contents, download)
				if err != nil {
					t.Errorf(fmt.Sprintf("download %s: %s", objPath, err))
				}
				if strings.Compare(contents.String(), objContent) != 0 {
					t.Error(fmt.Sprintf(
						"Downloaded bytes %v from uploaded bytes %v", contents.Bytes(), objContent))
				}
			}
		})
	}
}
