package nessie

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
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

const (
	numUploads           = 100
	randomDataPathLength = 1020
	prefix               = "main/data/"
)

var sigs = []struct {
	Name           string
	GetCredentials func(id, secret, token string) *credentials.Credentials
}{
	{"V2", credentials.NewStaticV2},
	{"V4", credentials.NewStaticV4},
}

func md5Base64String(b []byte) string {
	array := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(array[:])
}

func sha256HexString(b []byte) string {
	array := sha256.Sum256(b)
	return hex.EncodeToString(array[:])
}

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

func TestS3MultipartUploadAndDownload(t *testing.T) {
	const (
		numParts      = 15
		partSizeBytes = 5 * 1 << 20
		objectName    = "main/big"
	)

	ctx, _, repo := setupTest(t)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endpoint := viper.GetString("s3_endpoint")
	opts := minio.PutObjectOptions{}

	client, err := minio.NewCore(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		t.Fatalf("Create new minio (core) client: %s", err)
	}

	uploadID, err := client.NewMultipartUpload(ctx, repo, objectName, opts)
	if err != nil {
		t.Fatalf("New multipart upload: %s", err)
	}

	var (
		wg             sync.WaitGroup
		mu             sync.Mutex // protects parts
		completedParts = make([]minio.CompletePart, 0, numParts)
	)

	for i := 0; i < numParts; i++ {
		// Upload in parallel, to increase rate _and_ chance of confusion.
		wg.Add(1)
		go func() {
			r := rand.New(rand.NewSource(int64(i)))
			part := make([]byte, partSizeBytes)
			for j := 0; j < partSizeBytes; j++ {
				part[j] = byte(r.Int31n(256))
			}

			md5Sum := md5Base64String(part)
			sha256Sum := sha256HexString(part)

			data := bytes.NewReader(part)
			objectPart, err := client.PutObjectPart(ctx, repo, objectName, uploadID, i, data, partSizeBytes, md5Sum, sha256Sum, nil)
			if err != nil {
				t.Errorf("PutObjectPart %s ID %s part %d: %s", objectName, uploadID, i, err)
				return
			}
			if objectPart.Size != partSizeBytes {
				t.Errorf("PutObjectPart %s ID %s part %d: got %d not %d bytes uploaded", objectName, uploadID, i, objectPart.Size, partSizeBytes)
			}
			completedPart := minio.CompletePart{
				PartNumber: i,
				ETag:       objectPart.ETag,
			}
			mu.Lock()
			defer mu.Unlock()
			completedParts = append(completedParts, completedPart)
			wg.Done()
		}()
	}
	wg.Wait()

	mu.Lock()
	_, err = client.CompleteMultipartUpload(ctx, repo, objectName, uploadID, completedParts, opts)
	mu.Unlock()
	if err != nil {
		t.Errorf("CompleteMultipartUpload %s ID %s parts %+v: %s", objectName, uploadID, completedParts, err)
	}

	// Verify uploaded part.
	d, _, _, err := client.GetObject(ctx, repo, objectName, minio.GetObjectOptions{})
	if err != nil {
		t.Errorf("GetObject %s: %s", objectName, err)
	}
	defer d.Close()
	download := bufio.NewReader(d)
	for i := 0; i < numParts; i++ {
		r := rand.New(rand.NewSource(int64(i)))
		for j := 0; j < partSizeBytes; j++ {
			b, err := download.ReadByte()
			if err != nil {
				t.Fatalf("Failed to read part %d byte %d (offset %d): %s", i, j, i*partSizeBytes+j, err)
			}
			e := r.Int31n(256)
			if b != byte(e) {
				t.Fatalf("Part %d byte %d (offset %d): Got byte %d != %d", i, j, i*partSizeBytes+j, b, e)
			}
		}
	}
	b, err := download.ReadByte()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected to read precisely %d bytes to end, got next byte %02x (%c)", numParts*partSizeBytes, b, b)
	}
}
