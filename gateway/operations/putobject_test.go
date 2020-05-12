package operations_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/testutil"
	"github.com/treeverse/lakefs/upload"
)

const (
	bucketName      = "test"
	ObjectBlockSize = 1024 * 3
)

type mockAdapter struct {
	totalSize  int64
	count      int
	lastBucket string
}

func (a *mockAdapter) WithContext(ctx context.Context) block.Adapter {
	return a
}

func newMockAdapter() *mockAdapter {
	adapter := mockAdapter{
		totalSize: 0,
		count:     0,
	}
	return &adapter
}

func (a *mockAdapter) Put(repo string, _ string, _ int64, reader io.Reader) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	a.totalSize += int64(len(data))
	a.count++
	a.lastBucket = repo
	return nil
}
func (a *mockAdapter) Get(_ string, _ string) (io.ReadCloser, error) {
	return nil, nil
}
func (a *mockAdapter) GetRange(_ string, _ string, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}
func (s *mockAdapter) Remove(repo string, identifier string) error {

	return errors.New(" remove method not implemented in mock adapter")
}
func (s *mockAdapter) CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error) {
	panic("try to create multipart in mock adaptor ")
}
func (s *mockAdapter) UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
	panic("try to upload part in mock adaptor ")

}
func (s *mockAdapter) AbortMultiPartUpload(repo string, identifier string, uploadId string) error {
	panic("try to abort multipart in mock adaptor ")

}
func (s *mockAdapter) CompleteMultiPartUpload(repo string, identifier string, uploadId string, MultipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	panic("try to complete multipart in mock adaptor ")
}

func TestReadBlob(t *testing.T) {

	tt := []struct {
		name string
		size int64
	}{
		{"no data", 0},
		{"100 bytes", 100},
		{"1 block", ObjectBlockSize},
		{"1 block and 100 bytes", ObjectBlockSize + 100},
		{"2 blocks and 1 bytes", ObjectBlockSize*2 + 1},
		{"1000 blocks", ObjectBlockSize * 1000},
	}
	deduper := testutil.NewMockDedup()
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)

			_, err := rand.Read(data)
			if err != nil {
				t.Fatal(err)
			}
			reader := bytes.NewReader(data)
			adapter := newMockAdapter()
			checksum, physicalAddress_1, size, err := upload.WriteBlob(deduper, bucketName, bucketName, reader, adapter, tc.size)
			if err != nil {
				t.Fatal(err)
			}

			// test bucketName
			if adapter.lastBucket != bucketName && tc.size != 0 {
				t.Fatalf("write to wrong bucket: expected:%s got:%s", bucketName, adapter.lastBucket)
			}
			//test data size
			expectedSize := int64(len(data))
			if expectedSize != size {
				t.Fatalf("expected sent size to be equal to adapter read size, got: sent:%d , adapter:%d", expectedSize, adapter.totalSize)
			}
			if adapter.totalSize != size {
				t.Fatalf("expected blob size to be equal to adapter read size, got: blob:%d , adapter:%d", size, adapter.totalSize)
			}

			// test checksum
			expectedMD5 := fmt.Sprintf("%x", md5.Sum(data))
			if checksum != expectedMD5 {
				t.Fatalf("expected blob checksum to be equal to data checksum, got: blob:%s , data:%s", checksum, expectedMD5)
			}
			// write the same data again - make sure it is de-duped
			reader.Reset(data)
			adapter = newMockAdapter()
			_, physicalAddress_2, _, err := upload.WriteBlob(deduper, bucketName, bucketName, reader, adapter, tc.size)
			if err != nil {
				t.Fatal(err)
			}
			if physicalAddress_1 != physicalAddress_2 {
				t.Fatalf("duplocate data not identified, got: first: %s , second: %s", physicalAddress_1, physicalAddress_2)
			}
		})
	}
}
