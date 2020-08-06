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
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/upload"
)

const (
	bucketName      = "test"
	ObjectBlockSize = 1024 * 3
)

type mockAdapter struct {
	totalSize        int64
	count            int
	lastBucket       string
	lastStorageClass *string
}

func (s *mockAdapter) WithContext(ctx context.Context) block.Adapter {
	return s
}

func newMockAdapter() *mockAdapter {
	adapter := mockAdapter{
		totalSize:        0,
		count:            0,
		lastStorageClass: nil,
	}
	return &adapter
}

func (s *mockAdapter) Put(obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	s.totalSize += int64(len(data))
	s.count++
	s.lastBucket = obj.StorageNamespace
	s.lastStorageClass = opts.StorageClass
	return nil
}
func (s *mockAdapter) Get(obj block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	return nil, nil
}
func (s *mockAdapter) GetRange(_ block.ObjectPointer, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}

func (s *mockAdapter) GetProperties(_ block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, errors.New("getProperties method not implemented in mock adapter")
}

func (s *mockAdapter) Remove(_ block.ObjectPointer) error {
	return errors.New(" remove method not implemented in mock adapter")
}
func (s *mockAdapter) CreateMultiPartUpload(_ block.ObjectPointer, r *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {
	panic("try to create multipart in mock adaptor ")
}

func (s *mockAdapter) UploadPart(_ block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	panic("try to upload part in mock adaptor ")

}
func (s *mockAdapter) AbortMultiPartUpload(_ block.ObjectPointer, uploadID string) error {
	panic("try to abort multipart in mock adaptor ")

}
func (s *mockAdapter) CompleteMultiPartUpload(_ block.ObjectPointer, uploadID string, MultipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	panic("try to complete multipart in mock adaptor ")
}

func (s *mockAdapter) ValidateConfiguration(_ string) error {
	return nil
}

func (s *mockAdapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, nil
}

var (
	expensiveString = "EXPENSIVE"
	cheapString     = "CHEAP"
)

func TestReadBlob(t *testing.T) {
	tt := []struct {
		name         string
		size         int64
		storageClass *string
	}{
		{"no data", 0, nil},
		{"100 bytes", 100, nil},
		{"1 block", ObjectBlockSize, &expensiveString},
		{"1 block and 100 bytes", ObjectBlockSize + 100, &cheapString},
		{"2 blocks and 1 bytes", ObjectBlockSize*2 + 1, nil},
		{"1000 blocks", ObjectBlockSize * 1000, nil},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)

			_, err := rand.Read(data)
			if err != nil {
				t.Fatal(err)
			}
			reader := bytes.NewReader(data)
			adapter := newMockAdapter()
			opts := block.PutOpts{StorageClass: tc.storageClass}
			blob, err := upload.WriteBlob(adapter, bucketName, reader, tc.size, opts)
			if err != nil {
				t.Fatal(err)
			}

			// test bucketName
			if adapter.lastBucket != bucketName && tc.size != 0 {
				t.Fatalf("write to wrong bucket: expected:%s got:%s", bucketName, adapter.lastBucket)
			}
			// test data size
			expectedSize := int64(len(data))
			if expectedSize != blob.Size {
				t.Fatalf("expected sent size to be equal to adapter read size, got: sent:%d , adapter:%d", expectedSize, adapter.totalSize)
			}
			if adapter.totalSize != blob.Size {
				t.Fatalf("expected blob size to be equal to adapter read size, got: blob:%d , adapter:%d", blob.Size, adapter.totalSize)
			}
			// test storage class
			if adapter.lastStorageClass != tc.storageClass {
				t.Errorf("expected sent storage class to be equal to requested storage class, got: %v , requested: %v",
					adapter.lastStorageClass,
					tc.storageClass)
			}

			// test checksum
			expectedMD5 := fmt.Sprintf("%x", md5.Sum(data))
			if blob.Checksum != expectedMD5 {
				t.Fatalf("expected blob checksum to be equal to data checksum, got: blob:%s , data:%s", blob.Checksum, expectedMD5)
			}
		})
	}
}
