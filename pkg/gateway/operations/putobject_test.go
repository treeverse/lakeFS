package operations_test

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	bucketName      = "test"
	ObjectBlockSize = 1024 * 3

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
		{"1 block", ObjectBlockSize, swag.String(expensiveString)},
		{"1 block and 100 bytes", ObjectBlockSize + 100, swag.String(cheapString)},
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
			blob, err := upload.WriteBlob(context.Background(), adapter, bucketName, reader, tc.size, opts)
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
