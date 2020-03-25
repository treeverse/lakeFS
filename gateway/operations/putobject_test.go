package operations_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/treeverse/lakefs/block"

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

func (a *mockAdapter) Put(repo string, _ string, reader io.ReadSeeker) error {
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

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			_, err := rand.Read(data)
			if err != nil {
				t.Fatal(err)
			}
			reader := bytes.NewReader(data)
			adapter := newMockAdapter()
			blob, err := upload.ReadBlob(bucketName, reader, adapter, ObjectBlockSize)
			if err != nil {
				t.Fatal(err)
			}

			// test bucketName
			if adapter.lastBucket != bucketName && tc.size != 0 {
				t.Fatalf("write to wrong bucket: expected:%s got:%s", bucketName, adapter.lastBucket)
			}
			//test data size
			expectedSize := int64(len(data))
			if expectedSize != blob.Size {
				t.Fatalf("expected sent size to be equal to adapter read size, got: sent:%d , adapter:%d", expectedSize, adapter.totalSize)
			}
			if adapter.totalSize != blob.Size {
				t.Fatalf("expected blob size to be equal to adapter read size, got: blob:%d , adapter:%d", blob.Size, adapter.totalSize)
			}

			// test block number
			if adapter.count != len(blob.Blocks) {
				t.Fatalf("expected number of blocks in blob to be equal to number of calls to adapter adapter read size, got: blob:%d , adapter:%d", len(blob.Blocks), adapter.count)
			}

			expectedBlocks := int(tc.size/(ObjectBlockSize)) + 1
			if tc.size%ObjectBlockSize == 0 {
				expectedBlocks -= 1
			}

			if adapter.count != expectedBlocks {
				t.Fatalf("expected number of blocks in blob to be equal to %d got:%d", expectedBlocks, adapter.count)
			}

			// test checksum
			expectedMD5 := fmt.Sprintf("%x", md5.Sum(data))
			if blob.Checksum != expectedMD5 {
				t.Fatalf("expected blob checksum to be equal to data checksum, got: blob:%s , data:%s", blob.Checksum, expectedMD5)
			}
		})
	}
}
