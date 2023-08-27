package s3inventory

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func verifyObject(t *testing.T, actual *InventoryObject, expected *InventoryObject, index int, batchId int, indexInBatch int) {
	if expected.Bucket != actual.Bucket {
		t.Fatalf("bucket in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, expected.Bucket, actual.Bucket, batchId, indexInBatch)
	}
	if expected.Key != actual.Key {
		t.Fatalf("object key in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, expected.Key, actual.Key, batchId, indexInBatch)
	}
	if expected.Size != actual.Size {
		t.Fatalf("size in index %d different than expected. expected=%d, got=%d (batch #%d, index %d)", index, expected.Size, actual.Size, batchId, indexInBatch)
	}
	if expected.Checksum != actual.Checksum {
		t.Fatalf("e_tag in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, expected.Checksum, actual.Checksum, batchId, indexInBatch)
	}
	if !expected.LastModified.Equal(*actual.LastModified) {
		t.Fatalf("last_modified_time in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, expected.LastModified, actual.LastModified, batchId, indexInBatch)
	}
	if expected.IsDeleteMarker != actual.IsDeleteMarker {
		t.Fatalf("is_delete_marker in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, expected.IsDeleteMarker, actual.IsDeleteMarker, batchId, indexInBatch)
	}
	if actual.IsLatest != expected.IsLatest {
		t.Fatalf("is_latest in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, expected.IsLatest, actual.IsLatest, batchId, indexInBatch)
	}
}

func objs(num int, lastModified []time.Time) <-chan *TestObject {
	out := make(chan *TestObject)
	go func() {
		defer close(out)
		for i := 0; i < num; i++ {
			sz := int64(500)
			tm := lastModified[i%len(lastModified)].UnixNano() / 1_000_000
			checksum := "abcdefg"
			out <- &TestObject{
				Bucket:             inventoryBucketName,
				Key:                fmt.Sprintf("f%05d", i),
				Size:               &sz,
				LastModifiedMillis: &tm,
				Checksum:           &checksum,
			}
		}
	}()
	return out
}

func uploadFile(t *testing.T, client *s3.Client, inventoryBucket string, inventoryFilename string, f *os.File) {
	defer func() {
		_ = f.Close()
	}()
	uploader := manager.NewUploader(client)
	_, err := uploader.Upload(context.Background(),
		&s3.PutObjectInput{
			Bucket: aws.String(inventoryBucket),
			Key:    aws.String(inventoryFilename),
			Body:   f,
		})
	if err != nil {
		t.Fatalf("Upload file '%s/%s', failed: %s", inventoryBucket, inventoryFilename, err)
	}
}

func getS3Fake(t *testing.T) (*s3.Client, *httptest.Server) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(func() {
		ts.Close()
	})

	// configure S3 client
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "")),
		config.WithRegion("eu-central-1"),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	return client, ts
}
