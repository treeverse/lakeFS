package s3inventory

import (
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/go-openapi/swag"
)

func verifyObject(t *testing.T, actual *InventoryObject, expected *TestObject, index int, batchId int, indexInBatch int) {
	if expected.Bucket != actual.Bucket {
		t.Fatalf("bucket in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, expected.Bucket, actual.Bucket, batchId, indexInBatch)
	}
	if expected.Key != actual.Key {
		t.Fatalf("object key in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, expected.Key, actual.Key, batchId, indexInBatch)
	}
	if swag.Int64Value(expected.Size) != swag.Int64Value(actual.Size) {
		t.Fatalf("size in index %d different than expected. expected=%d, got=%d (batch #%d, index %d)", index, swag.Int64Value(expected.Size), swag.Int64Value(actual.Size), batchId, indexInBatch)
	}
	if swag.StringValue(expected.Checksum) != swag.StringValue(actual.Checksum) {
		t.Fatalf("e_tag in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", index, swag.StringValue(expected.Checksum), swag.StringValue(actual.Checksum), batchId, indexInBatch)
	}
	if *expected.LastModifiedMillis != *actual.LastModifiedMillis {
		t.Fatalf("last_modified_time in index %d different than expected. expected=%d, got=%d (batch #%d, index %d)", index, *expected.LastModifiedMillis, *actual.LastModifiedMillis, batchId, indexInBatch)
	}
	if *expected.IsDeleteMarker != swag.BoolValue(actual.IsDeleteMarker) {
		t.Fatalf("is_delete_marker in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, *expected.IsDeleteMarker, *actual.IsDeleteMarker, batchId, indexInBatch)
	}
	if actual.IsLatest == nil && !*expected.IsLatest {
		t.Fatalf("is_latest in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, *expected.IsLatest, nil, batchId, indexInBatch)
	}
	if actual.IsLatest != nil && *expected.IsLatest != swag.BoolValue(actual.IsLatest) {
		t.Fatalf("is_latest in index %d different than expected. expected=%v, got=%v (batch #%d, index %d)", index, *expected.IsLatest, *actual.IsLatest, batchId, indexInBatch)
	}
}

func objs(num int, lastModified []time.Time) <-chan *TestObject {
	out := make(chan *TestObject)
	go func() {
		defer close(out)
		for i := 0; i < num; i++ {
			out <- &TestObject{
				Bucket:             inventoryBucketName,
				Key:                fmt.Sprintf("f%05d", i),
				Size:               swag.Int64(500),
				LastModifiedMillis: swag.Int64(lastModified[i%len(lastModified)].Unix() * 1000),
				Checksum:           swag.String("abcdefg"),
			}
		}
	}()
	return out
}

func uploadFile(t *testing.T, s3 s3iface.S3API, inventoryBucket string, inventoryFilename string, f *os.File) {
	defer func() {
		_ = f.Close()
	}()
	uploader := s3manager.NewUploaderWithClient(s3)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(inventoryBucket),
		Key:    aws.String(inventoryFilename),
		Body:   f,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func getS3Fake(t *testing.T) (s3iface.S3API, *httptest.Server) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	// configure S3 client
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String("eu-central-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	return s3.New(newSession), ts
}
