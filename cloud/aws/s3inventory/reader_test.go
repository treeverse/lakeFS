package s3inventory

import (
	"context"
	"fmt"
	"io/ioutil"
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
	"github.com/cznic/mathutil"
	"github.com/go-openapi/swag"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/scritchley/orc"
	"github.com/treeverse/lakefs/logging"
)

const inventoryBucketName = "inventory-bucket"

func generateOrc(t *testing.T, objs <-chan *InventoryObject) string {
	f, err := ioutil.TempFile("", "orctest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()
	schema, err := orc.ParseSchema("struct<bucket:string,key:string,size:int,last_modified_date:timestamp,e_tag:string>")
	if err != nil {
		t.Fatal(err)
	}
	w, err := orc.NewWriter(f, orc.SetSchema(schema), orc.SetStripeTargetSize(100))
	if err != nil {
		t.Fatal(err)
	}
	for o := range objs {
		err = w.Write(o.Bucket, o.Key, *o.Size, time.Unix(*o.LastModifiedMillis/1000, 0), *o.Checksum)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	return f.Name()
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

func objs(num int, lastModified []time.Time) <-chan *InventoryObject {
	out := make(chan *InventoryObject)
	go func() {
		defer close(out)
		for i := 0; i < num; i++ {
			out <- &InventoryObject{
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

func uploadFile(t *testing.T, s3 s3iface.S3API, inventoryBucket string, inventoryFilename string, objs <-chan *InventoryObject) {
	localOrcFile := generateOrc(t, objs)
	f, err := os.Open(localOrcFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()
	uploader := s3manager.NewUploaderWithClient(s3)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(inventoryBucket),
		Key:    aws.String(inventoryFilename),
		Body:   f,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInventoryReader(t *testing.T) {
	svc, testServer := getS3Fake(t)
	defer testServer.Close()
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(inventoryBucketName),
	})
	if err != nil {
		t.Fatal(err)
	}
	testdata := []struct {
		ObjectNum           int
		ExpectedReadObjects int
		ExpectedMaxValue    string
		ExpectedMinValue    string
	}{
		{
			ObjectNum:           2,
			ExpectedReadObjects: 2,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f00001",
		},
		{
			ObjectNum:           12500,
			ExpectedReadObjects: 12500,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f12499",
		},
		{
			ObjectNum:           100,
			ExpectedReadObjects: 100,
			ExpectedMinValue:    "f00000",
			ExpectedMaxValue:    "f00099",
		},
	}

	for _, test := range testdata {
		now := time.Now()
		lastModified := []time.Time{now, now.Add(-1 * time.Hour), now.Add(-2 * time.Hour), now.Add(-3 * time.Hour)}
		uploadFile(t, svc, inventoryBucketName, "myFile.orc", objs(test.ObjectNum, lastModified))
		reader := NewReader(context.Background(), svc, logging.Default())
		fileReader, err := reader.GetFileReader("ORC", inventoryBucketName, "myFile.orc")
		if err != nil {
			t.Fatal(err)
		}
		numRowsResult := int(fileReader.GetNumRows())
		if test.ObjectNum != numRowsResult {
			t.Fatalf("unexpected result from GetNumRows. expected=%d, got=%d", test.ObjectNum, numRowsResult)
		}
		minValueResult := fileReader.FirstObjectKey()
		if test.ExpectedMinValue != minValueResult {
			t.Fatalf("unexpected result from FirstObjectKey. expected=%s, got=%s", test.ExpectedMinValue, minValueResult)
		}
		maxValueResult := fileReader.LastObjectKey()
		if test.ExpectedMaxValue != maxValueResult {
			t.Fatalf("unexpected result from LastObjectKey. expected=%s, got=%s", test.ExpectedMaxValue, maxValueResult)
		}
		readBatchSize := 1000
		res := make([]InventoryObject, readBatchSize)
		offset := 0
		readCount := 0
		for {
			err = fileReader.Read(&res)
			for i := offset; i < mathutil.Min(offset+readBatchSize, test.ObjectNum); i++ {
				if res[i-offset].Key != fmt.Sprintf("f%05d", i) {
					t.Fatalf("result in index %d different than expected. expected=%s, got=%s (batch #%d, index %d)", i, fmt.Sprintf("f%05d", i), res[i-offset].Key, offset/readBatchSize, i-offset)
				}
				expectedLastModified := lastModified[i%len(lastModified)].Unix() * 1000
				if *res[i-offset].LastModifiedMillis != expectedLastModified {
					t.Fatalf("unexpected timestamp for result in index %d. expected=%d, got=%d (batch #%d, index %d)", i, expectedLastModified, *res[i-offset].LastModifiedMillis, offset/readBatchSize, i-offset)
				}
			}
			offset += len(res)
			readCount += len(res)
			if err != nil {
				t.Fatal(err)
			}
			if len(res) != readBatchSize {
				break
			}
		}
		if test.ExpectedReadObjects != readCount {
			t.Fatalf("read unexpected number of keys from inventory. expected=%d, got=%d", test.ExpectedReadObjects, readCount)
		}
		if fileReader.Close() != nil {
			t.Fatalf("failed to close file reader")
		}
	}
}
