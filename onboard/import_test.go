package onboard

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io/ioutil"
	"strings"
	"testing"
)

type mockS3Client struct {
	s3iface.S3API
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	output := s3.GetObjectOutput{}
	if *input.Bucket != "example-bucket" {
		return &output, nil
	}
	if *input.Key != "/example-path/manifest.json" {
		return &output, nil
	}
	reader := strings.NewReader(`{
  "sourceBucket" : "lakefs-example-data",
  "destinationBucket" : "arn:aws:s3:::yoni-test3",
  "version" : "2016-11-30",
  "creationTimestamp" : "1593216000000",
  "fileFormat" : "Parquet",
  "fileSchema" : "message s3.inventory {  required binary bucket (STRING);  required binary key (STRING);  optional binary version_id (STRING);  optional boolean is_latest;  optional boolean is_delete_marker;  optional int64 size;  optional int64 last_modified_date (TIMESTAMP(MILLIS,true));  optional binary e_tag (STRING);  optional binary storage_class (STRING);  optional boolean is_multipart_uploaded;}",
  "files" : [ {
    "key" : "inventory/lakefs-example-data/my_inventory/data/ea8268b2-a6ba-42de-8694-91a9833b4ff1.parquet",
    "size" : 46473,
    "MD5checksum" : "f92e97e547625c91737495137cd306c7"
  } ]
}`)

	return output.SetBody(ioutil.NopCloser(reader)), nil
}

func TestFetchManifest(t *testing.T) {
	svc := &mockS3Client{}
	manifest, err := fetchManifest(svc, "s3://example-bucket/example-path/manifest.json")
	if err != nil {
		t.Fatalf("failed to fetch manifest: %v", err)
	}
	if len(manifest.Files) == 0 {
		t.Fatalf("could not find files in manifest")
	}
}
