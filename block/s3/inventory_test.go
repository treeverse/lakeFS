package s3_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"regexp"
	"strings"
	"testing"

	s32 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/s3"
)

// convenience converter functions
func keys(rows []block.InventoryObject) []string {
	if rows == nil {
		return nil
	}
	res := make([]string, 0, len(rows))
	for _, row := range rows {
		res = append(res, row.Key)
	}
	return res
}

func rows(keys ...string) []s3.ParquetInventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]s3.ParquetInventoryObject, len(keys))
	latest := true
	for i, key := range keys {
		res[i].Key = key
		res[i].IsLatest = &latest
	}
	return res
}

func mockReadRows(_ context.Context, _ s3iface.S3API, inventoryBucketName string, manifestFileKey string) ([]s3.ParquetInventoryObject, error) {
	if inventoryBucketName != "example-bucket" {
		return nil, fmt.Errorf("wrong bucket name: %s", inventoryBucketName)
	}
	return rows(fileContents[manifestFileKey]...), nil
}

var fileContents = map[string][]string{
	"f1": {"f1row1", "f1row2"},
	"f2": {"f2row1", "f2row2"},
	"f3": {"f3row1", "f3row2"},
	"f4": {"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
	"f5": {"a1", "a3", "a5"},
	"f6": {"a2", "a4", "a6", "a7"},
}

func TestFetch(t *testing.T) {
	testdata := []struct {
		InventoryFiles  []string
		Sort            bool
		ExpectedObjects []string
	}{
		{
			InventoryFiles:  []string{"f1", "f2", "f3"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{},
			ExpectedObjects: nil,
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ExpectedObjects: []string{"f3row1", "f3row2", "f2row1", "f2row2", "f1row1", "f1row2"},
		},
		{
			InventoryFiles:  []string{"f4"},
			ExpectedObjects: []string{"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
		},
		{
			InventoryFiles:  []string{"f1", "f4"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			Sort:            true,
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{"f5", "f6"},
			Sort:            true,
			ExpectedObjects: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
	}

	manifestURL := "s3://example-bucket/manifest1.json"
	for _, test := range testdata {
		inv, err := s3.GenerateInventory(manifestURL, &mockS3Client{
			FilesByManifestURL: map[string][]string{manifestURL: test.InventoryFiles},
		})
		s3Inv := inv.(*s3.Inventory)
		s3Inv.RowReader = mockReadRows
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		objects, err := inv.Objects(context.Background(), test.Sort)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if len(objects) != len(test.ExpectedObjects) {
			t.Fatalf("unexpected number of objects in inventory. expected=%d, got=%d", len(test.ExpectedObjects), len(objects))
		}
		if !reflect.DeepEqual(keys(objects), test.ExpectedObjects) {
			t.Fatalf("objects in inventory differrent than expected. expected=%v, got=%v", test.ExpectedObjects, keys(objects))
		}
	}
}

func (m *mockS3Client) GetObject(input *s32.GetObjectInput) (*s32.GetObjectOutput, error) {
	output := s32.GetObjectOutput{}
	manifestURL := fmt.Sprintf("s3://%s%s", *input.Bucket, *input.Key)
	if !manifestExists(manifestURL) {
		return &output, nil
	}
	manifestFileNames := m.FilesByManifestURL[manifestURL]
	if manifestFileNames == nil {
		manifestFileNames = []string{"inventory/lakefs-example-data/my_inventory/data/ea8268b2-a6ba-42de-8694-91a9833b4ff1.parquet"}
	}
	manifestFiles := make([]interface{}, len(manifestFileNames))
	for _, filename := range manifestFileNames {
		manifestFiles = append(manifestFiles, struct {
			Key string `json:"key"`
		}{
			Key: filename,
		})
	}
	filesJSON, err := json.Marshal(manifestFiles)
	if err != nil {
		return nil, err
	}
	destBucket := m.DestBucket
	if m.DestBucket == "" {
		destBucket = "example-bucket"
	}
	reader := strings.NewReader(fmt.Sprintf(`{
  "sourceBucket" : "lakefs-example-data",
  "destinationBucket" : "arn:aws:s3:::%s",
  "version" : "2016-11-30",
  "creationTimestamp" : "1593216000000",
  "fileFormat" : "Parquet",
  "fileSchema" : "message s3.inventory {  required binary bucket (STRING);  required binary key (STRING);  optional binary version_id (STRING);  optional boolean is_latest;  optional boolean is_delete_marker;  optional int64 size;  optional int64 last_modified_date (TIMESTAMP(MILLIS,true));  optional binary e_tag (STRING);  optional binary storage_class (STRING);  optional boolean is_multipart_uploaded;}",
  "files" : %s}`, destBucket, filesJSON))
	return output.SetBody(ioutil.NopCloser(reader)), nil
}

type mockS3Client struct {
	s3iface.S3API
	FilesByManifestURL map[string][]string
	DestBucket         string
}

func manifestExists(manifestURL string) bool {
	match, _ := regexp.MatchString("s3://example-bucket/manifest[0-9]+.json", manifestURL)
	return match
}
