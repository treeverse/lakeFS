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

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/logging"

	s32 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block/s3"
)

func rows(keys ...string) []*s3.InventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]*s3.InventoryObject, len(keys))
	for i, key := range keys {
		if key != "" {
			res[i] = new(s3.InventoryObject)
			res[i].Key = key
			res[i].IsLatest = swag.Bool(!strings.HasPrefix(key, "expired_"))
			res[i].IsDeleteMarker = swag.Bool(strings.HasPrefix(key, "del_"))
		}
	}
	return res
}

var fileContents = map[string][]string{
	"f1":          {"del_1", "f1row1", "f1row2", "del_2"},
	"f2":          {"f2row1", "f2row2"},
	"f3":          {"f3row1", "f3row2"},
	"f4":          {"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
	"f5":          {"a1", "a2", "a3"},
	"f6":          {"a4", "a5", "a6", "a7"},
	"f7":          {"f7row1", "del_1", "del_2", "del_3", "del_4", "del_5", "del_6", "expired_1", "expired_2", "expired_3", "f7row2"},
	"err_file1":   {"a4", "", "a6", "a7"},
	"err_file2":   {""},
	"all_deleted": {"del_1", "del_2", "del_3", "del_4", "del_5", "del_6", "del_7", "del_8"},
	"empty_file":  {},
}

func TestIterator(t *testing.T) {
	testdata := []struct {
		InventoryFiles  []string
		ExpectedObjects []string
		ErrExpected     bool
	}{
		{
			InventoryFiles:  []string{"f1", "f2", "f3"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{},
			ExpectedObjects: []string{},
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
			InventoryFiles:  []string{"f5", "f6"},
			ExpectedObjects: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
		{
			InventoryFiles:  []string{"f6", "f5"},
			ExpectedObjects: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
		{
			InventoryFiles: []string{"f5", "err_file1"},
			ErrExpected:    true,
		},
		{
			InventoryFiles: []string{"f1,", "f2", "f3", "f4", "f5", "f6", "err_file2"},
			ErrExpected:    true,
		},
		{
			InventoryFiles:  []string{"f7"},
			ExpectedObjects: []string{"f7row1", "f7row2"},
		},
		{
			InventoryFiles:  []string{"all_deleted", "all_deleted", "all_deleted"},
			ExpectedObjects: []string{},
		},
		{
			InventoryFiles:  []string{"all_deleted", "all_deleted", "f1", "all_deleted", "all_deleted", "all_deleted", "all_deleted", "all_deleted", "f2", "all_deleted", "all_deleted"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2"},
		},
		{
			InventoryFiles:  []string{"all_deleted", "all_deleted", "f2", "all_deleted", "all_deleted", "all_deleted", "all_deleted", "all_deleted", "f1", "all_deleted", "all_deleted"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2"},
		},
		{
			InventoryFiles:  []string{"empty_file"},
			ExpectedObjects: []string{},
		},
	}

	manifestURL := "s3://example-bucket/manifest1.json"
	for _, test := range testdata {
		for _, batchSize := range []int{1, 2, 3, 4, 5, 7, 9, 11, 15, 100, 1000, 10000} {
			s3api := &mockS3Client{
				FilesByManifestURL: map[string][]string{manifestURL: test.InventoryFiles},
			}
			inv, err := s3.GenerateInventory(context.Background(), logging.Default(), manifestURL, s3api)
			if !test.ErrExpected && err != nil {
				t.Fatalf("error: %v", err)
			} else if err != nil {
				continue
			}
			it := inv.Iterator()
			it.(*s3.InventoryIterator).Reader = &mockInventoryReader{}
			it.(*s3.InventoryIterator).ReadBatchSize = batchSize
			objects := make([]string, 0, len(test.ExpectedObjects))
			for it.Next() {
				objects = append(objects, it.Get().Key)
			}
			if !test.ErrExpected && it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if test.ErrExpected {
				if it.Err() == nil {
					print(len(test.ExpectedObjects))
					t.Fatalf("expected error but didn't get one")
				}
				continue
			}
			if len(objects) != len(test.ExpectedObjects) {
				t.Fatalf("unexpected number of objects in inventory. expected=%d, got=%d", len(test.ExpectedObjects), len(objects))
			}
			if !reflect.DeepEqual(objects, test.ExpectedObjects) {
				t.Fatalf("objects in inventory differrent than expected. expected=%v, got=%v", test.ExpectedObjects, objects)
			}
		}
	}
}

type mockParquetReader struct {
	rows    []*s3.InventoryObject
	nextIdx int
}

func (m *mockParquetReader) Close() error {
	m.nextIdx = -1
	m.rows = nil
	return nil
}

func (m *mockParquetReader) Read(dstInterface interface{}) error {
	res := make([]s3.InventoryObject, 0, len(m.rows))
	dst := dstInterface.(*[]s3.InventoryObject)
	for i := m.nextIdx; i < len(m.rows) && i < m.nextIdx+len(*dst); i++ {
		if m.rows[i] == nil {
			return fmt.Errorf("got empty key") // for test - simulate file with error
		}
		res = append(res, *m.rows[i])
	}
	m.nextIdx = m.nextIdx + len(res)
	*dst = res
	return nil
}

func (m *mockParquetReader) GetNumRows() int64 {
	return int64(len(m.rows))
}
func (m *mockParquetReader) SkipRows(skip int64) error {
	m.nextIdx += int(skip)
	if m.nextIdx > len(m.rows) {
		return fmt.Errorf("index out of bounds after skip. got index=%d, length=%d", m.nextIdx, len(m.rows))
	}
	return nil
}

type mockInventoryReader struct{}

func (m *mockInventoryReader) GetManifestFileReader(_ context.Context, _ s3.Manifest, key string) (s3.ManifestFileReader, error) {
	return &mockParquetReader{rows: rows(fileContents[key]...)}, nil
}
func (m *mockInventoryReader) Close(_ s3.Manifest) error {
	return nil
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
	manifestFiles := make([]interface{}, 0, len(manifestFileNames))
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
