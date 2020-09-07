package s3_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/go-openapi/swag"
	inventorys3 "github.com/treeverse/lakefs/inventory/s3"
	"github.com/treeverse/lakefs/logging"

	s32 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block/s3"
)

var ErrReadFile = errors.New("error reading file")

func rows(keys ...string) []*inventorys3.InventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]*inventorys3.InventoryObject, len(keys))
	for i, key := range keys {
		if key != "" {
			res[i] = new(inventorys3.InventoryObject)
			res[i].Key = key
			res[i].IsLatest = swag.Bool(!strings.HasPrefix(key, "expired_"))
			res[i].IsDeleteMarker = swag.Bool(strings.HasPrefix(key, "del_"))
		}
	}
	return res
}

var fileContents = map[string][]string{
	"f1":            {"del_1", "f1row1", "f1row2", "del_2"},
	"f2":            {"f2row1", "f2row2"},
	"f3":            {"f3row1", "f3row2"},
	"f4":            {"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
	"f5":            {"f5row1", "f5row2", "f5row3"},
	"f6":            {"f6row1", "f6row2", "f6row3", "f6row4"},
	"f7":            {"f7row1", "del_1", "del_2", "del_3", "del_4", "del_5", "del_6", "expired_1", "expired_2", "expired_3", "f7row2"},
	"err_file1":     {"f8row1", "", "f8row2", "f8row3"},
	"err_file2":     {""},
	"unsorted_file": {"f9row1", "f9row2", "f9row3", "f9row5", "f9row4"},
	"all_deleted":   {"del_1", "del_2", "del_3", "del_4", "del_5", "del_6", "del_7", "del_8"},
	"empty_file":    {},
}

func TestIterator(t *testing.T) {
	testdata := []struct {
		InventoryFiles  []string
		ExpectedObjects []string
		ErrExpected     error
		ShouldSort      bool
	}{
		{
			InventoryFiles:  []string{"f1", "f2", "f3"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ShouldSort:      true,
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
			ExpectedObjects: []string{"f5row1", "f5row2", "f5row3", "f6row1", "f6row2", "f6row3", "f6row4"},
		},
		{
			InventoryFiles: []string{"f1", "unsorted_file"},
			ErrExpected:    s3.ErrInventoryNotSorted,
			ShouldSort:     true,
		},
		{
			InventoryFiles: []string{"f5", "err_file1"},
			ErrExpected:    ErrReadFile,
		},
		{
			InventoryFiles: []string{"f1", "f2", "f3", "f4", "f5", "f6", "err_file2"},
			ErrExpected:    ErrReadFile,
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
			ShouldSort:      true,
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
			reader := &mockInventoryReader{openFiles: make(map[string]bool)}
			inv, err := s3.GenerateInventory(logging.Default(), manifestURL, s3api, reader, test.ShouldSort)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			it := inv.Iterator()
			it.(*s3.InventoryIterator).ReadBatchSize = batchSize
			objects := make([]string, 0, len(test.ExpectedObjects))
			for it.Next() {
				objects = append(objects, it.Get().Key)
			}
			if len(reader.openFiles) != 0 {
				t.Fatalf("some files stayed open: %v", reader.openFiles)
			}
			if test.ErrExpected == nil && it.Err() != nil {
				t.Fatalf("got unexpected error: %v. expected no error", it.Err())
			}
			if test.ErrExpected != nil {
				if it.Err() == nil {
					t.Fatalf("expected error but didn't get one")
				}
				if !errors.Is(it.Err(), test.ErrExpected) {
					t.Fatalf("got unexpected error. expected=%v. got=%v", test.ErrExpected, it.Err())
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

type mockInventoryReader struct {
	openFiles map[string]bool
}

type mockInventoryFileReader struct {
	rows    []*inventorys3.InventoryObject
	nextIdx int
	mgr     *mockInventoryReader
	key     string
}

func (m *mockInventoryFileReader) MinValue() string {
	if len(m.rows) == 0 {
		return ""
	}
	min := m.rows[0].Key
	for _, r := range m.rows {
		if r.Key < min {
			min = r.Key
		}
	}
	return min
}

func (m *mockInventoryFileReader) MaxValue() string {
	if len(m.rows) == 0 {
		return ""
	}
	max := m.rows[0].Key
	for _, r := range m.rows {
		if r.Key > max {
			max = r.Key
		}
	}
	return max
}

func (m *mockInventoryFileReader) Close() error {
	m.nextIdx = -1
	m.rows = nil
	delete(m.mgr.openFiles, m.key)
	return nil
}

func (m *mockInventoryFileReader) Read(dstInterface interface{}) error {
	res := make([]inventorys3.InventoryObject, 0, len(m.rows))
	dst := dstInterface.(*[]inventorys3.InventoryObject)
	for i := m.nextIdx; i < len(m.rows) && i < m.nextIdx+len(*dst); i++ {
		if m.rows[i] == nil {
			return ErrReadFile // for test - simulate file with error
		}
		res = append(res, *m.rows[i])
	}
	m.nextIdx = m.nextIdx + len(res)
	*dst = res
	return nil
}

func (m *mockInventoryFileReader) GetNumRows() int64 {
	return int64(len(m.rows))
}
func (m *mockInventoryFileReader) SkipRows(skip int64) error {
	m.nextIdx += int(skip)
	if m.nextIdx > len(m.rows) {
		return fmt.Errorf("index out of bounds after skip. got index=%d, length=%d", m.nextIdx, len(m.rows))
	}
	return nil
}

func (m *mockInventoryReader) GetFileReader(_ string, _ string, key string) (inventorys3.FileReader, error) {
	m.openFiles[key] = true
	return &mockInventoryFileReader{rows: rows(fileContents[key]...), mgr: m, key: key}, nil
}

func (m *mockInventoryReader) GetMetadataReader(_ string, _ string, key string) (inventorys3.MetadataReader, error) {
	m.openFiles[key] = true
	return &mockInventoryFileReader{rows: rows(fileContents[key]...), mgr: m, key: key}, nil
}
func (m *mockS3Client) GetObject(input *s32.GetObjectInput) (*s32.GetObjectOutput, error) {
	output := s32.GetObjectOutput{}
	manifestURL := fmt.Sprintf("s3://%s%s", *input.Bucket, *input.Key)
	if !manifestExists(manifestURL) {
		return &output, nil
	}
	inventoryFileNames := m.FilesByManifestURL[manifestURL]
	if inventoryFileNames == nil {
		inventoryFileNames = []string{"inventory/lakefs-example-data/my_inventory/data/ea8268b2-a6ba-42de-8694-91a9833b4ff1.parquet"}
	}
	inventoryFiles := make([]interface{}, 0, len(inventoryFileNames))
	for _, filename := range inventoryFileNames {
		inventoryFiles = append(inventoryFiles, struct {
			Key string `json:"key"`
		}{
			Key: filename,
		})
	}
	filesJSON, err := json.Marshal(inventoryFiles)
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
