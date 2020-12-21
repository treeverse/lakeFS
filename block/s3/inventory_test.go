package s3_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
	"testing"
	"time"

	s3sdk "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/cloud/aws/s3inventory"
	"github.com/treeverse/lakefs/logging"
)

var ErrReadFile = errors.New("error reading file")

func rows(keys []string, lastModified map[string]time.Time) []*s3inventory.InventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]*s3inventory.InventoryObject, len(keys))
	for i, key := range keys {
		if key != "" {
			res[i] = new(s3inventory.InventoryObject)
			res[i].Key = key
			res[i].IsLatest = !strings.Contains(key, "_expired")
			res[i].IsDeleteMarker = strings.Contains(key, "_del")
			if lastModified != nil {
				res[i].LastModified = swag.Time(lastModified[key])
			}
		}
	}
	return res
}

var fileContents = map[string][]string{
	"f1":            {"f1row1_del", "f1row2", "f1row3", "f1row4_del"},
	"f2":            {"f2row1", "f2row2"},
	"f3":            {"f3row1", "f3row2"},
	"f4":            {"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
	"f5":            {"f5row1", "f5row2", "f5row3"},
	"f6":            {"f6row1", "f6row2", "f6row3", "f6row4"},
	"f7":            {"f7row1", "f7row2_del", "f7row3_del", "f7row4_del", "f7row5_del", "f7row6_del", "f7row7_del", "f7row8_expired", "f7row9_expired", "f7row10_expired", "f7row11"},
	"err_file1":     {"f8row1", "", "f8row2", "f8row3"},
	"err_file2":     {""},
	"unsorted_file": {"f9row1", "f9row2", "f9row3", "f9row5", "f9row4"},
	"all_deleted1":  {"fd1_del1", "fd1_del2", "fd1_del3", "fd1_del4", "fd1_del5", "fd1_del6", "fd1_del7", "fd1_del8"},
	"all_deleted2":  {"fd2_del1", "fd2_del2", "fd2_del3", "fd2_del4", "fd2_del5", "fd2_del6", "fd2_del7", "fd2_del8"},
	"all_deleted3":  {"fd3_del1", "fd3_del2", "fd3_del3", "fd3_del4", "fd3_del5", "fd3_del6", "fd3_del7", "fd3_del8"},
	"all_deleted4":  {"fd4_del1", "fd4_del2", "fd4_del3", "fd4_del4", "fd4_del5", "fd4_del6", "fd4_del7", "fd4_del8"},
	"all_deleted5":  {"fd5_del1", "fd5_del2", "fd5_del3", "fd5_del4", "fd5_del5", "fd5_del6", "fd5_del7", "fd5_del8"},
	"all_deleted6":  {"fd6_del1", "fd6_del2", "fd6_del3", "fd6_del4", "fd6_del5", "fd6_del6", "fd6_del7", "fd6_del8"},
	"all_deleted7":  {"fd7_del1", "fd7_del2", "fd7_del3", "fd7_del4", "fd7_del5", "fd7_del6", "fd7_del7", "fd7_del8"},
	"all_deleted8":  {"fd8_del1", "fd8_del2", "fd8_del3", "fd8_del4", "fd8_del5", "fd8_del6", "fd8_del7", "fd8_del8"},
	"all_deleted9":  {"fd9_del1", "fd9_del2", "fd9_del3", "fd9_del4", "fd9_del5", "fd9_del6", "fd9_del7", "fd9_del8"},
	"empty_file":    {},
	"f_overlap1":    {"fo_row1", "fo_row3", "fo_row5"},
	"f_overlap2":    {"fo_row2", "fo_row4"},
	"f_overlap3":    {"fo_row2", "fo_row6"},
	"f_overlap4":    {"fo_row1", "fo_row4"},
	"f_overlap5":    {"fo_row2", "fo_row4"},
	"f1_prefix":     {"a1", "a2", "b1", "b2"},
	"f2_prefix":     {"b3", "b4", "c1", "c2"},
	"f3_prefix":     {"d1", "d2", "e1", "e2"},
	"f4_prefix":     {"a1", "a2", "b1", "b2", "c1", "c2", "d1", "d2"},
	"f5_prefix":     {"e1", "e2", "f1", "f2", "g1", "g2", "h1", "h2"},
}

func TestIterator(t *testing.T) {
	now := time.Now()
	lastModified := make(map[string]time.Time)
	for _, rows := range fileContents {
		for i, r := range rows {
			lastModified[r] = now.Add(time.Hour * time.Duration(-i))
		}
	}
	testdata := map[string]struct {
		InventoryFiles             []string
		ExpectedObjects            []string
		Prefixes                   []string
		ErrExpected                error
		ExpectedCountReadRows      int
		ExpectedCountGetFileReader int
		ShouldSort                 bool
	}{
		"new inventory": {
			InventoryFiles:  []string{"f1", "f2", "f3"},
			ExpectedObjects: []string{"f1row2", "f1row3", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		"new inventory - sort before": {
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ShouldSort:      true,
			ExpectedObjects: []string{"f1row2", "f1row3", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		"new inventory - unsorted": {
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ExpectedObjects: []string{"f3row1", "f3row2", "f2row1", "f2row2", "f1row2", "f1row3"},
		},
		"empty inventory": {
			InventoryFiles:  []string{},
			ExpectedObjects: []string{},
		},
		"single file": {
			InventoryFiles:  []string{"f4"},
			ExpectedObjects: []string{"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
		},
		"unsorted inventory file": {
			InventoryFiles: []string{"f1", "unsorted_file"},
			ErrExpected:    s3.ErrInventoryNotSorted,
			ShouldSort:     true,
		},
		"file with error": {
			InventoryFiles: []string{"f5", "err_file1"},
			ErrExpected:    ErrReadFile,
		},
		"file with error in the middle": {
			InventoryFiles: []string{"f1", "f2", "f3", "f4", "f5", "f6", "err_file2"},
			ErrExpected:    ErrReadFile,
		},
		"file with many deletions": {
			InventoryFiles:  []string{"f7"},
			ExpectedObjects: []string{"f7row1", "f7row11"},
		},
		"inventory with everything deleted": {
			InventoryFiles:  []string{"all_deleted1", "all_deleted2", "all_deleted3"},
			ExpectedObjects: []string{},
		},
		"many files with everything deleted": {
			InventoryFiles:  []string{"all_deleted1", "all_deleted2", "f1", "all_deleted3", "all_deleted4", "all_deleted5", "all_deleted6", "all_deleted7", "f2", "all_deleted8", "all_deleted9"},
			ExpectedObjects: []string{"f1row2", "f1row3", "f2row1", "f2row2"},
		},
		"many files with everything deleted - sort before test": {
			InventoryFiles:  []string{"all_deleted1", "all_deleted2", "f2", "all_deleted3", "all_deleted4", "all_deleted5", "all_deleted6", "all_deleted7", "f1", "all_deleted8", "all_deleted9"},
			ExpectedObjects: []string{"f1row2", "f1row3", "f2row1", "f2row2"},
			ShouldSort:      true,
		},
		"empty file": {
			InventoryFiles:  []string{"empty_file"},
			ExpectedObjects: []string{},
		},
		"overlapping inventory files": {
			InventoryFiles: []string{"f_overlap1", "f_overlap2"},
			ShouldSort:     true,
			ErrExpected:    s3.ErrInventoryFilesRangesOverlap,
		},
		"overlapping inventory files - type 2": {
			InventoryFiles: []string{"f_overlap1", "f_overlap3"},
			ShouldSort:     true,
			ErrExpected:    s3.ErrInventoryFilesRangesOverlap,
		},
		"overlapping inventory files - type 3": {
			InventoryFiles: []string{"f_overlap1", "f_overlap4"},
			ShouldSort:     true,
			ErrExpected:    s3.ErrInventoryFilesRangesOverlap,
		},
		"overlapping inventory files - type 4": {
			InventoryFiles: []string{"f_overlap4", "f_overlap5"},
			ShouldSort:     true,
			ErrExpected:    s3.ErrInventoryFilesRangesOverlap,
		},
		"import with prefix": {
			InventoryFiles:             []string{"f1_prefix", "f2_prefix"},
			Prefixes:                   []string{"b"},
			ExpectedObjects:            []string{"b1", "b2", "b3", "b4"},
			ExpectedCountReadRows:      8,
			ExpectedCountGetFileReader: 2,
		},
		"import with prefix - skip entire file": {
			InventoryFiles:             []string{"f1_prefix", "f2_prefix", "f3_prefix"},
			Prefixes:                   []string{"b"},
			ExpectedObjects:            []string{"b1", "b2", "b3", "b4"},
			ExpectedCountReadRows:      8,
			ExpectedCountGetFileReader: 2,
		},
		"import with prefix - skip first file": {
			InventoryFiles:             []string{"f1", "f2", "f3"},
			Prefixes:                   []string{"f2", "f3"},
			ExpectedObjects:            []string{"f2row1", "f2row2", "f3row1", "f3row2"},
			ExpectedCountReadRows:      4,
			ExpectedCountGetFileReader: 2,
		},
		"import with prefix - unsorted prefixes": {
			InventoryFiles:             []string{"f1", "f2", "f3"},
			Prefixes:                   []string{"f3", "f2"},
			ExpectedObjects:            []string{"f2row1", "f2row2", "f3row1", "f3row2"},
			ExpectedCountReadRows:      4,
			ExpectedCountGetFileReader: 2,
		},
		"import with prefix - unsorted inventory": {
			InventoryFiles:             []string{"f3", "f2", "f1"},
			Prefixes:                   []string{"f2", "f3"},
			ExpectedObjects:            []string{"f2row1", "f2row2", "f3row1", "f3row2"},
			ShouldSort:                 true,
			ExpectedCountReadRows:      4,
			ExpectedCountGetFileReader: 2,
		},
		"import with prefix - prefix in middle": {
			InventoryFiles:             []string{"f4_prefix", "f5_prefix"},
			Prefixes:                   []string{"b", "f"},
			ExpectedObjects:            []string{"b1", "b2", "f1", "f2"},
			ExpectedCountReadRows:      16,
			ExpectedCountGetFileReader: 2,
		},
	}
	manifestURL := "s3://example-bucket/manifest1.json"
	for name, test := range testdata {
		t.Run(name, func(t *testing.T) {

			s3api := &mockS3Client{
				FilesByManifestURL: map[string][]string{manifestURL: test.InventoryFiles},
			}
			reader := &mockInventoryReader{openFiles: make(map[string]bool), lastModified: lastModified}
			inv, err := s3.GenerateInventory(logging.Default(), manifestURL, s3api, reader, test.ShouldSort, test.Prefixes)
			if err != nil {
				if errors.Is(err, test.ErrExpected) {
					return
				}
				t.Fatalf("error: %v", err)
			}
			it := inv.Iterator()
			objects := make([]*block.InventoryObject, 0, len(test.ExpectedObjects))
			for it.Next() {
				objects = append(objects, it.Get())
			}
			if len(reader.openFiles) != 0 {
				t.Errorf("some files stayed open: %v", reader.openFiles)
			}
			if !errors.Is(it.Err(), test.ErrExpected) {
				t.Fatalf("got unexpected error. expected=%v, got=%v.", test.ErrExpected, it.Err())
			}
			if test.ErrExpected != nil {
				return
			}
			if len(objects) != len(test.ExpectedObjects) {
				t.Fatalf("unexpected number of objects in inventory. expected=%d, got=%d", len(test.ExpectedObjects), len(objects))
			}
			if test.ExpectedCountReadRows > 0 && test.ExpectedCountReadRows != reader.countReadRows {
				t.Fatalf("total number of read rows different than expected. expected=%d, got=%d", test.ExpectedCountReadRows, reader.countReadRows)
			}
			if test.ExpectedCountGetFileReader > 0 && test.ExpectedCountGetFileReader != reader.countGetFileReader {
				t.Fatalf("total number of get file reader different than expected. expected=%d, got=%d", test.ExpectedCountGetFileReader, reader.countGetFileReader)
			}
			for i, obj := range objects {
				if obj.Key != test.ExpectedObjects[i] {
					t.Fatalf("at index %d: expected=%s, got=%s", i, test.ExpectedObjects[i], obj.Key)
				}
				if *obj.LastModified != lastModified[obj.Key] {
					t.Fatalf("last modified for object in index %d different than expected. expected=%v, got=%v", i, lastModified[obj.Key], obj.LastModified)
				}
			}
		})
	}
}

type mockInventoryReader struct {
	openFiles          map[string]bool
	lastModified       map[string]time.Time
	countReadRows      int
	countGetFileReader int
}

type mockInventoryFileReader struct {
	rows            []*s3inventory.InventoryObject
	nextIdx         int
	inventoryReader *mockInventoryReader
	key             string
}

func (m *mockInventoryFileReader) FirstObjectKey() string {
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

func (m *mockInventoryFileReader) LastObjectKey() string {
	max := ""
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
	delete(m.inventoryReader.openFiles, m.key)
	return nil
}

func (m *mockInventoryFileReader) Read(n int) ([]*s3inventory.InventoryObject, error) {
	res := make([]*s3inventory.InventoryObject, 0, len(m.rows))
	for i := m.nextIdx; i < len(m.rows) && i < m.nextIdx+n; i++ {
		if m.rows[i] == nil {
			return nil, ErrReadFile // for test - simulate file with error
		}
		res = append(res, m.rows[i])
	}
	m.nextIdx = m.nextIdx + len(res)
	m.inventoryReader.countReadRows += len(res)
	return res, nil
}

func (m *mockInventoryFileReader) GetNumRows() int64 {
	return int64(len(m.rows))
}

func (m *mockInventoryReader) GetFileReader(_ string, _ string, key string) (s3inventory.FileReader, error) {
	m.openFiles[key] = true
	m.countGetFileReader++
	return &mockInventoryFileReader{rows: rows(fileContents[key], m.lastModified), inventoryReader: m, key: key}, nil
}

func (m *mockInventoryReader) GetMetadataReader(_ string, _ string, key string) (s3inventory.MetadataReader, error) {
	m.openFiles[key] = true
	return &mockInventoryFileReader{rows: rows(fileContents[key], m.lastModified), inventoryReader: m, key: key}, nil
}
func (m *mockS3Client) GetObject(input *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error) {
	output := s3sdk.GetObjectOutput{}
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
