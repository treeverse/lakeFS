package onboard_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/onboard"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	NewManifestURL      = "s3://example-bucket/manifest-new.json"
	PreviousManifestURL = "s3://example-bucket/manifest-prev.json"
)

type mockInventoryFactory struct {
	newManifestURL      string
	previousManifestURL string
	newInventory        []string
	previousInventory   []string
	sourceBucket        string
}

func (m mockInventoryFactory) NewInventory(manifestURL string) (onboard.Inventory, error) {
	if manifestURL == m.newManifestURL {
		return &mockInventory{rows: m.newInventory, manifestURL: manifestURL, sourceBucket: m.sourceBucket}, nil
	}
	if manifestURL == m.previousManifestURL {
		return &mockInventory{rows: m.previousInventory, manifestURL: manifestURL, sourceBucket: m.sourceBucket}, nil
	}
	return nil, errors.New("failed to create inventory")
}

type mockInventory struct {
	inventoryFetched bool
	rows             []string
	manifestURL      string
	sourceBucket     string
}

type objectActions struct {
	Added   []string
	Deleted []string
}

type mockCatalogActions struct {
	onboard.RepoActions
	previousCommitManifest string
	objectActions          objectActions
	lastCommitMetadata     catalog.Metadata
}

type mockS3Client struct {
	s3iface.S3API
	FilesByManifestURL map[string][]string
	DestBucket         string
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	output := s3.GetObjectOutput{}
	manifestURL := fmt.Sprintf("s3://%s%s", *input.Bucket, *input.Key)
	if !manifestExists(manifestURL) {
		return &output, nil
	}
	manifestFileNames := m.FilesByManifestURL[manifestURL]
	if manifestFileNames == nil {
		manifestFileNames = []string{"inventory/lakefs-example-data/my_inventory/data/ea8268b2-a6ba-42de-8694-91a9833b4ff1.parquet"}
	}
	manifestFiles := make([]onboard.ManifestFile, len(manifestFileNames))
	for _, filename := range manifestFileNames {
		manifestFiles = append(manifestFiles, onboard.ManifestFile{
			Key: filename,
		})
	}
	filesJSON, err := json.Marshal(manifestFiles)
	if err != nil {
		return nil, err
	}
	destBucket := m.DestBucket
	if m.DestBucket == "" {
		destBucket = "yoni-test3"
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

// convenience converter functions
func keys(rows []onboard.InventoryObject) []string {
	if rows == nil {
		return nil
	}
	res := make([]string, 0, len(rows))
	for _, row := range rows {
		res = append(res, row.Key)
	}
	return res
}

func files(keys ...string) []onboard.ManifestFile {
	res := make([]onboard.ManifestFile, 0, len(keys))
	for _, key := range keys {
		res = append(res, onboard.ManifestFile{Key: key})
	}
	return res
}

func rows(keys ...string) []onboard.InventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]onboard.InventoryObject, 0, len(keys))
	for _, key := range keys {
		res = append(res, onboard.InventoryObject{Key: key, IsLatest: true, IsDeleteMarker: false, Size: swag.Int64(0)})
	}
	return res
}

func (m *mockCatalogActions) CreateAndDeleteObjects(_ context.Context, objects []onboard.InventoryObject, objectsToDelete []onboard.InventoryObject) (err error) {
	m.objectActions.Added = append(m.objectActions.Added, keys(objects)...)
	m.objectActions.Deleted = append(m.objectActions.Deleted, keys(objectsToDelete)...)
	return nil
}

func (m *mockCatalogActions) GetPreviousCommit(_ context.Context) (commit *catalog.CommitLog, err error) {
	if m.previousCommitManifest != "" {
		return &catalog.CommitLog{Metadata: catalog.Metadata{"manifest_url": m.previousCommitManifest}}, nil
	}
	return nil, nil
}

func (m *mockCatalogActions) Commit(_ context.Context, _ string, metadata catalog.Metadata) error {
	m.lastCommitMetadata = metadata
	return nil
}

func manifestExists(manifestURL string) bool {
	match, _ := regexp.MatchString("s3://example-bucket/manifest[0-9]+.json", manifestURL)
	return match || manifestURL == NewManifestURL || manifestURL == PreviousManifestURL
}

func (m *mockInventory) Fetch(_ context.Context, sorted bool) error {
	if !manifestExists(m.manifestURL) {
		return errors.New("manifest not found")
	}
	if sorted {
		sort.Strings(m.rows)
	}
	m.inventoryFetched = true
	return nil
}

func (m *mockInventory) Objects() []onboard.InventoryObject {
	if !m.inventoryFetched {
		return nil
	}
	return rows(m.rows...)

}

func (m *mockInventory) SourceName() string {
	return m.sourceBucket
}
func (m *mockInventory) CreateCommitMetadata(diff onboard.InventoryDiff) catalog.Metadata {
	return catalog.Metadata{
		"manifest_url":             m.manifestURL,
		"source_bucket":            m.sourceBucket,
		"added_or_changed_objects": strconv.Itoa(len(diff.AddedOrChanged)),
		"deleted_objects":          strconv.Itoa(len(diff.Deleted)),
	}
}

func getSimpleDiffer(t *testing.T) func(leftInv []onboard.InventoryObject, rightInv []onboard.InventoryObject) *onboard.InventoryDiff {
	return func(leftInv []onboard.InventoryObject, rightInv []onboard.InventoryObject) *onboard.InventoryDiff {
		if !sort.StringsAreSorted(keys(leftInv)) || !sort.StringsAreSorted(keys(rightInv)) {
			t.Fatalf("inventory expected to be sorted at this point")
		}
		// inefficient diff
		diff := onboard.InventoryDiff{}
		for _, o1 := range leftInv {
			found := false
			for _, o2 := range rightInv {
				if o1.Key == o2.Key {
					found = true
					break
				}
			}
			if !found {
				diff.Deleted = append(diff.Deleted, o1)
			}
		}
		for _, o2 := range rightInv {
			found := false
			for _, o1 := range leftInv {
				if o1.Key == o2.Key {
					found = true
					break
				}
			}
			if !found {
				diff.AddedOrChanged = append(diff.AddedOrChanged, o2)
			}
		}
		return &diff
	}
}
