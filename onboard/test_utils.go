package onboard

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/catalog"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	NewManifestURL      = "s3://example-bucket/manifest-new.json"
	PreviousManifestURL = "s3://example-bucket/manifest-prev.json"
)

type mockInventory struct {
	IInventory
	manifest         *Manifest
	inventoryFetched bool
	rows             []string
}

type objectActions struct {
	Added   []string
	Deleted []string
}

type mockCatalogActions struct {
	ICatalogActions
	previousCommitManifest string
	objectActions          objectActions
	lastCommitMetadata     catalog.Metadata
}

type mockS3Client struct {
	s3iface.S3API
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	output := s3.GetObjectOutput{}
	if !manifestExists(fmt.Sprintf("s3://%s%s", *input.Bucket, *input.Key)) {
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

// convenience converter functions
func keys(rows []InventoryObject) []string {
	res := make([]string, 0, len(rows))
	for _, row := range rows {
		res = append(res, row.Key)
	}
	return res
}

func files(keys ...string) []ManifestFile {
	res := make([]ManifestFile, 0, len(keys))
	for _, key := range keys {
		res = append(res, ManifestFile{Key: key})
	}
	return res
}

func rows(keys ...string) []InventoryObject {
	res := make([]InventoryObject, 0, len(keys))
	for _, key := range keys {
		res = append(res, InventoryObject{Key: key, IsLatest: true, IsDeleteMarker: false, Size: swag.Int64(0)})
	}
	return res
}

func (m *mockCatalogActions) createAndDeleteObjects(_ context.Context, objects []InventoryObject, objectsToDelete []InventoryObject) (err error) {
	m.objectActions.Added = append(m.objectActions.Added, keys(objects)...)
	m.objectActions.Deleted = append(m.objectActions.Deleted, keys(objectsToDelete)...)
	return nil
}

func (m *mockCatalogActions) getPreviousCommit(_ context.Context) (commit *catalog.CommitLog, err error) {
	if m.previousCommitManifest != "" {
		return &catalog.CommitLog{Metadata: catalog.Metadata{"manifest_url": m.previousCommitManifest}}, nil
	}
	return nil, nil
}

func (m *mockCatalogActions) commit(_ context.Context, _ string, metadata catalog.Metadata) error {
	m.lastCommitMetadata = metadata
	return nil
}

func manifestExists(manifestURL string) bool {
	match, _ := regexp.MatchString("s3://example-bucket/manifest[0-9]+.json", manifestURL)
	return match || manifestURL == NewManifestURL || manifestURL == PreviousManifestURL
}

func (m *mockInventory) Fetch(_ context.Context, sorted bool) error {
	if !manifestExists(m.manifest.URL) {
		return errors.New("manifest not found")
	}
	if sorted {
		sort.Strings(m.rows)
	}
	m.inventoryFetched = true
	return nil
}

func (m *mockInventory) Objects() []InventoryObject {
	if !m.inventoryFetched {
		return nil
	}
	return rows(m.rows...)

}

func (m *mockInventory) Manifest() *Manifest {
	return m.manifest
}

func getInventoryCreator(newManifestURL, previousManifestURL string, newInventory []string, previousInventory []string) func(s3iface.S3API, *Manifest) IInventory {
	return func(s3 s3iface.S3API, manifest *Manifest) IInventory {
		if manifest.URL == newManifestURL {
			return &mockInventory{manifest: manifest, rows: newInventory}
		}
		if manifest.URL == previousManifestURL {
			return &mockInventory{manifest: manifest, rows: previousInventory}
		}
		return &mockInventory{manifest: manifest}
	}
}

func getSimpleDiffer(t *testing.T) func(leftInv []InventoryObject, rightInv []InventoryObject) Diff {
	return func(leftInv []InventoryObject, rightInv []InventoryObject) Diff {
		if !sort.StringsAreSorted(keys(leftInv)) || !sort.StringsAreSorted(keys(rightInv)) {
			t.Fatalf("inventory expected to be sorted at this point")
		}
		// inefficient diff
		diff := Diff{}
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
		return diff
	}
}
