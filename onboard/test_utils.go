package onboard

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/catalog"
	"regexp"
	"sort"
	"testing"
)

const (
	NewManifestURL      = "s3://manifest-new.json"
	PreviousManifestURL = "s3://manifest-prev.json"
)

type mockInventory struct {
	IInventory
	manifestURL      string
	manifestLoaded   bool
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

// convenience converter functions
func keys(rows []FileRow) []string {
	res := make([]string, 0, len(rows))
	for _, row := range rows {
		res = append(res, row.Key)
	}
	return res
}

func files(keys ...string) []File {
	res := make([]File, 0, len(keys))
	for _, key := range keys {
		res = append(res, File{Key: key})
	}
	return res
}

func rows(keys ...string) []FileRow {
	res := make([]FileRow, 0, len(keys))
	for _, key := range keys {
		res = append(res, FileRow{Key: key, IsLatest: true, IsDeleteMarker: false, Size: swag.Int64(0)})
	}
	return res
}

func (m *mockCatalogActions) createAndDeleteObjects(_ context.Context, objects []FileRow, objectsToDelete []FileRow) (err error) {
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
	match, _ := regexp.MatchString("s3://manifest[0-9]+.json", manifestURL)
	return match || manifestURL == NewManifestURL || manifestURL == PreviousManifestURL
}

func (m *mockInventory) LoadManifest() error {
	if !manifestExists(m.manifestURL) {
		return errors.New("manifest not found")
	}
	m.manifestLoaded = true
	return nil
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

func (m *mockInventory) Rows() []FileRow {
	if !m.manifestLoaded || !m.inventoryFetched {
		return nil
	}
	return rows(m.rows...)

}

func (m *mockInventory) Manifest() *Manifest {
	return &Manifest{}
}

func (m *mockInventory) ManifestURL() string {
	return m.manifestURL
}

func getInventoryCreator(newManifestURL, previousManifestURL string, newInventory []string, previousInventory []string) func(s3iface.S3API, string) IInventory {
	return func(s3 s3iface.S3API, manifestURL string) IInventory {
		if manifestURL == newManifestURL {
			return &mockInventory{manifestURL: manifestURL, rows: newInventory}
		}
		if manifestURL == previousManifestURL {
			return &mockInventory{manifestURL: manifestURL, rows: previousInventory}
		}
		return &mockInventory{manifestURL: manifestURL}
	}
}

func getSimpleDiffer(t *testing.T) func(leftInv []FileRow, rightInv []FileRow) Diff {
	return func(leftInv []FileRow, rightInv []FileRow) Diff {
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
