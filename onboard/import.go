package onboard

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/catalog"
	"strconv"
)

const (
	LauncherBranchName        = "launcher"
	LauncherCommitMsgTemplate = "Import from %s"
)

type Importer struct {
	s3               s3iface.S3API
	repository       string
	inventory        IInventory
	batchSize        int
	inventoryCreator func(s3 s3iface.S3API, manifestURL string) IInventory
	inventoryDiffer  func(leftInv []FileRow, rightInv []FileRow) Diff
	catalogActions   ICatalogActions
}

func NewImporter(s3 s3iface.S3API, cataloger catalog.Cataloger, manifestURL string, repository string) *Importer {
	res := &Importer{s3: s3, repository: repository, inventory: NewInventory(s3, manifestURL), batchSize: DefaultBatchSize, inventoryCreator: NewInventory, inventoryDiffer: InventoryDiff}
	res.catalogActions = NewCatalogActions(cataloger, repository)
	return res
}

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (diff Diff, err error) {
	previousManifestURL := commit.Metadata["manifest_url"]
	if previousManifestURL == "" {
		err = fmt.Errorf("no manifest_url in commit Metadata. commit_ref=%s", commit.Reference)
		return
	}
	previousInv := s.inventoryCreator(s.s3, previousManifestURL)
	err = previousInv.LoadManifest()
	if err != nil {
		return
	}
	err = previousInv.Fetch(ctx, true)
	if err != nil {
		return
	}
	err = s.inventory.Fetch(ctx, true)
	if err != nil {
		return
	}
	diff = s.inventoryDiffer(previousInv.Rows(), s.inventory.Rows())
	return
}

func (s *Importer) createMetadata(addedCount, deletedCount int) catalog.Metadata {
	return catalog.Metadata{
		"manifest_url":             s.inventory.ManifestURL(),
		"source_bucket":            s.inventory.Manifest().SourceBucket,
		"added_or_changed_objects": strconv.Itoa(addedCount),
		"deleted_objects":          strconv.Itoa(deletedCount),
	}
}

func (s *Importer) Import(ctx context.Context) error {
	var rows, rowsToDelete []FileRow
	var diff Diff
	commit, err := s.catalogActions.getPreviousCommit(ctx)
	if err != nil {
		return err
	}
	err = s.inventory.LoadManifest()
	if err != nil {
		return err
	}

	if commit == nil {
		// no previous commit, add whole inventory
		err = s.inventory.Fetch(ctx, false)
		if err != nil {
			return err
		}
		rows = s.inventory.Rows()
	} else {
		// has previous commit, add/delete according to diff
		diff, err = s.diffFromCommit(ctx, *commit)
		if err != nil {
			return err
		}
		rows = diff.AddedOrChanged
		rowsToDelete = diff.Deleted
	}
	err = s.catalogActions.createAndDeleteObjects(ctx, rows, rowsToDelete)
	if err != nil {
		return err
	}
	commitMetadata := s.createMetadata(len(rows), len(rowsToDelete))
	return s.catalogActions.commit(ctx, fmt.Sprintf(LauncherCommitMsgTemplate, s.inventory.Manifest().SourceBucket), commitMetadata)
}
