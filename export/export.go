package export

import (
	"context"
	"fmt"

	nanoid "github.com/matoous/go-nanoid"

	"github.com/treeverse/lakefs/parade"

	"github.com/treeverse/lakefs/catalog"
)

func getExportID(repo, branch, commitRef string) (string, error) {
	nid, err := nanoid.Nanoid()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-%s-%s", repo, branch, commitRef, nid), nil
}

// ExportBranchStart inserts a start task on branch, sets branch export state to pending.
// If export already in progress will return error
func ExportBranchStart(paradeDB parade.Parade, cataloger catalog.Cataloger, repo, branch string) (string, error) {
	commit, err := cataloger.GetCommit(context.Background(), repo, branch)
	if err != nil {
		return "", err
	}
	commitRef := commit.Reference
	exportID, err := getExportID(repo, branch, commitRef)
	if err != nil {
		return "", err
	}
	err = cataloger.ExportState(repo, branch, commitRef, func(oldRef string, state catalog.CatalogBranchExportStatus) (newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		config, err := cataloger.GetExportConfigurationForBranch(repo, branch)
		if err != nil {
			return "", nil, err
		}
		tasks, err := GetStartTasks(repo, branch, oldRef, commitRef, exportID, config)
		if err != nil {
			return "", nil, err
		}

		err = paradeDB.InsertTasks(context.Background(), tasks)
		if err != nil {
			return "", nil, err
		}
		return catalog.ExportStatusInProgress, nil, nil
	})
	return exportID, err
}

// ExportBranchDone ends the export branch process by changing the status
func ExportBranchDone(cataloger catalog.Cataloger, status catalog.CatalogBranchExportStatus, statusMsg *string, repo, branch, commitRef string) error {
	err := cataloger.ExportState(repo, branch, commitRef, func(oldRef string, state catalog.CatalogBranchExportStatus) (newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		return status, statusMsg, nil
	})
	return err
}
