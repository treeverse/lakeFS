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

// ExportBranchStart inserts a start task on branch and changes branch export state to pending
// if export on  already in progress will return error
func ExportBranchStart(paradeDB parade.Parade, cataloger catalog.Cataloger, repo, branch string) error {
	commit, err := cataloger.GetCommit(context.Background(), repo, branch)
	if err != nil {
		return err
	}
	commitRef := commit.Reference

	err = cataloger.ExportState(repo, branch, commitRef, func(oldRef string, state catalog.CatalogBranchExportStatus) (newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		// Todo(guys) consider checking commitRef bigger than or equal oldRef
		// Todo(guys) consider checking if branch configured and fail here
		exportID, err := getExportID(repo, branch, commitRef)
		if err != nil {
			msg := "failed generating ID"
			return catalog.ExportStatusFailed, &msg, err
		}
		tasks, err := GetStartTasks(repo, branch, oldRef, commitRef, exportID)
		if err != nil {
			msg := "failed creating start task"
			return catalog.ExportStatusFailed, &msg, err
		}

		err = paradeDB.InsertTasks(context.Background(), tasks)
		if err != nil {
			msg := "failed inserting start task"
			return catalog.ExportStatusFailed, &msg, err
		}
		return catalog.ExportStatusInProgress, nil, nil
	})
	return err
}

// ExportBranchDone ends the export branch process by changing the status
func ExportBranchDone(cataloger catalog.Cataloger, status catalog.CatalogBranchExportStatus, repo, branch, commitRef string) error {
	err := cataloger.ExportState(repo, branch, commitRef, func(oldRef string, state catalog.CatalogBranchExportStatus) (newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		return status, nil, nil
	})
	return err
}
