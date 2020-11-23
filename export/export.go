package export

import (
	"context"
	"errors"
	"fmt"

	nanoid "github.com/matoous/go-nanoid"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
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

var ErrExportInProgress = errors.New("export currently in progress")

// ExportBranchStart inserts a start task on branch, sets branch export state to pending.
// It returns ErrExportInProgress if an export is already in progress.
func ExportBranchStart(parade parade.Parade, cataloger catalog.Cataloger, repo, branch string) (string, error) {
	commit, err := cataloger.GetCommit(context.Background(), repo, branch)
	if err != nil {
		return "", err
	}
	commitRef := commit.Reference
	exportID, err := getExportID(repo, branch, commitRef)
	if err != nil {
		return "", err
	}
	l := logging.Default().WithFields(logging.Fields{
		"repo":       repo,
		"branch":     branch,
		"commit_ref": commit,
		"export_id":  exportID,
	})

	err = cataloger.ExportStateSet(repo, branch, func(oldRef string, state catalog.CatalogBranchExportStatus) (newRef string, newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		l.WithField("initial_state", state).Info("export update state")
		if state == catalog.ExportStatusInProgress {
			return oldRef, state, nil, ErrExportInProgress
		}
		if state == catalog.ExportStatusFailed {
			return oldRef, state, nil, catalog.ErrExportFailed
		}
		config, err := cataloger.GetExportConfigurationForBranch(repo, branch)
		if err != nil {
			return oldRef, "", nil, err
		}
		tasks, err := GetStartTasks(repo, branch, oldRef, commitRef, exportID, config)
		if err != nil {
			return oldRef, "", nil, err
		}

		l.WithFields(logging.Fields{"num_tasks": len(tasks), "target_path": config.Path}).
			Info("insert export tasks")

		err = parade.InsertTasks(context.Background(), tasks)
		if err != nil {
			return "", "", nil, err
		}
		return commitRef, catalog.ExportStatusInProgress, nil, nil
	})
	return exportID, err
}

var (
	ErrConflictingRefs = errors.New("conflicting references")
	ErrWrongStatus     = errors.New("incorrect status")
)

// ExportBranchDone ends the export branch process by changing the status
func ExportBranchDone(parade parade.Parade, cataloger catalog.Cataloger, status catalog.CatalogBranchExportStatus, statusMsg *string, repo, branch, commitRef string) error {
	l := logging.Default().WithFields(logging.Fields{"repo": repo, "branch": branch, "commit_ref": commitRef, "status": status, "status_message": statusMsg})
	if status == catalog.ExportStatusSuccess {
		// Start the next export if continuous.
		isContinuous, err := hasContinuousExport(cataloger, repo, branch)
		if err != nil {
			// Consider branch export failed: it was supposed to be continuous but
			// might have stopped.  So set an error for the admin to fix before
			// re-enabling continuous export.
			return err
		}
		if isContinuous {
			l.Info("start new continuous export")
			_, err := ExportBranchStart(parade, cataloger, repo, branch)
			if errors.Is(err, ErrExportInProgress) {
				l.Info("export already in progress when restarting continuous export (unlikely)")
				err = nil
			}
			if err != nil {
				return fmt.Errorf("restart continuous export repo %s branch %s: %w", repo, branch, err)
			}
			return nil
		}
		return err
	}

	err := cataloger.ExportStateSet(repo, branch, func(oldRef string, oldStatus catalog.CatalogBranchExportStatus) (newRef string, newStatus catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		if commitRef != oldRef {
			return "", "", nil, fmt.Errorf("ExportBranchDone: currentRef: %s, newRef: %s: %w", oldRef, commitRef, ErrConflictingRefs)
		}
		if oldStatus != catalog.ExportStatusInProgress {
			l.WithField("old_state", oldStatus).Error("expected old_state to be in-progress")
			return "", "", nil, fmt.Errorf("expected old status to be in-progress not %s: %w", oldStatus, ErrWrongStatus)
		}
		l.WithField("old_state", oldStatus).Info("updating branch export")
		return oldRef, status, statusMsg, nil
	})
	return err
}

// ExportBranchRepair changes state from Failed To Repair and starts a new export.
// It fails if the current state is not ExportStatusFailed.
func ExportBranchRepair(cataloger catalog.Cataloger, repo, branch string) error {
	return cataloger.ExportStateSet(repo, branch, func(oldRef string, state catalog.CatalogBranchExportStatus) (newRef string, newState catalog.CatalogBranchExportStatus, newMessage *string, err error) {
		if state != catalog.ExportStatusFailed {
			return oldRef, "", nil, ErrWrongStatus
		}
		return oldRef, catalog.ExportStatusRepaired, nil, nil
	})
}

func hasContinuousExport(c catalog.Cataloger, repo, branch string) (bool, error) {
	exportConfiguration, err := c.GetExportConfigurationForBranch(repo, branch)
	if errors.Is(err, db.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check if export configuration is continuous for repo %s branch %s %w", repo, branch, err)
	}
	return exportConfiguration.IsContinuous, nil
}
