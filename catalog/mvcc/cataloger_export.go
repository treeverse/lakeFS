package mvcc

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetExportConfigurationForBranch(repository string, branch string) (catalog.ExportConfiguration, error) {
	ret, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		var ret catalog.ExportConfiguration
		if err != nil {
			return nil, err
		}
		err = c.db.Get(&ret,
			`SELECT export_path, export_status_path, last_keys_in_prefix_regexp, continuous
                         FROM catalog_branches_export
                         WHERE branch_id = $1`, branchID)
		return &ret, err
	})
	if ret == nil {
		return catalog.ExportConfiguration{}, err
	}
	return *ret.(*catalog.ExportConfiguration), err
}

func (c *cataloger) GetExportConfigurations() ([]catalog.ExportConfigurationForBranch, error) {
	ret := make([]catalog.ExportConfigurationForBranch, 0)
	rows, err := c.db.Query(
		`SELECT r.name repository, b.name branch,
                     e.export_path export_path, e.export_status_path export_status_path,
                     e.last_keys_in_prefix_regexp last_keys_in_prefix_regexp,
                     e.continuous continuous
                 FROM catalog_branches_export e JOIN catalog_branches b ON e.branch_id = b.id
                    JOIN catalog_repositories r ON b.repository_id = r.id`)
	if err != nil {
		return nil, err
	}
	err = pgxscan.ScanAll(&ret, rows)
	return ret, err
}

func (c *cataloger) PutExportConfiguration(repository string, branch string, conf *catalog.ExportConfiguration) error {
	// Validate all fields could be compiled as regexps.
	for i, r := range conf.LastKeysInPrefixRegexp {
		if _, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("invalid regexp /%s/ at position %d in LastKeysInPrefixRegexp: %w", r, i, err)
		}
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		_, err = c.db.Exec(
			`INSERT INTO catalog_branches_export (
                             branch_id, export_path, export_status_path, last_keys_in_prefix_regexp, continuous)
                         VALUES ($1, $2, $3, $4, $5)
                         ON CONFLICT (branch_id)
                         DO UPDATE SET (branch_id, export_path, export_status_path, last_keys_in_prefix_regexp, continuous) =
                             (EXCLUDED.branch_id, EXCLUDED.export_path, EXCLUDED.export_status_path, EXCLUDED.last_keys_in_prefix_regexp, EXCLUDED.continuous)`,
			branchID, conf.Path, conf.StatusPath, conf.LastKeysInPrefixRegexp, conf.IsContinuous)
		return nil, err
	})
	return err
}

func (c *cataloger) GetExportState(repo string, branch string) (catalog.ExportState, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res catalog.ExportState

		branchID, err := c.getBranchIDCache(tx, repo, branch)
		if err != nil {
			return nil, err
		}
		// get current state
		err = tx.Get(&res, `
		SELECT current_ref, state, error_message
		FROM catalog_branches_export_state
		WHERE branch_id=$1`,
			branchID)
		return res, err
	})
	return res.(catalog.ExportState), err
}

func (c *cataloger) ExportStateSet(repo, branch string, cb catalog.ExportStateCallback) error {
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res struct {
			CurrentRef   string
			State        catalog.CatalogBranchExportStatus
			ErrorMessage *string
		}

		branchID, err := c.getBranchIDCache(tx, repo, branch)
		if err != nil {
			return nil, err
		}
		// get current state
		err = tx.Get(&res, `
		SELECT current_ref, state, error_message
		FROM catalog_branches_export_state
		WHERE branch_id=$1 FOR UPDATE`,
			branchID)
		missing := errors.Is(err, db.ErrNotFound)
		if err != nil && !missing {
			err = fmt.Errorf("ExportStateSet: failed to get existing state: %w", err)
			return nil, err
		}
		oldRef := res.CurrentRef
		state := res.State

		// run callback
		newRef, newState, newMsg, err := cb(oldRef, state)
		if err != nil {
			return nil, err
		}
		// update new state
		var query string
		if missing {
			query = `
			INSERT INTO catalog_branches_export_state (branch_id, current_ref, state, error_message)
			VALUES ($1, $2, $3, $4)`
		} else {
			query = `
			UPDATE catalog_branches_export_state
			SET current_ref=$2, state=$3, error_message=$4
			WHERE branch_id=$1`
		}

		tag, err := tx.Exec(query, branchID, newRef, newState, newMsg)
		if err != nil {
			return nil, fmt.Errorf("ExportStateSet: update state: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("ExportStateSet: could not update single row %s: %w", tag, catalog.ErrExportFailed)
		}
		return nil, err
	})
	return err
}
