package catalog

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/lib/pq"
	"github.com/treeverse/lakefs/db"
)

// ExportConfiguration describes the export configuration of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportConfiguration struct {
	Path                   string         `db:"export_path" json:"export_path"`
	StatusPath             string         `db:"export_status_path" json:"export_status_path"`
	LastKeysInPrefixRegexp pq.StringArray `db:"last_keys_in_prefix_regexp" json:"last_keys_in_prefix_regexp"`
	IsContinuous           bool           `db:"continuous" json:"is_continuous"`
}

// ExportConfigurationForBranch describes how to export BranchID.  It is stored in the database.
// Unfortunately golang sql doesn't know about embedded structs, so you get a useless copy of
// ExportConfiguration embedded here.
type ExportConfigurationForBranch struct {
	Repository string `db:"repository"`
	Branch     string `db:"branch"`

	Path                   string         `db:"export_path"`
	StatusPath             string         `db:"export_status_path"`
	LastKeysInPrefixRegexp pq.StringArray `db:"last_keys_in_prefix_regexp"`
	IsContinuous           bool           `db:"continuous"`
}

type CatalogBranchExportStatus string

const (
	ExportStatusInProgress = CatalogBranchExportStatus("in-progress")
	ExportStatusSuccess    = CatalogBranchExportStatus("exported-successfully")
	ExportStatusFailed     = CatalogBranchExportStatus("export-failed")
	ExportStatusRepaired   = CatalogBranchExportStatus("export-repaired")
	ExportStatusUnknown    = CatalogBranchExportStatus("[unknown]")
)

// ExportStatus describes the current export status of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportStatus struct {
	CurrentRef string `db:"current_ref"`
	State      CatalogBranchExportStatus
}

var ErrBadTypeConversion = errors.New("bad type")

// nolint: stylecheck
func (dst *CatalogBranchExportStatus) Scan(src interface{}) error {
	var sc CatalogBranchExportStatus
	switch s := src.(type) {
	case string:
		sc = CatalogBranchExportStatus(strings.ToLower(s))
	case []byte:
		sc = CatalogBranchExportStatus(strings.ToLower(string(s)))
	default:
		return fmt.Errorf("cannot convert %T to CatalogBranchExportStatus: %w", src, ErrBadTypeConversion)
	}

	if !(sc == ExportStatusInProgress || sc == ExportStatusSuccess || sc == ExportStatusFailed) {
		// not a failure, "just" be a newer enum value than known
		*dst = ExportStatusUnknown
		return nil
	}
	*dst = sc
	return nil
}

func (src CatalogBranchExportStatus) Value() (driver.Value, error) {
	return string(src), nil
}

func (c *cataloger) GetExportConfigurationForBranch(repository string, branch string) (ExportConfiguration, error) {
	ret, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		var ret ExportConfiguration
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
		return ExportConfiguration{}, err
	}
	return *ret.(*ExportConfiguration), err
}

func (c *cataloger) GetExportConfigurations() ([]ExportConfigurationForBranch, error) {
	ret := make([]ExportConfigurationForBranch, 0)
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

func (c *cataloger) PutExportConfiguration(repository string, branch string, conf *ExportConfiguration) error {
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

var ErrExportFailed = errors.New("export failed")

type ExportState struct {
	CurrentRef   string
	State        CatalogBranchExportStatus
	ErrorMessage *string
}

func (c *cataloger) GetExportState(repo string, branch string) (ExportState, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res ExportState

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
	return res.(ExportState), err
}

func (c *cataloger) ExportState(repo string, branch string, newRef string, cb stateCB) error {
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res struct {
			CurrentRef   string
			State        CatalogBranchExportStatus
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
		newState, newMsg, err := cb(oldRef, state)
		if err != nil {
			return nil, err
		}
		// in case newRef was not set leave oldRef
		if newRef == "" {
			newRef = oldRef
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

		tag, err := tx.Exec(query,
			branchID, newRef, newState, newMsg)
		if err != nil {
			return nil, fmt.Errorf("ExportState: update state: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("ExportState: could not update single row %s: %w", tag, ErrExportFailed)
		}
		return nil, err
	})
	return err
}
