package catalog

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"github.com/treeverse/lakefs/db"
)

// ExportConfiguration describes the export configuration of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportConfiguration struct {
	Path                   string         `db:"export_path"`
	StatusPath             string         `db:"export_status_path"`
	LastKeysInPrefixRegexp pq.StringArray `db:"last_keys_in_prefix_regexp"`
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
}

type CatalogBranchExportStatus string

const (
	ExportStatusInProgress = CatalogBranchExportStatus("in-progress")
	ExportStatusSuccess    = CatalogBranchExportStatus("exported-successfully")
	ExportStatusFailed     = CatalogBranchExportStatus("export-failed")
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
			`SELECT export_path, export_status_path, last_keys_in_prefix_regexp
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
                     e.last_keys_in_prefix_regexp last_keys_in_prefix_regexp
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
                             branch_id, export_path, export_status_path, last_keys_in_prefix_regexp)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (branch_id)
                         DO UPDATE SET (branch_id, export_path, export_status_path, last_keys_in_prefix_regexp) =
                             (EXCLUDED.branch_id, EXCLUDED.export_path, EXCLUDED.export_status_path, EXCLUDED.last_keys_in_prefix_regexp)`,
			branchID, conf.Path, conf.StatusPath, conf.LastKeysInPrefixRegexp)
		return nil, err
	})
	return err
}

type errExportFailed struct {
	Message string // Error string reported in database
}

func (e errExportFailed) Error() string {
	return e.Message
}

var ErrExportFailed = errExportFailed{}

func (c *cataloger) ExportMarkStart(tx db.Tx, repo string, branch string, newRef string) (oldRef string, state CatalogBranchExportStatus, err error) {
	var res struct {
		CurrentRef   string
		State        CatalogBranchExportStatus
		ErrorMessage string
	}
	branchID, err := c.getBranchIDCache(tx, repo, branch)
	if err != nil {
		return
	}
	err = tx.Get(&res, `
		SELECT current_ref, state, error_message
		FROM catalog_branches_export_state
		WHERE branch_id=$1 FOR UPDATE`,
		branchID)
	if err != nil && !errors.Is(err, ErrEntryNotFound) {
		err = fmt.Errorf("ExportMarkStart: failed to get existing state: %w", err)
		return
	}
	oldRef = res.CurrentRef
	state = res.State

	tag, err := tx.Exec(`
		UPDATE catalog_branches
		SET current_ref=$2, state='in-progress', error_message=NULL
		WHERE branch_id=$1`,
		branchID, newRef)
	if err != nil {
		return
	}
	if tag.RowsAffected() != 1 {
		err = fmt.Errorf("[I] ExportMarkStart: Updated %d rows instead of just 1: %w", pgx.ErrNoRows, err)
		return
	}
	if state == ExportStatusFailed {
		err = errExportFailed{res.ErrorMessage}
	}
	return
}

func (c *cataloger) ExportStateDelete(tx db.Tx, repo string, branch string) error {
	branchID, err := c.getBranchIDCache(tx, repo, branch)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(`DELETE FROM catalog_branches_export_state WHERE branch_id=$1`, branchID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("[I] ExportStateDelete: deleted %d rows instead of just 1: %w", tag.RowsAffected(), pgx.ErrNoRows)
	}
	return nil
}
