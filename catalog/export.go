package catalog

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

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
type ExportState struct {
	CurrentRef   string                    `db:"current_ref"`
	State        CatalogBranchExportStatus `db:"state"`
	ErrorMessage *string                   `db:"error_message"`
}

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
