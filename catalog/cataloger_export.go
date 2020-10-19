package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"

	"github.com/treeverse/lakefs/db"
)

// ExportConfiguration describes the export configuration of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportConfiguration struct {
	Path                   string `db:"export_path" json:"exportPath"`
	StatusPath             string `db:"export_status_path" json:"exportStatusPath"`
	LastKeysInPrefixRegexp string `db:"last_keys_in_prefix_regexp" json:"lastKeysInPrefixRegexp"`
}

// ExportConfigurationForBranch describes how to export BranchID.  It is stored in the database.
type ExportConfigurationForBranch struct {
	ExportConfiguration
	Repository string `db:"repository"`
	Branch     string `db:"branch"`
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
                 FROM catalog_branches_export e JOIN catalog_branches b ON e.branch_id = b.branch_id
                      JOIN catalog_repositories r ON b.repository_id = r.id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var rec ExportConfigurationForBranch
		if err = rows.Scan(&rec); err != nil {
			return nil, fmt.Errorf("scan configuration %+v: %w", rows, err)
		}
		ret = append(ret, rec)
	}
	return ret, nil
}

func (c *cataloger) PutExportConfiguration(repository string, branch string, conf *ExportConfiguration) error {
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

const (
	groupStart = "(?:"
	groupEnd   = ")"
)

// DisjunctRegexps returns a single regexp holding the disjunction ("|") of regexps.
func DisjunctRegexps(regexps []string) (*regexp.Regexp, error) {
	parts := make([]string, len(regexps))
	for i, re := range regexps {
		_, err := syntax.Parse(re, syntax.Perl)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", re, err)
		}
		parts[i] = groupStart + re + groupEnd
	}
	d := strings.Join(parts, "|")
	return regexp.Compile(d)
}

var (
	ErrNotBracketed = errors.New("not a bracketed string")
	unbracketRegexp = regexp.MustCompile("^" + regexp.QuoteMeta(groupStart) + "(.*)" + regexp.QuoteMeta(groupEnd) + "$")
)

func unbracket(s string) (string, error) {
	sub := unbracketRegexp.FindStringSubmatch(s)
	if len(sub) == 0 {
		return "", fmt.Errorf("%s: %w", s, ErrNotBracketed)
	}
	return sub[1], nil
}

// DeconstructDisjunction returns the text forms of the regexps in the disjunction rex.  rex
// should be constructed (only) by DisjunctRegexps.
func DeconstructDisjunction(regexp *regexp.Regexp) ([]string, error) { // nolint:interfacer
	// Why nollnt above? I really do want this regexp handling function to take a regexp, not
	// expvar.Var hich is a silly alias to Stringer.

	s := regexp.String()
	if len(s) == 0 {
		return nil, nil
	}
	regexpParts := strings.Split(s, "|")
	ret := make([]string, len(regexpParts))
	for i, regexpPart := range regexpParts {
		part, err := unbracket(regexpPart)
		if err != nil {
			return nil, err
		}
		ret[i] = part
	}
	return ret, nil
}
