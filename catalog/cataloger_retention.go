package catalog

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

// Avoid rounding by keeping whole hours (not Durations)
type TimePeriodHours int

type Expiration struct {
	All         *TimePeriodHours `json:",omitempty"`
	Uncommitted *TimePeriodHours `json:",omitempty"`
	Noncurrent  *TimePeriodHours `json:",omitempty"`
}

type Rule struct {
	Enabled      bool
	FilterPrefix string `json:",omitempty"`
	Expiration   Expiration
}

type Rules []Rule

type Policy struct {
	Rules       Rules
	Description string
}

type PolicyWithCreationTime struct {
	Policy
	CreatedAt time.Time `db:"created_at"`
}

// RulesHolder is a dummy struct for helping pg serialization: it has
// poor support for passing an array-valued parameter.
type RulesHolder struct {
	Rules Rules
}

func (a *RulesHolder) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *Rules) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return ErrByteSliceTypeAssertion
	}
	return json.Unmarshal(b, a)
}

// TODO(ariels) Move retention policy CRUD from retention service to
// here.

const entriesTable = "catalog_entries"

func byRepository(repository string) sq.Sqlizer {
	return sq.Expr("catalog_repositories.name = ?", repository)
}

func byPathPrefix(pathPrefix string) sq.Sqlizer {
	if len(pathPrefix) == 0 {
		return sq.Eq{}
	}
	parts := strings.SplitN(pathPrefix, "/", 2)
	branchExpr := sq.Eq{"catalog_branches.name": parts[0]}
	if len(parts) == 1 || parts[1] == "" {
		return branchExpr
	}
	pathPrefixExpr := sq.Like{"path": db.Prefix(parts[1])}
	return sq.And{branchExpr, pathPrefixExpr}
}

func byExpiration(hours TimePeriodHours) sq.Sqlizer {
	return sq.Expr("NOW() - catalog_entries.creation_date > make_interval(hours => ?)", int(hours))
}

type retentionQueryRecord struct {
	PhysicalAddress string   `db:"physical_address"`
	Branch          string   `db:"branch"`
	BranchID        int64    `db:"branch_id"`
	MinCommit       CommitID `db:"min_commit"`
	Path            string   `db:"path"`
}

func buildRetentionQuery(repositoryName string, policy *Policy) sq.SelectBuilder {
	var (
		byNonCurrent  = sq.Expr("min_commit != 0 AND max_commit < catalog_max_commit_id()")
		byUncommitted = sq.Expr("min_commit = 0")
	)

	repositorySelector := byRepository(repositoryName)

	// An expression to select for each rule.  Select by ORing all these.
	ruleSelectors := make([]sq.Sqlizer, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		if !rule.Enabled {
			continue
		}
		pathExpr := byPathPrefix(rule.FilterPrefix)
		expirationExprs := make([]sq.Sqlizer, 0, 3)
		if rule.Expiration.All != nil {
			expirationExprs = append(expirationExprs, byExpiration(*rule.Expiration.All))
		}
		if rule.Expiration.Noncurrent != nil {
			expirationExprs = append(expirationExprs,
				sq.And{
					byExpiration(*rule.Expiration.Noncurrent),
					byNonCurrent,
				})
		}
		if rule.Expiration.Uncommitted != nil {
			expirationExprs = append(expirationExprs,
				sq.And{
					byExpiration(*rule.Expiration.Uncommitted),
					byUncommitted,
				})
		}
		selector := sq.And{
			pathExpr,
			sq.Or(expirationExprs),
		}
		ruleSelectors = append(ruleSelectors, selector)
	}

	query := psql.Select("physical_address", "catalog_branches.name AS branch", "branch_id", "path", "min_commit").
		From(entriesTable).
		Where(sq.And{repositorySelector, sq.Or(ruleSelectors)})
	query = query.Join("catalog_branches ON catalog_entries.branch_id = catalog_branches.id").
		Join("catalog_repositories on catalog_branches.repository_id = catalog_repositories.id")
	return query
}

// expiryRows implements ExpiryRows.
type expiryRows struct {
	rows           *sqlx.Rows
	RepositoryName string
}

func (e *expiryRows) Next() bool {
	return e.rows.Next()
}

func (e *expiryRows) Err() error {
	return e.rows.Err()
}

func (e *expiryRows) Close() error {
	return e.rows.Close()
}

func (e *expiryRows) Read() (*ExpireResult, error) {
	var record retentionQueryRecord
	err := e.rows.StructScan(&record)
	if err != nil {
		return nil, err
	}
	return &ExpireResult{
		Repository:      e.RepositoryName,
		Branch:          record.Branch,
		PhysicalAddress: record.PhysicalAddress,
		InternalReference: (&InternalObjectRef{
			BranchID:  record.BranchID,
			MinCommit: record.MinCommit,
			Path:      record.Path,
		}).String(),
	}, nil
}

func (c *cataloger) QueryExpired(ctx context.Context, repositoryName string, policy *Policy) (ExpiryRows, error) {
	logger := logging.FromContext(ctx).WithField("policy", *policy)

	// TODO(ariels): Get lowest possible isolation level here.
	expiryByEntriesQuery := buildRetentionQuery(repositoryName, policy)
	expiryByEntriesQueryString, args, err := expiryByEntriesQuery.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	// Hold retention query results as a CTE in a WITH prefix.  Everything must live in a
	// single SQL statement because multiple statements would occur on separate connections,
	// and using a transaction would close the returned rows iterator on exit... preventing
	// the caller from reading the returned rows.

	dedupedQuery := fmt.Sprintf(`
                    WITH to_expire AS (%s) SELECT * FROM to_expire
                    WHERE physical_address IN (
                        SELECT a.physical_address FROM
                            ((SELECT physical_address, COUNT(*) c FROM to_expire GROUP BY physical_address) AS a JOIN
                             (SELECT physical_address, COUNT(*) c FROM catalog_entries GROUP BY physical_address) AS b
                             ON a.physical_address = b.physical_address)
                            WHERE a.c = b.c)
                    `,
		expiryByEntriesQueryString,
	)

	logger.WithFields(logging.Fields{
		"dedupe_query": dedupedQuery,
		"args":         args,
	}).Info("retention dedupe")
	// Return only those entries to expire for which *all* entry references are due:
	// An object may have been deduped onto several branches with different names
	// and will have multiple entries; it can only be remove once it expires from
	// all of those.
	rows, err := c.db.Queryx(dedupedQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}
	var ret ExpiryRows = &expiryRows{rows: rows, RepositoryName: repositoryName}
	return ret, nil
}

func (c *cataloger) MarkExpired(ctx context.Context, repositoryName string, expireResults []*ExpireResult) error {
	logger := logging.FromContext(ctx).WithFields(logging.Fields{"repository_name": repositoryName, "num_records": len(expireResults)})

	result, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`CREATE TEMPORARY TABLE temp_expiry (
				path text NOT NULL, branch_id bigint NOT NULL, min_commit bigint NOT NULL)
			ON COMMIT DROP`)
		if err != nil {
			return nil, fmt.Errorf("creating temporary expiry table: %w", err)
		}

		// TODO(ariels): Use COPY.  Hard because requires bailing out of sql(x) for the
		//     entire transaction.
		insert := psql.Insert("temp_expiry").Columns("branch_id", "min_commit", "path")
		for _, expire := range expireResults {
			ref, err := ParseInternalObjectRef(expire.InternalReference)
			if err != nil {
				logger.WithField("record", expire).WithError(err).Error("bad reference - skip")
				continue
			}
			insert = insert.Values(ref.BranchID, ref.MinCommit, ref.Path)
		}
		insertString, args, err := insert.ToSql()
		if err != nil {
			return nil, fmt.Errorf("building SQL: %w", err)
		}
		_, err = tx.Exec(insertString, args...)
		if err != nil {
			return nil, fmt.Errorf("executing: %w", err)
		}

		result, err := tx.Exec(`UPDATE catalog_entries SET is_expired = true
		    WHERE (path, branch_id, min_commit) IN (SELECT path, branch_id, min_commit FROM temp_expiry)`)
		if err != nil {
			return nil, fmt.Errorf("updating entries to expire: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("getting number of updated rows: %w", err)
		}
		return int(count), nil
	})
	if err != nil {
		return err
	}
	count := result.(int)
	if count != len(expireResults) {
		return fmt.Errorf("%w: tried to expire %d entries but expired only %d entries", ErrUnexpected, len(expireResults), count)
	}
	logger.WithField("count", count).Info("expired records")
	return nil
}
