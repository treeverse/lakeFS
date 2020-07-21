package catalog

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
)

// TODO(ariels) Move retention policy CRUD from retention service to
// here.

const entriesTable = "entries"

func byRepository(repository string) sq.Sqlizer {
	return sq.Expr("repositories.name = ?", repository)
}

func byPathPrefix(pathPrefix string) sq.Sqlizer {
	if len(pathPrefix) == 0 {
		return sq.Eq{}
	}
	parts := strings.SplitN(pathPrefix, "/", 2)
	branchExpr := sq.Eq{"branches.name": parts[0]}
	if len(parts) == 1 || parts[1] == "" {
		return branchExpr
	}
	pathPrefixExpr := sq.Like{"path": db.Prefix(parts[1])}
	return sq.And{branchExpr, pathPrefixExpr}
}

func byExpiration(hours retention.TimePeriodHours) sq.Sqlizer {
	return sq.Expr("NOW() - entries.creation_date > make_interval(hours => ?)", int(hours))
}

type retentionQueryRecord struct {
	PhysicalAddress string   `db:"physical_address"`
	Branch          string   `db:"branch"`
	BranchID        int64    `db:"branch_id"`
	MinCommit       CommitID `db:"min_commit"`
	Path            string   `db:"path"`
}

func buildRetentionQuery(repositoryName string, policy *retention.Policy) sq.SelectBuilder {
	var (
		byNonCurrent  = sq.Expr("min_commit != 0 AND max_commit < max_commit_id()")
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

	query := psql.Select("physical_address", "branches.name AS branch", "branch_id", "path", "min_commit").
		From(entriesTable).
		Where(sq.And{repositorySelector, sq.Or(ruleSelectors)})
	query = query.Join("branches ON entries.branch_id = branches.id").
		Join("repositories on branches.repository_id = repositories.id")
	return query
}

// expiryRows implements ExpiryRows.
type expiryRows struct {
	rows           *sqlx.Rows
	repositoryName string
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
		Repository:      e.repositoryName,
		Branch:          record.Branch,
		PhysicalAddress: record.PhysicalAddress,
		InternalReference: (&InternalObjectRef{
			BranchID:  record.BranchID,
			MinCommit: record.MinCommit,
			Path:      record.Path,
		}).String(),
	}, nil
}

// QueryExpired returns ExpiryRows iterating over all objects to expire on repositoryName
// according to policy to channel out.
func (c *cataloger) QueryExpired(ctx context.Context, repositoryName string, policy *retention.Policy) (ExpiryRows, error) {
	logger := logging.Default().WithContext(ctx).WithField("policy", *policy)

	query := buildRetentionQuery(repositoryName, policy)
	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	logger = logger.WithFields(logging.Fields{"query": queryString, "args": args})
	logger.Info("retention query")

	// TODO(ariels): Get lowest possible isolation level here.
	rows, err := c.db.Queryx(queryString, args...)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}
	return &expiryRows{rows, repositoryName}, nil
}

func (c *cataloger) MarkExpired(ctx context.Context, repositoryName string, expireResults []*ExpireResult) error {
	logger := logging.Default().WithContext(ctx).WithFields(logging.Fields{"repository_name": repositoryName, "num_records": len(expireResults)})

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

		result, err := tx.Exec(`UPDATE entries SET is_expired = true
		    WHERE (path, branch_id, min_commit) IN (SELECT path, branch_id, min_commit FROM temp_expiry)`)
		if err != nil {
			return nil, fmt.Errorf("updating entries to expire: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("getting number of updated rows: %s", err)
		}
		return int(count), nil
	})
	if err != nil {
		return err
	}
	count := result.(int)
	if count != len(expireResults) {
		return fmt.Errorf("tried to expire %d entries but expired only %d entries", len(expireResults), count)
	}
	logger.WithField("count", count).Info("expired records")
	return nil
}
