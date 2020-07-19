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

const entriesTable = "lakefs_catalog.entries"

// escapeSqlPattern returns pattern with any special pattern
// characters prefixed by "\".  It assumes Postgres option
// standard_conforming_strings is ON (the default) without checking.
func escapeSqlPattern(pattern string) string {
	var ret strings.Builder
	for _, c := range pattern {
		if c == '_' || c == '%' {
			ret.WriteRune('\\')
		}
		ret.WriteRune(c)
	}
	return ret.String()
}

func byRepository(repository string) sq.Sqlizer {
	return sq.Expr("repositories.name = ?", repository)
}

func byPathPrefix(pathPrefix string) sq.Sqlizer {
	if len(pathPrefix) == 0 {
		return sq.Expr("1=1")
	}
	parts := strings.SplitN(pathPrefix, "/", 2)
	clauses := make([]sq.Sqlizer, 0, 2)

	clauses = append(clauses, sq.Expr("branches.name = ?", parts[0]))
	if parts[1] != "" {
		clauses = append(clauses, sq.Like{"path": escapeSqlPattern(parts[1]) + "%"})
	}

	return sq.And(clauses)
}

func byExpiration(hours retention.TimePeriodHours) sq.Sqlizer {
	return sq.Expr("NOW() - entries.creation_date > make_interval(hours => ?)", int(hours))
}

var (
	byNoncurrent  = sq.Expr("min_commit != 0 AND max_commit < max_commit_id()")
	byUncommitted = sq.Expr("min_commit = 0")
)

func buildRetentionQuery(repositoryName string, policy *retention.Policy) sq.SelectBuilder {
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
					byNoncurrent,
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
		From("entries").
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

func (e *expiryRows) Read() (*ExpireResult, error) {
	var res ExpireResult
	err := e.rows.StructScan(&res)
	if err != nil {
		return nil, err
	}
	res.Repository = e.repositoryName
	return &res, nil
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
	logger := logging.Default().WithContext(ctx).WithFields(logging.Fields{"repositoryName": repositoryName, "numRecords": len(expireResults)})

	result, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`CREATE TEMPORARY TABLE temp_expiry (
				path text NOT NULL, branch_id bigint NOT NULL, min_commit bigint NOT NULL)
			ON COMMIT DROP`)
		if err != nil {
			return nil, fmt.Errorf("CREATE TEMPORARY TABLE: %s", err)
		}

		// TODO(ariels): Use COPY.  Hard because requires bailing out of sql(x) for the
		//     entire transaction.
		insert := psql.Insert("temp_expiry").Columns("path", "branch_id", "min_commit")
		for _, expire := range expireResults {
			insert = insert.Values(expire.Path, expire.BranchId, expire.MinCommit)
		}
		insertString, args, err := insert.ToSql()
		if err != nil {
			return nil, fmt.Errorf("generating INSERT SQL: %s", err)
		}
		result, err := tx.Exec(insertString, args...)
		if err != nil {
			return nil, fmt.Errorf("executing INSERT: %s", err)
		}

		result, err = tx.Exec(`UPDATE entries SET is_expired = true
		    WHERE (path, branch_id, min_commit) IN (SELECT path, branch_id, min_commit FROM temp_expiry)
		    `)
		if err != nil {
			return nil, fmt.Errorf("UPDATE entries to expire: %s", err)
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
	count, ok := result.(int)
	if !ok {
		return fmt.Errorf("bad result %#v from UPDATE count", result)
	}
	if count != len(expireResults) {
		return fmt.Errorf("tried to expire %d entries but expired only %d entries", len(expireResults), count)
	}
	logger.WithField("count", count).Info("expired records")
	return nil
}
