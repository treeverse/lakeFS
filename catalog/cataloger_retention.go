package catalog

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

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

func (c *cataloger) buildRetentionQuery(repositoryName string, policy *retention.Policy) sq.SelectBuilder {
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

	query := psql.Select("physical_address", "branches.name AS branch", "path").
		From("entries").
		Where(sq.And{repositorySelector, sq.Or(ruleSelectors)})
	query = query.Join("branches ON entries.branch_id = branches.id").
		Join("repositories on branches.repository_id = repositories.id")
	return query
}

func (c *cataloger) ScanExpired(ctx context.Context, repositoryName string, policy *retention.Policy, out chan ExpireResult) error {
	defer func() {
		close(out)
	}()
	logger := logging.Default().WithContext(ctx).WithField("policy", *policy)

	query := c.buildRetentionQuery(repositoryName, policy)
	queryString, args, err := query.ToSql()
	if err != nil {
		logger.WithError(err).Error("building query")
		return fmt.Errorf("building query: %s", err)
	}

	logger = logger.WithFields(logging.Fields{"query": queryString, "args": args})
	logger.Info("retention query")

	// TODO(ariels): Get lowest possible isolation level here.
	_, err = c.db.Transact(func(tx db.Tx) (interface{}, error) {
		rows, err := tx.Query(queryString, args...)
		if err != nil {
			logger.WithError(err).Error("running query")
			return nil, fmt.Errorf("running query: %s", err)
		}
		for rows.Next() {
			err = rows.Err()
			if err != nil {
				logger.WithError(err).Error("scanning rows")
				return nil, fmt.Errorf("scanning rows: %s", err)
			}
			var res ExpireResult
			err = rows.StructScan(&res)
			if err != nil {
				logger.WithError(err).Error("bad row (keep going)")
			}
			res.Repository = repositoryName
			out <- res
		}
		return nil, nil
	})
	return err
}
