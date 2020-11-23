package mvcc

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

// TODO(ariels) Move retention policy CRUD from retention service to
// here.

const entriesTable = "catalog_entries"

type StringRows struct {
	rows pgx.Rows
}

func (s *StringRows) Next() bool {
	return s.rows.Next()
}

func (s *StringRows) Err() error {
	return s.rows.Err()
}

func (s *StringRows) Close() {
	s.rows.Close()
}

func (s *StringRows) Read() (string, error) {
	var ret string
	err := s.rows.Scan(&ret)
	return ret, err
}

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

func byExpiration(hours catalog.TimePeriodHours) sq.Sqlizer {
	return sq.Expr("NOW() - catalog_entries.creation_date > make_interval(hours => ?)", int(hours))
}

type retentionQueryRecord struct {
	PhysicalAddress string   `db:"physical_address"`
	Branch          string   `db:"branch"`
	BranchID        int64    `db:"branch_id"`
	MinCommit       CommitID `db:"min_commit"`
	Path            string   `db:"path"`
}

func buildRetentionQuery(repositoryName string, policy *catalog.Policy, afterRow sq.RowScanner, limit *uint64) (sq.SelectBuilder, error) {
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
			byNonCurrent := sq.And{
				sq.NotEq{"min_commit": MinCommitUncommittedIndicator},
				sq.Lt{"max_commit": MaxCommitID},
			}
			expirationExprs = append(expirationExprs,
				sq.And{
					byExpiration(*rule.Expiration.Noncurrent),
					byNonCurrent,
				})
		}
		if rule.Expiration.Uncommitted != nil {
			byUncommitted := sq.Eq{"min_commit": MinCommitUncommittedIndicator}
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

	repositorySelector := byRepository(repositoryName)
	filter := sq.And{repositorySelector, sq.Or(ruleSelectors)}
	if afterRow != nil {
		var r retentionQueryRecord
		if err := afterRow.Scan(&r); err != nil {
			return sq.SelectBuilder{}, fmt.Errorf("failed to unwrap last row %v: %w", afterRow, err)
		}
		filter = sq.And{
			filter,
			sq.Expr("(physical_address, branch_id, min_commit) > (?, ?, ?)", r.PhysicalAddress, r.BranchID, r.MinCommit),
		}
	}

	query := psql.Select("physical_address", "catalog_branches.name AS branch", "branch_id", "path", "min_commit").
		From(entriesTable).
		Where(filter)
	query = query.Join("catalog_branches ON catalog_entries.branch_id = catalog_branches.id").
		Join("catalog_repositories on catalog_branches.repository_id = catalog_repositories.id")
	// todo: Ariel - problematic sort
	if limit != nil {
		query = query.OrderBy("physical_address", "branch_id", "min_commit").
			Limit(*limit)
	}
	return query, nil
}

// expiryRows implements ExpiryRows.
type expiryRows struct {
	rows           pgx.Rows
	RepositoryName string
}

func (e *expiryRows) Next() bool {
	return e.rows.Next()
}

func (e *expiryRows) Err() error {
	return e.rows.Err()
}

func (e *expiryRows) Close() {
	e.rows.Close()
}

func (e *expiryRows) Read() (*catalog.ExpireResult, error) {
	var record retentionQueryRecord
	err := pgxscan.ScanRow(&record, e.rows)
	if err != nil {
		return nil, err
	}
	return &catalog.ExpireResult{
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

func (c *cataloger) QueryEntriesToExpire(ctx context.Context, repositoryName string, policy *catalog.Policy) (catalog.ExpiryRows, error) {
	logger := logging.FromContext(ctx).WithField("policy", *policy)

	// TODO(ariels): page!
	expiryByEntriesQuery, err := buildRetentionQuery(repositoryName, policy, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}
	// TODO(ariels): Get lowest possible isolation level here.
	expiryByEntriesQueryString, args, err := expiryByEntriesQuery.ToSql()
	if err != nil {
		return nil, fmt.Errorf("converting query to SQL: %w", err)
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
	rows, err := c.db.WithContext(ctx).Query(dedupedQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}
	var ret catalog.ExpiryRows = &expiryRows{rows: rows, RepositoryName: repositoryName}
	return ret, nil
}

func (c *cataloger) MarkEntriesExpired(ctx context.Context, repositoryName string, expireResults []*catalog.ExpireResult) error {
	logger := logging.FromContext(ctx).WithFields(logging.Fields{"repository_name": repositoryName, "num_records": len(expireResults)})

	if len(expireResults) == 0 {
		logger.Info("nothing to expire")
		return nil
	}

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
		count := result.RowsAffected()
		return int(count), nil
	})
	if err != nil {
		return err
	}
	count := result.(int)
	if count != len(expireResults) {
		return fmt.Errorf("%w: tried to expire %d entries but expired only %d entries", catalog.ErrUnexpected, len(expireResults), count)
	}
	logger.WithField("count", count).Info("expired records")
	return nil
}

// TODO(ariels): chunk.
func (c *cataloger) MarkObjectsForDeletion(ctx context.Context, repositoryName string) (int64, error) {
	// TODO(ariels): This query is difficult to chunk.  One way: Perform the inner SELECT
	// once into a temporary table, then in a separate transaction chunk the UPDATE by
	// dedup_id (this is not yet the real deletion).
	result, err := c.db.WithContext(ctx).Exec(`
                    UPDATE catalog_object_dedup SET deleting=true
                    WHERE repository_id IN (SELECT id FROM catalog_repositories WHERE name = $1) AND
                          physical_address IN (
                              SELECT physical_address FROM (
                                SELECT physical_address, bool_and(is_expired) all_expired
                                FROM catalog_entries
                                GROUP BY physical_address
                              ) physical_addresses_with_expiry
                              WHERE all_expired)`, repositoryName)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// TODO(ariels): Process in chunks.  Can store the inner physical_address query in a table for
//     the duration.
func (c *cataloger) DeleteOrUnmarkObjectsForDeletion(ctx context.Context, repositoryName string) (catalog.StringIterator, error) {
	rows, err := c.db.WithContext(ctx).Query(`
		WITH ids AS (SELECT id repository_id FROM catalog_repositories WHERE name = $1),
		    update_result AS (
			UPDATE catalog_object_dedup SET deleting=all_expired
			 FROM (
			     SELECT physical_address, bool_and(is_expired) all_expired
			     FROM catalog_entries
			     GROUP BY physical_address
			 ) AS by_entries
			 WHERE repository_id IN (SELECT repository_id FROM ids) AND
			       catalog_object_dedup.physical_address = by_entries.physical_address
			 RETURNING by_entries.physical_address, all_expired
		    )
		SELECT physical_address FROM update_result WHERE all_expired`,
		repositoryName,
	)
	return &StringRows{rows: rows}, err
}
