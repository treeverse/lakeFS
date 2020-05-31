package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) DeleteRepo(ctx context.Context, repo string) error {
	if err := Validate(ValidateFields{
		"repo": ValidateRepoName(repo),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		b := db.Builder.NewUpdateBuilder()
		sqlRepos, argsRepos := b.Update("repositories").
			Set(b.Assign("deleted", true)).
			Where(b.Equal("name", repo)).
			Build()
		res, err := tx.Exec(sqlRepos, argsRepos...)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, ErrRepoNotFound
		}
		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"affected": affected,
				"repo":     repo,
			}).Debug("Repository deleted")
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
