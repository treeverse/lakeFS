package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) GetRepo(ctx context.Context, repo string) (*Repo, error) {
	if err := Validate(ValidateRepoName(repo)); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		sb := db.Builder.NewSelectBuilder()
		sql, args := sb.From("repositories r").
			Select("r.name", "r.storage_namespace", "b.name as default_branch", "r.creation_date").
			Where(sb.Equal("r.name", repo)).
			Join("branches b", "r.id = b.repository_id", "r.default_branch = b.id").
			Build()
		var repo Repo
		if err := tx.Get(&repo, sql, args...); err != nil {
			return nil, err
		}

		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"repo": repo,
			}).Debug("Repository get repo")
		return &repo, nil
	}, c.transactOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Repo), nil
}
