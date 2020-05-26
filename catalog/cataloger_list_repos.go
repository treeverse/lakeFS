package catalog

import (
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListRepos(amount int, after int) ([]*Repo, bool, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var repos []*Repo
		var err error
		if amount < 0 {
			err = tx.Select(&repos, `SELECT * FROM repositories WHERE id > $1 ORDER BY id ASC`, after)
		} else {
			err = tx.Select(&repos, `SELECT * FROM repositories WHERE id > $1 ORDER BY id ASC LIMIT $2`, after, amount+1)
		}
		return repos, err
	}, append(c.transactOpts(), db.ReadOnly())...)
	if err != nil {
		return nil, false, err
	}
	repos := res.([]*Repo)
	hasMore := false
	if amount >= 0 && len(repos) > amount {
		repos = repos[0:amount]
		hasMore = true
	}
	return repos, hasMore, err
}
