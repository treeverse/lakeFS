package retention

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type Manager struct {
	graveler graveler.Graveler
	adapter  block.Adapter
}

func (m *Manager) Prepare(ctx context.Context, repositoryID graveler.RepositoryID) {
	expiredCommitFinder := NewExpiredCommitFinder(m.graveler.RefManager)
	commits, err := expiredCommitFinder.Find(ctx, repositoryID, nil)

}
