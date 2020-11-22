package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) RollbackCommit(ctx context.Context, repository, reference string) error {
	c.log.WithContext(ctx).WithFields(logging.Fields{
		"repository": repository,
		"reference":  reference,
	}).Debug("Rollback commit - feature not supported")
	return catalog.ErrFeatureNotSupported
}
