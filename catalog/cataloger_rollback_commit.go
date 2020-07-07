package catalog

import (
	"context"

	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) RollbackCommit(ctx context.Context, repository, reference string) error {
	c.log.WithFields(logging.Fields{
		"ctx":        ctx,
		"repository": repository,
		"reference":  reference,
	}).Debug("Implement me")
	return nil
}
