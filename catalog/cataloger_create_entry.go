package catalog

import (
	"context"
)

func (c *cataloger) CreateEntry(ctx context.Context, repository, branch string, entry Entry) error {
	return c.CreateEntryDedup(ctx, repository, branch, entry, DedupParams{})
}
