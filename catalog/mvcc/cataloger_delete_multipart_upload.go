package mvcc

import (
	"context"
)

func (c *cataloger) DeleteMultipartUpload(ctx context.Context, _ string, uploadID string) error {
	return c.multiparts.Delete(ctx, uploadID)
}
