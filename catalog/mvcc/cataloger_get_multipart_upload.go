package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
)

func (c *cataloger) GetMultipartUpload(ctx context.Context, _ string, uploadID string) (*catalog.MultipartUpload, error) {
	return c.multiparts.Get(ctx, uploadID)
}
