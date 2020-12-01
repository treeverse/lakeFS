package mvcc

import (
	"context"
	"time"
)

func (c *cataloger) CreateMultipartUpload(ctx context.Context, _ string, uploadID, path, physicalAddress string, creationTime time.Time) error {
	return c.multiparts.Create(ctx, uploadID, path, physicalAddress, creationTime)
}
