package catalog

import (
	"context"
	"time"
)

type Multiparts interface {
	Create(ctx context.Context, uploadID, path, physicalAddress string, creationTime time.Time) error
	Get(ctx context.Context, uploadID string) (*MultipartUpload, error)
	Delete(ctx context.Context, uploadID string) error
}
