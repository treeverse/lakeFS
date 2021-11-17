package cloud

import "context"

const (
	IDTypeKey = "cloud_id_type"
	IDKey     = "cloud_id"
)

type MetadataProvider interface {
	GetMetadata(ctx context.Context) map[string]string
}
