package cloud

const (
	IDTypeKey = "cloud_id_type"
	IDKey     = "cloud_id"
)

type MetadataProvider interface {
	GetMetadata() map[string]string
}
