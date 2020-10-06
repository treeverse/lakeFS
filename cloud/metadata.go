package cloud

type MetadataProvider interface {
	GetMetadata() map[string]string
}
