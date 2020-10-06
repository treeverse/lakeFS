package s3

type MetadataProvider struct{}

func NewMetadataProvider() *MetadataProvider {
	return &MetadataProvider{}
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	panic("implement me")
}
