package stats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type mockMetadataProvider struct {
	metadata map[string]string
	err      error
}

func (m *mockMetadataProvider) GetMetadata(ctx context.Context) (map[string]string, error) {
	return m.metadata, m.err
}

const (
	installationIDKey   = "installation_id"
	installationIDValue = "test_installation_id"
	testEntryKey        = "test_key"
	testEntryValue      = "test_value"
)

func TestNewMetadata_Success(t *testing.T) {
	ctx := context.Background()
	logger := logging.FromContext(ctx)
	provider := &mockMetadataProvider{
		metadata: map[string]string{
			installationIDKey: installationIDValue,
			testEntryKey:      testEntryValue,
		},
	}
	cfg := &config.BaseConfig{
		Blockstore: config.Blockstore{
			Type: "s3",
			S3:   &config.BlockstoreS3{},
		},
	}

	metadata := stats.NewMetadata(ctx, logger, provider, cfg.StorageConfig())

	require.Equal(t, "test_installation_id", metadata.InstallationID)
	require.Len(t, metadata.Entries, 5)
	require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: installationIDKey, Value: installationIDValue})
	require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: testEntryKey, Value: testEntryValue})
	require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: stats.BlockstoreTypeKey, Value: "s3"})
}
