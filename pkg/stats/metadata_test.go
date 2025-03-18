package stats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/cloud"
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

func TestNewMetadata(t *testing.T) {
	tests := []struct {
		name                   string
		blockstore             config.Blockstore
		expectedBlockstoreType string
		expectedIDKeyType      string
		expectedIDKey          string
	}{
		{
			name: "s3",
			blockstore: config.Blockstore{
				Type: "s3",
				S3:   &config.BlockstoreS3{},
			},
			expectedBlockstoreType: "s3",
			expectedIDKeyType:      "err", // no s3 cloud provider in the test env, hence err
			expectedIDKey:          "err", // no s3 cloud provider in the test env, hence err
		},
		{
			name: "azure",
			blockstore: config.Blockstore{
				Type:  "azure",
				Azure: &config.BlockstoreAzure{},
			},
			expectedBlockstoreType: "azure",
			expectedIDKeyType:      "err", // no azure cloud provider in the test env, hence err
			expectedIDKey:          "err", // no azure cloud provider in the test env, hence err
		},
		{
			name: "gs",
			blockstore: config.Blockstore{
				Type: "gs",
				GS:   &config.BlockstoreGS{},
			},
			expectedBlockstoreType: "gs",
			expectedIDKeyType:      "err", // no gs cloud provider in the test env, hence err
			expectedIDKey:          "err", // no gs cloud provider in the test env, hence err
		},
		{
			name: "local",
			blockstore: config.Blockstore{
				Type: "local",
			},
			expectedBlockstoreType: "local",
			expectedIDKeyType:      "nil",
			expectedIDKey:          "nil",
		},
	}

	for _, tt := range tests {
		ctx := context.Background()
		logger := logging.FromContext(ctx)
		provider := &mockMetadataProvider{
			metadata: map[string]string{
				installationIDKey: installationIDValue,
				testEntryKey:      testEntryValue,
			},
		}

		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.BaseConfig{
				Blockstore: tt.blockstore,
			}

			metadata := stats.NewMetadata(ctx, logger, provider, cfg.StorageConfig())

			require.Equal(t, installationIDValue, metadata.InstallationID)
			require.Len(t, metadata.Entries, 5)
			require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: installationIDKey, Value: installationIDValue})
			require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: testEntryKey, Value: testEntryValue})
			require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: stats.BlockstoreTypeKey, Value: tt.expectedBlockstoreType})
			require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: cloud.IDTypeKey, Value: tt.expectedIDKeyType})
			require.Contains(t, metadata.Entries, stats.MetadataEntry{Name: cloud.IDKey, Value: tt.expectedIDKey})
		})
	}
}
