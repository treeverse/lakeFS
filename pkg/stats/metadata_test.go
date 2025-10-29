package stats

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
)

type mockMetadataProvider struct {
	metadata map[string]string
	err      error
}

func (m *mockMetadataProvider) GetMetadata(context.Context) (map[string]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.metadata, nil
}

func TestNewMetadata(t *testing.T) {
	ctx := context.Background()
	logger := logging.Dummy()

	t.Run("extract installation_id", func(t *testing.T) {
		provider := &mockMetadataProvider{
			metadata: map[string]string{
				"installation_id": "test-id-123",
				"other_key":       "other_value",
			},
		}

		metadata := NewMetadata(ctx, logger, []MetadataProvider{provider})

		assert.Equal(t, "test-id-123", metadata.InstallationID)
		assert.Len(t, metadata.Entries, 2)
	})

	t.Run("multiple providers with same key", func(t *testing.T) {
		provider1 := &mockMetadataProvider{
			metadata: map[string]string{
				"common_key": "value1",
				"key1":       "value1",
			},
		}
		provider2 := &mockMetadataProvider{
			metadata: map[string]string{
				"common_key": "value2",
				"key2":       "value2",
			},
		}

		metadata := NewMetadata(ctx, logger, []MetadataProvider{provider1, provider2})

		require.Len(t, metadata.Entries, 4)

		// Verify all entries are present
		entriesMap := make(map[string][]string)
		for _, entry := range metadata.Entries {
			entriesMap[entry.Name] = append(entriesMap[entry.Name], entry.Value)
		}

		require.Len(t, entriesMap["common_key"], 2)
		require.Contains(t, entriesMap["common_key"], "value1")
		require.Contains(t, entriesMap["common_key"], "value2")
		require.Equal(t, []string{"value1"}, entriesMap["key1"])
		require.Equal(t, []string{"value2"}, entriesMap["key2"])
	})

	t.Run("skip failed provider", func(t *testing.T) {
		failingProvider := &mockMetadataProvider{
			err: errors.New("provider failed"),
		}
		successProvider := &mockMetadataProvider{
			metadata: map[string]string{
				"installation_id": "test-id-456",
				"key1":            "value1",
			},
		}

		metadata := NewMetadata(ctx, logger, []MetadataProvider{failingProvider, successProvider})

		// Verify that metadata from successful provider is present
		require.Len(t, metadata.Entries, 2)
		assert.Equal(t, "test-id-456", metadata.InstallationID)
		assert.Contains(t, metadata.Entries, MetadataEntry{Name: "key1", Value: "value1"})
	})
}
