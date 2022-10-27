package auth

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	InstallationIDKeyName = "installation_id"
	SetupTimestampKeyName = "setup_timestamp"
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType("auth", InstallationIDKeyName, nil)
	kv.MustRegisterType("auth", SetupTimestampKeyName, nil)
}

type MetadataManager interface {
	IsInitialized(ctx context.Context) (bool, error)
	UpdateSetupTimestamp(context.Context, time.Time) error
	Write(context.Context) (map[string]string, error)
}

type KVMetadataManager struct {
	version        string
	kvType         string
	installationID string
	store          kv.Store
}

func NewKVMetadataManager(version, fixedInstallationID, kvType string, store kv.Store) *KVMetadataManager {
	return &KVMetadataManager{
		version:        version,
		kvType:         kvType,
		installationID: generateInstallationID(fixedInstallationID),
		store:          store,
	}
}

func generateInstallationID(installationID string) string {
	if installationID == "" {
		installationID = uuid.New().String()
	}
	return installationID
}

func (m *KVMetadataManager) insertOrGetInstallationID(ctx context.Context, installationID string) (string, error) {
	err := m.store.SetIf(ctx, []byte(model.PartitionKey), []byte(InstallationIDKeyName), []byte(installationID), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			valWithPred, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(InstallationIDKeyName))
			if err != nil {
				return "", err
			}
			return string(valWithPred.Value), nil
		}
		return "", err
	}
	return installationID, nil
}

func (m *KVMetadataManager) getSetupTimestamp(ctx context.Context) (time.Time, error) {
	valWithPred, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(SetupTimestampKeyName)))
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, string(valWithPred.Value))
}

func (m *KVMetadataManager) writeMetadata(ctx context.Context, items map[string]string) error {
	for key, value := range items {
		kvKey := model.MetadataKeyPath(key)
		err := m.store.Set(ctx, []byte(model.PartitionKey), []byte(kvKey), []byte(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *KVMetadataManager) UpdateSetupTimestamp(ctx context.Context, ts time.Time) error {
	return m.writeMetadata(ctx, map[string]string{
		SetupTimestampKeyName: ts.UTC().Format(time.RFC3339),
	})
}

func (m *KVMetadataManager) IsInitialized(ctx context.Context) (bool, error) {
	setupTimestamp, err := m.getSetupTimestamp(ctx)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return !setupTimestamp.IsZero(), nil
}

func (m *KVMetadataManager) Write(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = m.version
	metadata["lakefs_kv_type"] = m.kvType
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS
	err := m.writeMetadata(ctx, metadata)
	if err != nil {
		return nil, err
	}
	// write installation id
	m.installationID, err = m.insertOrGetInstallationID(ctx, m.installationID)
	if err == nil {
		metadata[InstallationIDKeyName] = m.installationID
	}

	setupTS, err := m.getSetupTimestamp(ctx)
	if err == nil {
		metadata[SetupTimestampKeyName] = setupTS.UTC().Format(time.RFC3339)
	}

	return metadata, err
}
