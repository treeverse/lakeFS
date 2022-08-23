package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

// DBManager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and can be any proto.Message.
// Fetched settings are cached using cache.Cache with a default expiry time of 1 second. Hence, the store is eventually consistent.
type DBManager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
	cache                       cache.Cache
}

func NewDBManager(refManager graveler.RefManager, branchLock graveler.BranchLocker, blockAdapter block.Adapter, committedBlockStoragePrefix string, options ...ManagerOption) Manager {
	m := &DBManager{
		refManager:                  refManager,
		branchLock:                  branchLock,
		blockAdapter:                blockAdapter,
		committedBlockStoragePrefix: committedBlockStoragePrefix,
		cache:                       cache.NewCache(cacheSize, defaultCacheExpiry, cache.NewJitterFn(defaultCacheExpiry)),
	}
	for _, o := range options {
		o(m)
	}
	return m
}

func (m *DBManager) WithCache(cache cache.Cache) {
	m.cache = cache
}

// Save persists the given setting under the given repository and key.
func (m *DBManager) Save(ctx context.Context, repository *graveler.RepositoryRecord, key string, setting proto.Message) error {
	messageBytes, err := proto.Marshal(setting)
	if err != nil {
		return err
	}
	err = m.blockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: string(repository.StorageNamespace),
		Identifier:       fmt.Sprintf(graveler.SettingsRelativeKey, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
	if err != nil {
		return err
	}
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting, "saving repository-level setting")
	return nil
}

func (m *DBManager) GetLatest(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, error) {
	messageBytes, err := m.getFromStore(ctx, repository, key)
	if err != nil {
		return nil, err
	}
	setting := proto.Clone(settingTemplate)
	err = proto.Unmarshal(messageBytes, setting)
	if err != nil {
		return nil, err
	}
	return setting, nil
}

// Get fetches the setting under the given repository and key, and returns the result.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
// The settingTemplate parameter is used to determine the returned type.
func (m *DBManager) Get(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, error) {
	k := cacheKey{
		RepositoryID: repository.RepositoryID,
		Key:          key,
	}
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		return m.GetLatest(ctx, repository, key, settingTemplate)
	})
	if err != nil {
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting.(proto.Message), "got repository-level setting")
	return setting.(proto.Message), nil
}

func (m *DBManager) getFromStore(ctx context.Context, repository *graveler.RepositoryRecord, key string) ([]byte, error) {
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(repository.StorageNamespace),
		Identifier:       fmt.Sprintf(graveler.SettingsRelativeKey, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.blockAdapter.Get(ctx, objectPointer, -1)
	if errors.Is(err, adapter.ErrDataNotFound) {
		return nil, graveler.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	return io.ReadAll(reader)
}

// Update atomically gets a setting, performs the update function, and persists the setting to the store.
// The settingTemplate parameter is used to determine the type passed to the update function.
func (m *DBManager) Update(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message, update updateFunc) error {
	_, err := m.branchLock.MetadataUpdater(ctx, repository, repository.DefaultBranchID, func() (interface{}, error) {
		setting, err := m.GetLatest(ctx, repository, key, settingTemplate)
		if errors.Is(err, graveler.ErrNotFound) {
			setting = proto.Clone(settingTemplate)
		} else if err != nil {
			return nil, err
		}
		logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting, "got repository-level setting")
		err = update(setting)
		if err != nil {
			return nil, err
		}
		return nil, m.Save(ctx, repository, key, setting)
	})
	return err
}
