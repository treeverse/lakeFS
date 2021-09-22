package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	settingsRelativeKey = "%s/settings/%s.json" // where the settings are saved relative to the storage namespace
	cacheSize           = 100_000
	defaultCacheExpiry  = 1 * time.Second
)

type cacheKey struct {
	RepositoryID graveler.RepositoryID
	Key          string
}

// Manager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and can be any proto.Message.
// Fetched settings are cached using cache.Cache with a default expiry time of 1 second. Hence, the store is eventually consistent.
type Manager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
	cache                       cache.Cache
}

type ManagerOption func(m *Manager)

func WithCache(cache cache.Cache) ManagerOption {
	return func(m *Manager) {
		m.cache = cache
	}
}

func NewManager(refManager graveler.RefManager, branchLock graveler.BranchLocker, blockAdapter block.Adapter, committedBlockStoragePrefix string, options ...ManagerOption) *Manager {
	m := &Manager{
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

// Save persists the given setting under the given repository and key.
func (m *Manager) Save(ctx context.Context, repositoryID graveler.RepositoryID, key string, setting proto.Message) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	messageBytes, err := proto.Marshal(setting)
	if err != nil {
		return err
	}
	err = m.blockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsRelativeKey, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
	if err != nil {
		return err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, setting, "saving repository-level setting")
	return nil
}

// Get fetches the setting under the given repository and key, and returns the result.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
// The settingTemplate parameter is used to determine the returned type.
func (m *Manager) Get(ctx context.Context, repositoryID graveler.RepositoryID, key string, settingTemplate proto.Message) (proto.Message, error) {
	k := cacheKey{
		RepositoryID: repositoryID,
		Key:          key,
	}
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		messageBytes, err := m.getFromStore(ctx, repositoryID, key)
		if err != nil {
			return nil, err
		}
		setting := proto.Clone(settingTemplate)
		err = proto.Unmarshal(messageBytes, setting)
		if err != nil {
			return nil, err
		}
		return setting, nil
	})
	if err != nil {
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, setting.(proto.Message), "got repository-level setting")
	return setting.(proto.Message), nil
}

func (m *Manager) getFromStore(ctx context.Context, repositoryID graveler.RepositoryID, key string) ([]byte, error) {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsRelativeKey, m.committedBlockStoragePrefix, key),
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

// UpdateWithLock atomically gets a setting, performs the update function, and persists the setting to the store.
// The emptySetting parameter is used to determine the type passed to the update function.
func (m *Manager) UpdateWithLock(ctx context.Context, repositoryID graveler.RepositoryID, key string, emptySetting proto.Message, update func(setting proto.Message)) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	_, err = m.branchLock.MetadataUpdater(ctx, repositoryID, repo.DefaultBranchID, func() (interface{}, error) {
		setting := proto.Clone(emptySetting)
		messageBytes, err := m.getFromStore(ctx, repositoryID, key)
		if err == nil {
			err = proto.Unmarshal(messageBytes, setting)
			if err != nil {
				return nil, err
			}
		} else if !errors.Is(err, graveler.ErrNotFound) {
			return nil, err
		}
		logSetting(logging.FromContext(ctx), repositoryID, key, setting, "got repository-level setting")
		update(setting)
		err = m.Save(ctx, repositoryID, key, setting)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func logSetting(logger logging.Logger, repositoryID graveler.RepositoryID, key string, setting proto.Message, logMsg string) {
	if logger.IsTracing() {
		logger.
			WithField("repo", repositoryID).
			WithField("key", key).
			WithField("setting", protojson.Format(setting)).
			Trace(logMsg)
	}
}
