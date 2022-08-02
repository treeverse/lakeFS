package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	cacheSize                 = 100_000
	defaultCacheExpiry        = 1 * time.Second
	settingsUniqueRelativeKey = "%s/settings/%s_%s.json" // where the settings are saved relative to the storage namespace

	settingsPrefix = "settings"
)

type cacheKey struct {
	RepositoryID graveler.RepositoryID
	Key          string
}

type updateFunc func(proto.Message) error

// TODO: Delete settings from repo path
func settingsPath(key string) string {
	return kv.FormatPath(settingsPrefix, key)
}

// TODO (niro): Remove interface once DB implementation is deleted
type Manager interface {
	Save(context.Context, graveler.RepositoryID, string, proto.Message) error
	GetLatest(context.Context, graveler.RepositoryID, string, proto.Message) (proto.Message, error)
	Get(context.Context, graveler.RepositoryID, string, proto.Message) (proto.Message, error)
	Update(context.Context, graveler.RepositoryID, string, proto.Message, updateFunc) error
	WithCache(ache cache.Cache)
}

// KVManager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and can be any proto.Message.
// Fetched settings are cached using cache.Cache with a default expiry time of 1 second. Hence, the store is eventually consistent.
type KVManager struct {
	store                       kv.StoreMessage
	refManager                  graveler.RefManager
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
	cache                       cache.Cache
}

type ManagerOption func(m Manager)

func WithCache(cache cache.Cache) ManagerOption {
	return func(m Manager) {
		m.WithCache(cache)
	}
}

func (m *KVManager) WithCache(cache cache.Cache) {
	m.cache = cache
}

func NewManager(refManager graveler.RefManager, store kv.StoreMessage, blockAdapter block.Adapter, committedBlockStoragePrefix string, options ...ManagerOption) *KVManager {
	m := &KVManager{
		refManager:                  refManager,
		store:                       store,
		blockAdapter:                blockAdapter,
		committedBlockStoragePrefix: committedBlockStoragePrefix,
		cache:                       cache.NewCache(cacheSize, defaultCacheExpiry, cache.NewJitterFn(defaultCacheExpiry)),
	}
	for _, o := range options {
		o(m)
	}
	return m
}

// Save persists the given setting under the given repository and key. Overrides settings key in KV Store
func (m *KVManager) Save(ctx context.Context, repositoryID graveler.RepositoryID, key string, setting proto.Message) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	messageBytes, err := proto.Marshal(setting)
	if err != nil {
		return err
	}

	data := Settings{
		Identifier: fmt.Sprintf(settingsUniqueRelativeKey, m.committedBlockStoragePrefix, key, xid.NewWithTime(time.Now())),
	}
	obj := block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       data.Identifier,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	// Save to  block store
	err = m.blockAdapter.Put(ctx, obj, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
	if err != nil {
		return err
	}

	// Write key in KV Store
	err = m.store.SetMsg(ctx, graveler.RepoPartition(&graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}), []byte(settingsPath(key)), &data)
	if err != nil {
		// Delete file from blockstore
		_ = m.blockAdapter.Remove(ctx, obj)
		return err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, setting, "saving repository-level setting")
	return nil
}

func (m *KVManager) getWithPredicate(ctx context.Context, repo *graveler.RepositoryRecord, key string, data *Settings) (kv.Predicate, error) {
	pred, err := m.store.GetMsg(ctx, graveler.RepoPartition(repo), []byte(settingsPath(key)), data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, err
	}
	return pred, nil
}

func (m *KVManager) GetLatest(ctx context.Context, repositoryID graveler.RepositoryID, key string, settingTemplate proto.Message) (proto.Message, error) {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	data := Settings{}
	_, err = m.getWithPredicate(ctx, &graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, key, &data)
	if err != nil {
		return nil, err
	}
	messageBytes, err := m.getFromStore(ctx, repo.StorageNamespace, data.Identifier)
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
func (m *KVManager) Get(ctx context.Context, repositoryID graveler.RepositoryID, key string, settingTemplate proto.Message) (proto.Message, error) {
	k := cacheKey{
		RepositoryID: repositoryID,
		Key:          key,
	}
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		return m.GetLatest(ctx, repositoryID, key, settingTemplate)
	})
	if err != nil {
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, setting.(proto.Message), "got repository-level setting")
	return setting.(proto.Message), nil
}

func (m *KVManager) getFromStore(ctx context.Context, storageNamespace graveler.StorageNamespace, identifier string) ([]byte, error) {
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(storageNamespace),
		Identifier:       identifier,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.blockAdapter.Get(ctx, objectPointer, -1)
	if errors.Is(err, adapter.ErrDataNotFound) {
		return nil, graveler.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// Update atomically gets a setting, performs the update function, and persists the setting to the store.
// The settingTemplate parameter is used to determine the type passed to the update function.
func (m *KVManager) Update(ctx context.Context, repositoryID graveler.RepositoryID, key string, settingTemplate proto.Message, update updateFunc) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}

	data := Settings{}
	pred, err := m.getWithPredicate(ctx, &graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, key, &data)
	if err != nil && !errors.Is(err, graveler.ErrNotFound) {
		return err
	}
	setting, err := m.GetLatest(ctx, repositoryID, key, settingTemplate)
	if errors.Is(err, graveler.ErrNotFound) {
		setting = proto.Clone(settingTemplate)
	} else if err != nil {
		return err
	}

	logSetting(logging.FromContext(ctx), repositoryID, key, setting, "got repository-level setting")
	err = update(setting)
	if err != nil {
		return err
	}
	messageBytes, err := proto.Marshal(setting)
	if err != nil {
		return err
	}
	oldIdentifier := data.Identifier // Save old identifier for deletion
	data.Identifier = fmt.Sprintf(settingsUniqueRelativeKey, m.committedBlockStoragePrefix, key, xid.NewWithTime(time.Now()))
	obj := block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       data.Identifier,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	// Save to  blockstore
	err = m.blockAdapter.Put(ctx, obj, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
	if err != nil {
		return err
	}

	// Write key in KV Store
	err = m.store.SetMsgIf(ctx, graveler.RepoPartition(&graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}), []byte(settingsPath(key)), &data, pred)
	if err != nil {
		// Delete file from blockstore
		_ = m.blockAdapter.Remove(ctx, obj)
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = fmt.Errorf("save setting in progress: %w", graveler.ErrPreconditionFailed)
		}
		return err
	}

	// Remove old file
	obj.Identifier = oldIdentifier
	err = m.blockAdapter.Remove(ctx, obj)
	if err != nil {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"StorageNS":  repo.StorageNamespace,
			"Identifier": oldIdentifier,
		}).Warn("Failed to delete old settings file from block store.", repo.StorageNamespace, oldIdentifier)
	}
	return nil
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
