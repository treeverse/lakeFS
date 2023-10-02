package settings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	cacheSize          = 100_000
	defaultCacheExpiry = 3 * time.Second
)

type cacheKey struct {
	RepositoryID graveler.RepositoryID
	Key          string
}

// Manager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and can be any proto.Message.
// Fetched settings are cached using cache.Cache with a default expiry time of 1 second. Hence, the store is eventually consistent.
type Manager struct {
	store      kv.Store
	refManager graveler.RefManager
	cache      cache.Cache
}

type ManagerOption func(m *Manager)

func WithCache(cache cache.Cache) ManagerOption {
	return func(m *Manager) {
		m.WithCache(cache)
	}
}

func (m *Manager) WithCache(cache cache.Cache) {
	m.cache = cache
}

func NewManager(refManager graveler.RefManager, store kv.Store, options ...ManagerOption) *Manager {
	m := &Manager{
		refManager: refManager,
		store:      store,
		cache:      cache.NewCache(cacheSize, defaultCacheExpiry, cache.NewJitterFn(defaultCacheExpiry)),
	}
	for _, o := range options {
		o(m)
	}
	return m
}

// Save persists the given setting under the given repository and key. Overrides settings key in KV Store
func (m *Manager) Save(ctx context.Context, repository *graveler.RepositoryRecord, key string, setting proto.Message) error {
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting, "saving repository-level setting")
	// Write key in KV Store
	return kv.SetMsg(ctx, m.store, graveler.RepoPartition(repository), []byte(graveler.SettingsPath(key)), setting)
}

// SaveIf persists the given setting under the given repository and key. Overrides settings key in KV Store.
// The setting is persisted only if the current version of the setting matches the given checksum.
// If lastKnownChecksum is nil, the setting is persisted only if it does not exist.
func (m *Manager) SaveIf(ctx context.Context, repository *graveler.RepositoryRecord, key string, setting proto.Message, lastKnownChecksum *string) error {
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting, "saving repository-level setting")
	valueWithPredicate, err := m.store.Get(ctx, []byte(graveler.RepoPartition(repository)), []byte(graveler.SettingsPath(key)))
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	var currentChecksum *string
	var currentPredicate kv.Predicate
	if valueWithPredicate != nil && valueWithPredicate.Value != nil {
		currentChecksum, err = computeChecksum(valueWithPredicate.Value)
		if err != nil {
			return err
		}
		currentPredicate = valueWithPredicate.Predicate
	}
	if swag.StringValue(currentChecksum) != swag.StringValue(lastKnownChecksum) {
		return graveler.ErrPreconditionFailed
	}
	err = kv.SetMsgIf(ctx, m.store, graveler.RepoPartition(repository), []byte(graveler.SettingsPath(key)), setting, currentPredicate)
	if err != nil && errors.Is(err, kv.ErrPredicateFailed) {
		return graveler.ErrPreconditionFailed
	}
	return err
}

func computeChecksum(value []byte) (*string, error) {
	h := sha256.New()
	_, err := h.Write(value)
	if err != nil {
		return nil, err
	}
	return swag.String(hex.EncodeToString(h.Sum(nil))), nil
}

// GetLatest returns the latest setting under the given repository and key, without using the cache.
// The returned checksum represents the version of the setting, and can be passed to SaveIf for conditional updates.
func (m *Manager) GetLatest(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, *string, error) {
	settings, err := m.store.Get(ctx, []byte(graveler.RepoPartition(repository)), []byte(graveler.SettingsPath(key)))
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, nil, err
	}
	checksum, err := computeChecksum(settings.Value)
	if err != nil {
		return nil, nil, err
	}
	data := settingTemplate.ProtoReflect().Interface()
	err = proto.Unmarshal(settings.Value, data)
	if err != nil {
		return nil, nil, err
	}
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, data, "got repository-level setting")
	return data, checksum, nil
}

// Get fetches the setting under the given repository and key, and returns the result.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
// The settingTemplate parameter is used to determine the returned type.
// The returned checksum represents the version of the setting, and can be used in SaveIf to perform an atomic update.
func (m *Manager) Get(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, error) {
	k := cacheKey{
		RepositoryID: repository.RepositoryID,
		Key:          key,
	}
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		setting, _, err := m.GetLatest(ctx, repository, key, settingTemplate)
		if errors.Is(err, graveler.ErrNotFound) {
			return nil, nil
		}
		return setting, err
	})
	if err != nil {
		return nil, err
	}
	if setting == nil {
		return nil, graveler.ErrNotFound
	}
	return setting.(proto.Message), nil
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
