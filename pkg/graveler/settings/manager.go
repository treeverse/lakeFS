package settings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/treeverse/lakefs/modules/cache"
	"time"

	"github.com/go-openapi/swag"
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

// Save persists the given setting under the given repository and key. Overrides settings key in KV Store.
// The setting is persisted only if the current version of the setting matches the given checksum.
// If lastKnownChecksum is the empty string, the setting is persisted only if it does not exist.
// If lastKnownChecksum is nil, the setting is persisted unconditionally.
func (m *Manager) Save(ctx context.Context, repository *graveler.RepositoryRecord, key string, setting proto.Message, lastKnownChecksum *string) error {
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, setting, "saving repository-level setting")
	repoPartition := graveler.RepoPartition(repository)
	keyPath := []byte(graveler.SettingsPath(key))
	if lastKnownChecksum == nil {
		return kv.SetMsg(ctx, m.store, repoPartition, keyPath, setting)
	}
	if *lastKnownChecksum == "" {
		err := kv.SetMsgIf(ctx, m.store, repoPartition, keyPath, setting, nil)
		if errors.Is(err, kv.ErrPredicateFailed) {
			return graveler.ErrPreconditionFailed
		}
		return err
	}
	valueWithPredicate, err := m.store.Get(ctx, []byte(repoPartition), keyPath)
	if errors.Is(err, kv.ErrNotFound) {
		return graveler.ErrPreconditionFailed
	}
	if err != nil {
		return err
	}
	currentChecksum, err := computeChecksum(valueWithPredicate.Value)
	if err != nil {
		return err
	}
	if *currentChecksum != *lastKnownChecksum {
		return graveler.ErrPreconditionFailed
	}
	err = kv.SetMsgIf(ctx, m.store, repoPartition, keyPath, setting, valueWithPredicate.Predicate)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return graveler.ErrPreconditionFailed
	}
	return err
}

func computeChecksum(value []byte) (*string, error) {
	if len(value) == 0 {
		// empty value checksum is the empty string
		return swag.String(""), nil
	}
	h := sha256.New()
	_, err := h.Write(value)
	if err != nil {
		return nil, err
	}
	return swag.String(hex.EncodeToString(h.Sum(nil))), nil
}

// GetLatest loads the latest setting into dst.
// It returns a checksum representing the version of the setting, which can be passed to SaveIf for conditional updates.
// The checksum of a non-existent setting is the empty string.
func (m *Manager) GetLatest(ctx context.Context, repository *graveler.RepositoryRecord, key string, dst proto.Message) (*string, error) {
	settings, err := m.store.Get(ctx, []byte(graveler.RepoPartition(repository)), []byte(graveler.SettingsPath(key)))
	if errors.Is(err, kv.ErrNotFound) {
		// return empty list and do not consider this an error
		proto.Reset(dst)
		return swag.String(""), nil
	}
	if err != nil {
		return nil, err
	}
	checksum, err := computeChecksum(settings.Value)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(settings.Value, dst)
	if err != nil {
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, dst, "got repository-level setting")
	return checksum, nil
}

// Get fetches the setting under the given repository and key, and loads it into dst.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
func (m *Manager) Get(ctx context.Context, repository *graveler.RepositoryRecord, key string, dst proto.Message) error {
	k := cacheKey{
		RepositoryID: repository.RepositoryID,
		Key:          key,
	}
	tmp := proto.Clone(dst)
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		_, err = m.GetLatest(ctx, repository, key, tmp)
		if errors.Is(err, graveler.ErrNotFound) {
			// do not return this error here, so that empty settings are cached
			return tmp, nil
		}
		return tmp, err
	})
	if err != nil {
		return err
	}
	if setting == nil {
		return graveler.ErrNotFound
	}
	proto.Merge(dst, setting.(proto.Message))
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
