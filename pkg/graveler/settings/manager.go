package settings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
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

type updateFunc func(proto.Message) (proto.Message, error)

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

func (m *Manager) getWithPredicate(ctx context.Context, repo *graveler.RepositoryRecord, key string, data proto.Message) (kv.Predicate, error) {
	pred, err := kv.GetMsg(ctx, m.store, graveler.RepoPartition(repo), []byte(graveler.SettingsPath(key)), data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, err
	}
	return pred, nil
}

func (m *Manager) GetLatest(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, error) {
	data := settingTemplate.ProtoReflect().Interface()
	_, err := m.getWithPredicate(ctx, repository, key, data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repository.RepositoryID, key, data, "got repository-level setting")
	return data, nil
}

// Get fetches the setting under the given repository and key, and returns the result.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
// The settingTemplate parameter is used to determine the returned type.
func (m *Manager) Get(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message) (proto.Message, error) {
	k := cacheKey{
		RepositoryID: repository.RepositoryID,
		Key:          key,
	}
	setting, err := m.cache.GetOrSet(k, func() (v interface{}, err error) {
		setting, err := m.GetLatest(ctx, repository, key, settingTemplate)
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

// Update atomically gets a setting, performs the update function, and persists the setting to the store.
// The settingTemplate parameter is used to determine the type passed to the update function.
func (m *Manager) Update(ctx context.Context, repository *graveler.RepositoryRecord, key string, settingTemplate proto.Message, update updateFunc) error {
	const (
		maxIntervalSec = 2
		maxElapsedSec  = 5
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxIntervalSec * time.Second
	bo.MaxElapsedTime = maxElapsedSec * time.Second

	err := backoff.Retry(func() error {
		data := settingTemplate.ProtoReflect().Interface()
		pred, err := m.getWithPredicate(ctx, repository, key, data)
		if errors.Is(err, graveler.ErrNotFound) {
			data = proto.Clone(settingTemplate)
		} else if err != nil {
			return backoff.Permanent(err)
		}

		logSetting(logging.FromContext(ctx), repository.RepositoryID, key, data, "update repository-level setting")
		newData, err := update(data)
		if err != nil {
			return backoff.Permanent(err)
		}
		err = kv.SetMsgIf(ctx, m.store, graveler.RepoPartition(repository), []byte(graveler.SettingsPath(key)), newData, pred)
		if errors.Is(err, kv.ErrPredicateFailed) {
			logging.Default().WithError(err).Warn("Predicate failed on settings update. Retrying")
			return graveler.ErrPreconditionFailed
		} else if err != nil {
			return backoff.Permanent(err)
		}
		return nil
	}, bo)
	if errors.Is(err, graveler.ErrPreconditionFailed) {
		return fmt.Errorf("update settings: %w", graveler.ErrTooManyTries)
	}
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
