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
	defaultCacheExpiry = 1 * time.Second
)

type cacheKey struct {
	RepositoryID graveler.RepositoryID
	Key          string
}

type updateFunc func(proto.Message) error

// TODO (niro): Remove interface once DB implementation is deleted
type Manager interface {
	// TODO (niro): Delete Save (unused)
	Save(context.Context, graveler.RepositoryID, string, proto.Message) error
	GetLatest(context.Context, graveler.RepositoryID, string, proto.Message) (proto.Message, error)
	Get(context.Context, graveler.RepositoryID, string, proto.Message) (proto.Message, error)
	Update(context.Context, graveler.RepositoryID, string, proto.Message, updateFunc) error
	WithCache(cache cache.Cache)
}

// KVManager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and can be any proto.Message.
// Fetched settings are cached using cache.Cache with a default expiry time of 1 second. Hence, the store is eventually consistent.
type KVManager struct {
	store      kv.StoreMessage
	refManager graveler.RefManager
	cache      cache.Cache
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

func NewManager(refManager graveler.RefManager, store kv.StoreMessage, options ...ManagerOption) *KVManager {
	m := &KVManager{
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
func (m *KVManager) Save(ctx context.Context, repositoryID graveler.RepositoryID, key string, setting proto.Message) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}

	logSetting(logging.FromContext(ctx), repositoryID, key, setting, "saving repository-level setting")
	// Write key in KV Store
	return m.store.SetMsg(ctx, graveler.RepoPartition(&graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}), []byte(graveler.SettingsPath(key)), setting)
}

func (m *KVManager) getWithPredicate(ctx context.Context, repo *graveler.RepositoryRecord, key string, data proto.Message) (kv.Predicate, error) {
	pred, err := m.store.GetMsg(ctx, graveler.RepoPartition(repo), []byte(graveler.SettingsPath(key)), data)
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

	data := settingTemplate.ProtoReflect().Interface()
	_, err = m.getWithPredicate(ctx, &graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, key, data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, data, "got repository-level setting")
	return data, nil
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
	return setting.(proto.Message), nil
}

// Update atomically gets a setting, performs the update function, and persists the setting to the store.
// The settingTemplate parameter is used to determine the type passed to the update function.
func (m *KVManager) Update(ctx context.Context, repositoryID graveler.RepositoryID, key string, settingTemplate proto.Message, update updateFunc) error {
	const (
		maxIntervalSec = 2
		maxElapsedSec  = 5
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxIntervalSec * time.Second
	bo.MaxElapsedTime = maxElapsedSec * time.Second

	err := backoff.Retry(func() error {
		repo, err := m.refManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return backoff.Permanent(err)
		}

		data := settingTemplate.ProtoReflect().Interface()
		pred, err := m.getWithPredicate(ctx, &graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, key, data)
		if errors.Is(err, graveler.ErrNotFound) {
			data = proto.Clone(settingTemplate)
		} else if err != nil {
			return backoff.Permanent(err)
		}

		logSetting(logging.FromContext(ctx), repositoryID, key, data, "update repository-level setting")
		err = update(data)
		if err != nil {
			return backoff.Permanent(err)
		}
		err = m.store.SetMsgIf(ctx, graveler.RepoPartition(&graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: repo}), []byte(graveler.SettingsPath(key)), data, pred)
		if errors.Is(err, kv.ErrPredicateFailed) {
			logging.Default().WithError(err).Warn("Predicate failed on settings update. Retrying")
			err = graveler.ErrPreconditionFailed
		} else if err != nil {
			return backoff.Permanent(err)
		}
		return err
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
