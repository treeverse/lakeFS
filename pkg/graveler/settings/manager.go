package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	settingsSuffixTemplate = "%s/settings/%s.json"
)

type cache struct {
	lock   sync.RWMutex
	caches map[graveler.RepositoryID]*repoCache
}

type repoCache struct {
	lock    sync.RWMutex
	entries map[string]*entry
}

type entry struct {
	message proto.Message
	expiry  time.Time
}

// Manager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and it can be any proto.Message.
type Manager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
	cache                       *cache
	timeGetter                  func() time.Time
}

func NewManager(refManager graveler.RefManager, branchLock graveler.BranchLocker, blockAdapter block.Adapter, committedBlockStoragePrefix string) *Manager {
	return &Manager{
		refManager:                  refManager,
		branchLock:                  branchLock,
		blockAdapter:                blockAdapter,
		committedBlockStoragePrefix: committedBlockStoragePrefix,
		cache: &cache{
			caches: make(map[graveler.RepositoryID]*repoCache),
		},
		timeGetter: time.Now,
	}
}

// Save persists the given proto.Message under the given repository and key.
func (m *Manager) Save(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	err = m.blockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsSuffixTemplate, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
	if err != nil {
		return err
	}
	logger := logging.FromContext(ctx)
	if logger.IsTracing() {
		logger.
			WithField("repo", repositoryID).
			WithField("key", key).
			WithField("setting", protojson.Format(message)).
			Trace("saving repository-level setting")
	}
	return nil
}

// Get fetches the setting under the given repository and key.
// The result is placed in message.
// The result is eventually consistent: it is not guaranteed to be the most up-to-date setting. The cache expiry period is 1 second.
func (m *Manager) Get(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message) error {
	rCache := m.getRepoCache(repositoryID)
	e, err := m.getEntry(ctx, repositoryID, key, message, rCache)
	if err != nil {
		return err
	}
	proto.Merge(message, e.message)
	return nil
}

// getEntry returns the setting entry for the given key from the given repoCache.
// If the entry is not in the cache or is expired, the entry is retrieved from the storage.
// This method is thread-safe. Only one concurrent request to the storage will occur.
func (m *Manager) getEntry(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message, rCache *repoCache) (*entry, error) {
	rCache.lock.RLock()
	e, ok := rCache.entries[key]
	rCache.lock.RUnlock()
	if !ok || e.expiry.Before(m.timeGetter()) {
		rCache.lock.Lock()
		defer rCache.lock.Unlock()
		e, ok = rCache.entries[key]
		if !ok || e.expiry.Before(m.timeGetter()) {
			messageBytes, err := m.getFromStore(ctx, repositoryID, key)
			if err != nil {
				return nil, err
			}
			err = proto.Unmarshal(messageBytes, message)
			if err != nil {
				return nil, err
			}
			logSetting(logging.FromContext(ctx), repositoryID, key, message)
			e = &entry{
				message: message,
				expiry:  m.timeGetter().Add(1 * time.Second),
			}
			rCache.entries[key] = e
		}
	}
	return e, nil
}

// getRepoCache returns the repoCache for the given repository.
// If there is repoCache for this repository, it is created.
// This method is thread-safe, and concurrent calls are guaranteed to return the same instance.
func (m *Manager) getRepoCache(repositoryID graveler.RepositoryID) *repoCache {
	m.cache.lock.RLock()
	rCache := m.cache.caches[repositoryID]
	m.cache.lock.RUnlock()
	if rCache == nil {
		m.cache.lock.Lock()
		rCache = m.cache.caches[repositoryID]
		if rCache == nil {
			rCache = &repoCache{
				entries: make(map[string]*entry),
			}
			m.cache.caches[repositoryID] = rCache
		}
		m.cache.lock.Unlock()
	}
	return rCache
}

func logSetting(logger logging.Logger, repositoryID graveler.RepositoryID, key string, message proto.Message) {
	if logger.IsTracing() {
		logger.
			WithField("repo", repositoryID).
			WithField("key", key).
			WithField("setting", protojson.Format(message)).
			Trace("got repository-level setting")
	}
}

func (m *Manager) getFromStore(ctx context.Context, repositoryID graveler.RepositoryID, key string) ([]byte, error) {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsSuffixTemplate, m.committedBlockStoragePrefix, key),
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

// UpdateWithLock atomically performs the following sequence:
// 1. Fetch the setting under the given repository and key, and place it in message.
// 2. Call the given update function (which should presumably update the message).
// 3. Persist message to the store.
func (m *Manager) UpdateWithLock(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message, update func()) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	_, err = m.branchLock.MetadataUpdater(ctx, repositoryID, repo.DefaultBranchID, func() (interface{}, error) {
		messageBytes, err := m.getFromStore(ctx, repositoryID, key)
		if err != nil {
			return nil, err
		}
		err = proto.Unmarshal(messageBytes, message)
		if err != nil {
			return nil, err
		}
		logSetting(logging.FromContext(ctx), repositoryID, key, message)
		update()
		err = m.Save(ctx, repositoryID, key, message)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
