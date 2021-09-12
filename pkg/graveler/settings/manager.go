package settings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/treeverse/lakefs/pkg/cache"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	settingsSuffixTemplate = "%s/settings/%s.json"
	cacheSize              = 100_000
	defaultCacheExpiry     = 1 * time.Second
)

type cacheKey struct {
	RepositoryID graveler.RepositoryID
	Key          string
}

// Manager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and it can be any proto.Message.
type Manager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
	cache                       cache.Cache
}

type ManagerOption func(m *Manager)

func WithCacheExpiry(expiry time.Duration) ManagerOption {
	return func(m *Manager) {
		m.cache.SetExpiry(expiry)
	}
}

func NewManager(refManager graveler.RefManager, branchLock graveler.BranchLocker, blockAdapter block.Adapter, committedBlockStoragePrefix string, options ...ManagerOption) *Manager {
	m := &Manager{
		refManager:                  refManager,
		branchLock:                  branchLock,
		blockAdapter:                blockAdapter,
		committedBlockStoragePrefix: committedBlockStoragePrefix,
		cache: cache.NewCache(cacheSize, defaultCacheExpiry, func() time.Duration {
			return 0
		}),
	}
	for _, o := range options {
		o(m)
	}
	return m
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
	qualifiedKey, err := json.Marshal(cacheKey{
		RepositoryID: repositoryID,
		Key:          key,
	})
	if err != nil {
		return err
	}
	messageBytes, err := m.cache.GetOrSet(string(qualifiedKey), func() (v interface{}, err error) {
		return m.getFromStore(ctx, repositoryID, key)
	})
	if err != nil {
		return err
	}
	err = proto.Unmarshal(messageBytes.([]byte), message)
	if err != nil {
		return err
	}
	logSetting(logging.FromContext(ctx), repositoryID, key, message)
	return nil
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
