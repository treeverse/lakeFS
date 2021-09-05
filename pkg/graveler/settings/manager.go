package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/graveler"
	"google.golang.org/protobuf/proto"
)

const (
	settingsSuffixTemplate = "/%s/settings/%s.json"
)

// Manager is a key-value store for Graveler repository-level settings.
// Each setting is stored under a key, and it can be any proto.Message.
// A setting is saved as an object in the backing storage, so the performance of save/get is bound by the
// performance of a storage put/get respectively.
type Manager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
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
	return m.blockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsSuffixTemplate, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(messageBytes)), bytes.NewReader(messageBytes), block.PutOpts{})
}

// Get fetches the configuration under the given repository and key.
// The result is placed in message.
func (m *Manager) Get(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(repo.StorageNamespace),
		Identifier:       fmt.Sprintf(settingsSuffixTemplate, m.committedBlockStoragePrefix, key),
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.blockAdapter.Get(ctx, objectPointer, -1)
	if errors.Is(err, adapter.ErrDataNotFound) {
		return graveler.ErrNotFound
	}
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()
	messageBytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return proto.Unmarshal(messageBytes, message)
}

// UpdateWithLock atomically performs the following sequence:
// 1. Fetch the configuration under the given repository and key, and place it in message.
// 2. Call the given update function (which should presumably update the message).
// 3. Persist message to the store.
func (m *Manager) UpdateWithLock(ctx context.Context, repositoryID graveler.RepositoryID, key string, message proto.Message, update func()) error {
	repo, err := m.refManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	_, err = m.branchLock.MetadataUpdater(ctx, repositoryID, repo.DefaultBranchID, func() (interface{}, error) {
		err = m.Get(ctx, repositoryID, key, message)
		if err != nil {
			return nil, err
		}
		update()
		err = m.Save(ctx, repositoryID, key, message)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
