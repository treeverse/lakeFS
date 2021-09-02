package settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/graveler"
	"google.golang.org/protobuf/proto"
)

const (
	settingsSuffixTemplate = "/%s/settings/%s.json"
)

type Manager struct {
	refManager                  graveler.RefManager
	branchLock                  graveler.BranchLocker
	blockAdapter                block.Adapter
	committedBlockStoragePrefix string
}

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

func main() {
	m := &Manager{
		blockAdapter:                mem.New(),
		committedBlockStoragePrefix: "",
	}
	rules := &graveler.GarbageCollectionRules{
		DefaultRetentionDays: 1231,
		BranchRetentionDays:  map[string]int32{"boo": -11},
	}
	err := m.Save(context.Background(), "", "rules", rules)
	if err != nil {
		log.Fatalf("got error: %v", err)
	}
	outRules := &graveler.GarbageCollectionRules{}
	err = m.Get(context.Background(), "", "rules", outRules)
	if err != nil {
		log.Fatalf("got error: %v", err)
	}
	fmt.Printf("%d\n", outRules.BranchRetentionDays["boo"])
	outRules2 := &graveler.GarbageCollectionRules{}
	err = m.UpdateWithLock(context.Background(), "", "rules", outRules2, func() {
		outRules2.DefaultRetentionDays = 341
		outRules2.BranchRetentionDays["loo"] = -341
	})
	if err != nil {
		log.Fatalf("got error: %v", err)
	}
	fmt.Printf("%d\n", outRules2.BranchRetentionDays["loo"])
}
