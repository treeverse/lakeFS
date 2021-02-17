package catalog

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/graveler"
)

const repositoryLocation = "_lakefs_actions/"

type ActionsSource struct {
	catalog *EntryCatalog
}

func NewActionsSource(entryCatalog *EntryCatalog) *ActionsSource {
	return &ActionsSource{
		catalog: entryCatalog,
	}
}

func (s *ActionsSource) List(ctx context.Context, record graveler.HookRecord) ([]string, error) {
	it, err := s.catalog.ListEntries(ctx, record.RepositoryID, record.SourceRef, repositoryLocation, DefaultPathDelimiter)
	if err != nil {
		return nil, fmt.Errorf("listing actions: %w", err)
	}
	defer it.Close()

	var names []string
	for it.Next() {
		name := it.Value().Path.String()
		names = append(names, name)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("entries iterator: %w", err)
	}
	return names, nil
}

func (s *ActionsSource) Load(ctx context.Context, record graveler.HookRecord, name string) ([]byte, error) {
	// get name's address
	repositoryID := record.RepositoryID
	ent, err := s.catalog.GetEntry(ctx, repositoryID, record.SourceRef, Path(name))
	if err != nil {
		return nil, fmt.Errorf("get action file metadata %s: %w", name, err)
	}
	// get repo storage namespace
	repo, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, fmt.Errorf("get repository %s: %w", repositoryID, err)
	}
	// get action address
	blockAdapter := s.catalog.BlockAdapter
	reader, err := blockAdapter.
		WithContext(ctx).
		Get(block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace.String(),
			Identifier:       ent.Address,
		}, 0)
	if err != nil {
		return nil, fmt.Errorf("getting action file %s: %w", name, err)
	}
	defer func() {
		_ = reader.Close()
	}()

	// read action
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading action file %s: %w", name, err)
	}
	return bytes, nil
}
