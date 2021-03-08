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
	catalog *Catalog
}

func NewActionsSource(catalog *Catalog) *ActionsSource {
	return &ActionsSource{
		catalog: catalog,
	}
}

func (s *ActionsSource) List(ctx context.Context, record graveler.HookRecord) ([]string, error) {
	const amount = 1000
	var after string
	hasMore := true
	var names []string
	for hasMore {
		var res []*DBEntry
		var err error
		res, hasMore, err = s.catalog.ListEntries(ctx, record.RepositoryID.String(), record.SourceRef.String(), repositoryLocation, after, DefaultPathDelimiter, amount)
		if err != nil {
			return nil, fmt.Errorf("listing actions: %w", err)
		}
		for _, result := range res {
			names = append(names, result.Path)
		}
	}
	return names, nil
}

func (s *ActionsSource) Load(ctx context.Context, record graveler.HookRecord, name string) ([]byte, error) {
	// get name's address
	repositoryID := record.RepositoryID
	ent, err := s.catalog.GetEntry(ctx, repositoryID.String(), record.SourceRef.String(), name, GetEntryParams{})
	if err != nil {
		return nil, fmt.Errorf("get action file metadata %s: %w", name, err)
	}
	// get repo storage namespace
	repo, err := s.catalog.GetRepository(ctx, repositoryID.String())
	if err != nil {
		return nil, fmt.Errorf("get repository %s: %w", repositoryID, err)
	}
	// get action address
	blockAdapter := s.catalog.BlockAdapter
	reader, err := blockAdapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       ent.PhysicalAddress,
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
