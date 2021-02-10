package catalog

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/block"
)

type actionsSource struct {
	catalog *EntryCatalog
	adapter block.Adapter

	repositoryID graveler.RepositoryID
	repository   graveler.Repository
	ref          graveler.Ref
}

const actionsPrefix = "_lakefs_actions/"

func (as *actionsSource) List(ctx context.Context) ([]string, error) {
	it, err := as.catalog.ListEntries(ctx, as.repositoryID, as.ref, actionsPrefix, DefaultPathDelimiter)
	if err != nil {
		return nil, fmt.Errorf("listing actions: %w", err)
	}
	defer it.Close()

	var addresses []string
	for it.Next() {
		addresses = append(addresses, it.Value().Entry.Address)
	}
	if it.Err() != nil {
		return nil, fmt.Errorf("entries iterator: %w", it.Err())
	}

	return addresses, nil
}

func (as *actionsSource) Load(address string) ([]byte, error) {
	reader, err := as.adapter.Get(block.ObjectPointer{
		StorageNamespace: as.repository.StorageNamespace.String(),
		Identifier:       address,
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("getting action file: %w", err)
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading action file: %w", err)
	}
	return bytes, nil
}
