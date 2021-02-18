package catalog

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path"

	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/graveler"
)

type actionsSource struct {
	catalog          *EntryCatalog
	repositoryID     graveler.RepositoryID
	storageNamespace graveler.StorageNamespace
	ref              graveler.Ref
}

const (
	actionsRepositoryPrefix = "_lakefs_actions/"
	actionsStorageNamespace = "_lakefs/actions/log"
)

func (as *actionsSource) List(ctx context.Context) ([]actions.FileRef, error) {
	it, err := as.catalog.ListEntries(ctx, as.repositoryID, as.ref, actionsRepositoryPrefix, DefaultPathDelimiter)
	if err != nil {
		return nil, fmt.Errorf("listing actions: %w", err)
	}
	defer it.Close()

	var addresses []actions.FileRef
	for it.Next() {
		addresses = append(addresses, actions.FileRef{
			Path:    it.Value().Path.String(),
			Address: it.Value().Entry.Address,
		})
	}
	if it.Err() != nil {
		return nil, fmt.Errorf("entries iterator: %w", it.Err())
	}

	return addresses, nil
}

func (as *actionsSource) Load(ctx context.Context, fileRef actions.FileRef) ([]byte, error) {
	blockAdapter := as.catalog.BlockAdapter
	reader, err := blockAdapter.
		WithContext(ctx).
		Get(block.ObjectPointer{
			StorageNamespace: as.storageNamespace.String(),
			Identifier:       fileRef.Address,
		}, 0)
	if err != nil {
		return nil, fmt.Errorf("getting action file %s: %w", fileRef.Path, err)
	}
	defer func() {
		_ = reader.Close()
	}()

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading action file: %w", err)
	}
	return bytes, nil
}

type actionsWriter struct {
	adapter          block.Adapter
	storageNamespace graveler.StorageNamespace
}

//
func (aw *actionsWriter) OutputWrite(ctx context.Context, outputPath string, reader io.Reader, size int64) error {
	storageNamespace := aw.storageNamespace.String()
	identifier := path.Join(actionsStorageNamespace, outputPath)
	return aw.adapter.WithContext(ctx).Put(block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       identifier,
	}, size, reader, block.PutOpts{})
}
