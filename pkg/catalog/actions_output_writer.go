package catalog

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type ActionsOutputWriter struct {
	adapter block.Adapter
}

func NewActionsOutputWriter(blockAdapter block.Adapter) *ActionsOutputWriter {
	return &ActionsOutputWriter{
		adapter: blockAdapter,
	}
}

func (o *ActionsOutputWriter) OutputWrite(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
	_, err := o.adapter.Put(ctx, block.ObjectPointer{
		StorageID:        repository.StorageID.String(),
		StorageNamespace: repository.StorageNamespace.String(),
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       name,
	}, size, reader, block.PutOpts{})
	return err
}
