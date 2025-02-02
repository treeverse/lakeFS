package catalog

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/pkg/block"
)

type ActionsOutputWriter struct {
	adapter block.Adapter
}

func NewActionsOutputWriter(blockAdapter block.Adapter) *ActionsOutputWriter {
	return &ActionsOutputWriter{
		adapter: blockAdapter,
	}
}

func (o *ActionsOutputWriter) OutputWrite(ctx context.Context, storageNamespace, name string, reader io.Reader, size int64) error {
	// TODO (gilo): ObjectPointer init - add StorageID here
	_, err := o.adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       name,
	}, size, reader, block.PutOpts{})
	return err
}
