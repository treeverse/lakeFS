package catalog

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/block"
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
	return o.adapter.WithContext(ctx).Put(block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       name,
	}, size, reader, block.PutOpts{})
}
