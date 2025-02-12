package actions

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type OutputWriter interface {
	OutputWrite(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error
}
