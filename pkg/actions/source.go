package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type Source interface {
	List(ctx context.Context, record graveler.HookRecord) ([]string, error)
	Load(ctx context.Context, record graveler.HookRecord, name string) ([]byte, error)
}
