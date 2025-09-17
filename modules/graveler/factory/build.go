package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictsResolver(ctx context.Context, strategy graveler.MergeStrategy) graveler.ConflictsResolver {
	return nil
}
