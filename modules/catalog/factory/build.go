package factory

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictsResolver(objectReader graveler.ObjectReader) graveler.ConflictsResolver {
	return nil
}
