package graveler

import "github.com/treeverse/lakefs/pkg/kv"

const (
	tagsPrefix = "tags"
)

func TagPath(tagID string) string {
	return kv.FormatPath(tagsPrefix, tagID)
}

func TagPartition(repoID string) string {
	return repoID
}
