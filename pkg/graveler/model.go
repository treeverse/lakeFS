package graveler

import "github.com/treeverse/lakefs/pkg/kv"

const (
	tagsPrefix = "tags"
)

func TagPath(tagID TagID) string {
	return kv.FormatPath(tagsPrefix, tagID.String())
}

func TagPartition(repoID RepositoryID) string {
	return repoID.String()
}
