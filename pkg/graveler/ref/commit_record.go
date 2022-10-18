package ref

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

func CommitDataToCommitRecord(c *graveler.CommitData) *graveler.CommitRecord {
	var parents []graveler.CommitID
	for _, parent := range c.Parents {
		parents = append(parents, graveler.CommitID(parent))
	}

	return &graveler.CommitRecord{
		CommitID: graveler.CommitID(c.Id),
		Commit: &graveler.Commit{
			Committer:    c.Committer,
			Message:      c.Message,
			CreationDate: c.CreationDate.AsTime(),
			MetaRangeID:  graveler.MetaRangeID(c.MetaRangeId),
			Metadata:     c.Metadata,
			Parents:      parents,
			Version:      graveler.CommitVersion(c.Version),
			Generation:   int(c.Generation),
		},
	}
}
