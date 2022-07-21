package ref

import (
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type commitRecord struct {
	Version      graveler.CommitVersion `db:"version"`
	CommitID     string                 `db:"id"`
	Committer    string                 `db:"committer"`
	Message      string                 `db:"message"`
	RangeID      string                 `db:"meta_range_id"`
	CreationDate time.Time              `db:"creation_date"`
	Parents      []string               `db:"parents"`
	Metadata     map[string]string      `db:"metadata"`
	Generation   int                    `db:"generation"`
}

func (c *commitRecord) toGravelerCommit() *graveler.Commit {
	parents := make([]graveler.CommitID, len(c.Parents))
	for i := range c.Parents {
		parents[i] = graveler.CommitID(c.Parents[i])
	}
	return &graveler.Commit{
		Version:      c.Version,
		Committer:    c.Committer,
		Message:      c.Message,
		MetaRangeID:  graveler.MetaRangeID(c.RangeID),
		CreationDate: c.CreationDate,
		Parents:      parents,
		Metadata:     c.Metadata,
		Generation:   c.Generation,
	}
}

func (c *commitRecord) toGravelerCommitRecord() *graveler.CommitRecord {
	return &graveler.CommitRecord{
		CommitID: graveler.CommitID(c.CommitID),
		Commit:   c.toGravelerCommit(),
	}
}

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
