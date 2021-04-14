package ref

import (
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type commitRecord struct {
	Version      string            `db:"version"`
	CommitID     string            `db:"id"`
	Committer    string            `db:"committer"`
	Message      string            `db:"message"`
	RangeID      string            `db:"meta_range_id"`
	CreationDate time.Time         `db:"creation_date"`
	Parents      []string          `db:"parents"`
	Metadata     map[string]string `db:"metadata"`
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
	}
}

func (c *commitRecord) toGravelerCommitRecord() *graveler.CommitRecord {
	return &graveler.CommitRecord{
		CommitID: graveler.CommitID(c.CommitID),
		Commit:   c.toGravelerCommit(),
	}
}
