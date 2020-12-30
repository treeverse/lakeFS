package ref

import (
	"time"

	"github.com/treeverse/lakefs/graveler"
)

type commitRecord struct {
	CommitID     string            `db:"id"`
	Committer    string            `db:"committer"`
	Message      string            `db:"message"`
	RangeID      string            `db:"range_id"`
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
		Committer:    c.Committer,
		Message:      c.Message,
		RangeID:      graveler.RangeID(c.RangeID),
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
