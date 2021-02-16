package actions

import (
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventTypePreCommit  EventType = "pre-commit"
	EventTypePostCommit EventType = "post-commit"
	EventTypePreMerge   EventType = "pre-merge"
	EventTypePostMerge  EventType = "post-merge"
)

type Deps struct {
	Source Source
	Output OutputWriter
}

type Event struct {
	RunID         uuid.UUID
	EventType     EventType
	EventTime     time.Time
	RepositoryID  string
	BranchID      string
	SourceRef     string
	CommitMessage string
	Committer     string
	Metadata      map[string]string
	CommitID      string
}
