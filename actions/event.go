package actions

import (
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventTypePreCommit EventType = "pre-commit"
	EventTypePreMerge  EventType = "pre-merge"
)

type Event struct {
	EventID       uuid.UUID
	Source        Source
	Output        OutputWriter
	EventType     EventType
	EventTime     time.Time
	RepositoryID  string
	BranchID      string
	SourceRef     string
	CommitMessage string
	Committer     string
	Metadata      map[string]string
}
