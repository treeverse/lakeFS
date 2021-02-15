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
	EventID          uuid.UUID
	EventType        EventType
	EventTime        time.Time
	RepositoryID     string
	StorageNamespace string
	BranchID         string
	SourceRef        string
	CommitMessage    string
	Committer        string
	Metadata         map[string]string
}
