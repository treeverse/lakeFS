package actions

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventTypePreCommit EventType = "pre-commit"
	EventTypePreMerge  EventType = "pre-merge"
)

type Event struct {
	RunID         string
	EventType     EventType
	EventTime     time.Time
	RepositoryID  string
	BranchID      string
	SourceRef     string
	CommitMessage string
	Committer     string
	Metadata      map[string]string
}

func NewEvent(eventType EventType) Event {
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	now := time.Now().UTC()
	runID := now.Format(time.RFC3339) + "_" + uid
	return Event{
		RunID:     runID,
		EventType: eventType,
		EventTime: now,
	}
}
