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
	EventType     EventType
	EventTime     time.Time
	RepositoryID  string
	BranchID      string
	SourceRef     string
	CommitMessage string
	Committer     string
	Metadata      map[string]string
}

func NewRunID(t time.Time) string {
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	runID := t.UTC().Format(time.RFC3339) + "_" + uid
	return runID
}
