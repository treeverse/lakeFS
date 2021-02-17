package actions

import (
	"time"

	nanoid "github.com/matoous/go-nanoid"
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
	Type          EventType
	Time          time.Time
	RepositoryID  string
	BranchID      string
	SourceRef     string
	CommitMessage string
	Committer     string
	Metadata      map[string]string
	CommitID      string
}

func NewRunID() string {
	const nanoLen = 6
	id := nanoid.MustID(nanoLen)
	tm := time.Now().UTC().Format("20060102150405")
	return tm + id
}
