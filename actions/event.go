package actions

import (
	"context"
	"io"
	"time"
)

type EventType string

const (
	EventTypePreCommit EventType = "pre-commit"
	EventTypePreMerge  EventType = "pre-merge"
)

type Source interface {
	List() ([]string, error)
	Load(name string) ([]byte, error)
}

type OutputWriter interface {
	OutputWrite(ctx context.Context, name string, reader io.Reader) error
}

type Event struct {
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
