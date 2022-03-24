package actions

import (
	"encoding/json"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type EventInfo struct {
	EventType      string            `json:"event_type"`
	EventTime      string            `json:"event_time"`
	ActionName     string            `json:"action_name"`
	HookID         string            `json:"hook_id"`
	RepositoryID   string            `json:"repository_id"`
	BranchID       string            `json:"branch_id"`
	SourceRef      string            `json:"source_ref,omitempty"`
	TagID          string            `json:"tag_id,omitempty"`
	CommitID       string            `json:"commit_id,omitempty"`
	CommitMessage  string            `json:"commit_message"`
	Committer      string            `json:"committer"`
	CommitMetadata map[string]string `json:"commit_metadata,omitempty"`
}

func marshalEventInformation(actionName, hookID string, record graveler.HookRecord) ([]byte, error) {
	now := time.Now()
	info := EventInfo{
		EventType:      string(record.EventType),
		EventTime:      now.UTC().Format(time.RFC3339),
		ActionName:     actionName,
		HookID:         hookID,
		RepositoryID:   record.RepositoryID.String(),
		BranchID:       record.BranchID.String(),
		SourceRef:      record.SourceRef.String(),
		TagID:          record.TagID.String(),
		CommitID:       record.CommitID.String(),
		CommitMessage:  record.Commit.Message,
		Committer:      record.Commit.Committer,
		CommitMetadata: record.Commit.Metadata,
	}
	return json.Marshal(info)
}
