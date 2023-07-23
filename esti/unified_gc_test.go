package esti

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/api"
)

type objectEvent struct {
	key           string
	branch        string
	commitDaysAgo int // if set to -1, do not commit
	eventType     eventType
}

type eventType int

const (
	eventTypeCreate eventType = iota
	eventTypeDelete
)

func doObjectEvent(t *testing.T, ctx context.Context, repo string, e objectEvent) {
	if e.eventType == eventTypeCreate {
		_, _ = uploadFileRandomData(ctx, t, repo, e.branch, e.key, false)

	} else {
		_, err := client.DeleteObjectWithResponse(ctx, repo, e.branch, &api.DeleteObjectParams{Path: e.key})
		if err != nil {
			t.Fatalf("delete file %s: %s", e.key, err)
		}
	}
	if e.commitDaysAgo > -1 {
		commitTimeSeconds := time.Now().AddDate(0, 0, -e.commitDaysAgo).Unix()
		_, err := client.CommitWithResponse(ctx, repo, e.branch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "commit event", Date: &commitTimeSeconds})
		if err != nil {
			t.Fatalf("Commit event: %s", err)
		}
	}

}
func TestUnifiedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	ctx := context.Background()
	prepareForUnifiedGC(t, ctx)
	createEvents := []objectEvent{
		{
			key:           "file_1",
			branch:        "main",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_2",
			branch:        "main",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_3",
			branch:        "main",
			commitDaysAgo: -1,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_4",
			branch:        "main",
			commitDaysAgo: -1,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_5",
			branch:        "dev",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_6",
			branch:        "dev",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_7",
			branch:        "dev",
			commitDaysAgo: -1,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_8",
			branch:        "dev2",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_9",
			branch:        "dev2",
			commitDaysAgo: 14,
			eventType:     eventTypeCreate,
		},
		{
			key:           "file_10",
			branch:        "dev2",
			commitDaysAgo: -1,
			eventType:     eventTypeCreate,
		},
	}
	deleteEvents := []objectEvent{
		{
			key:           "file_2",
			branch:        "main",
			commitDaysAgo: 12,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_1",
			branch:        "main",
			commitDaysAgo: 8,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_5",
			branch:        "dev",
			commitDaysAgo: 8,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_9",
			branch:        "dev2",
			commitDaysAgo: 8,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_8",
			branch:        "dev2",
			commitDaysAgo: 6,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_6",
			branch:        "dev",
			commitDaysAgo: 4,
			eventType:     eventTypeDelete,
		},
		{
			key:           "file_4",
			branch:        "dev",
			commitDaysAgo: -1,
			eventType:     eventTypeDelete,
		},
	}

	for _, e := range createEvents {
		doObjectEvent(t, ctx, "repo1", e)
	}
	for _, e := range deleteEvents {
		doObjectEvent(t, ctx, "repo1", e)
	}

	_, err := client.DeleteBranchWithResponse(ctx, "repo1", "dev2")
	if err != nil {
		t.Fatalf("delete dev2 branch: %s", err)
	}
	_, err = client.ResetBranchWithResponse(ctx, "repo1", "dev", api.ResetBranchJSONRequestBody{})
	if err != nil {
		t.Fatalf("reset dev branch: %s", err)
	}
	err = runSparkSubmit(&sparkSubmitConfig{
		sparkVersion: sparkImageTag,
		localJar:     metaClientJarPath,
		entryPoint:   "io.treeverse.gc.GarbageCollection",
		programArgs:  []string{"repo1", "us-east-1"},
		logSource:    fmt.Sprintf("gc-%s", "repo1"),
	})
	if err != nil {
		t.Fatalf("run gc job: %s", err)
	}

}

func prepareForUnifiedGC(t *testing.T, ctx context.Context) {
	repo := createRepositoryByName(ctx, t, "repo1")
	_, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: "dev", Source: mainBranch})
	if err != nil {
		t.Fatalf("Create new branch %s", err)
	}
	_, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: "dev2", Source: mainBranch})
	if err != nil {
		t.Fatalf("Create new branch %s", err)
	}

	_, err = client.SetGarbageCollectionRulesWithResponse(ctx, repo, api.SetGarbageCollectionRulesJSONRequestBody{Branches: []api.GarbageCollectionRule{
		{BranchId: "main", RetentionDays: 10}, {BranchId: "dev", RetentionDays: 7}, {BranchId: "dev", RetentionDays: 7},
	}, DefaultRetentionDays: 7})
	if err != nil {
		t.Fatalf("Set GC rules %s", err)
	}

	res, _ := client.GetObjectWithResponse(ctx, repo, "main", &api.GetObjectParams{Path: "file_1"})
	fileExists := res.StatusCode() == 200
	if !fileExists {
		t.Fatalf("expected file_1 but it doesn't exist")
	}
	res, _ = client.GetObjectWithResponse(ctx, repo, "main", &api.GetObjectParams{Path: "file_2"})
	fileExists = res.StatusCode() == 200
	if fileExists {
		t.Fatalf("file_2 exists but not expected")
	}
}
