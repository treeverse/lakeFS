package esti

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
)

const RepoName = "repo1"

type objectEvent struct {
	key           string
	branch        string
	commitDaysAgo int // if set to -1, do not commit
}

func createObject(t *testing.T, ctx context.Context, branch string, key string) string {
	_, _ = uploadFileRandomData(ctx, t, RepoName, branch, key, false)
	res, err := client.StatObjectWithResponse(ctx, RepoName, branch, &api.StatObjectParams{
		Path:    key,
		Presign: swag.Bool(true),
	})
	if err != nil {
		t.Fatalf("stats object after upload: %s", err)
	}
	return res.JSON200.PhysicalAddress
}

func commitBranch(t *testing.T, ctx context.Context, branch string, daysAgo int) {
	commitTimeSeconds := time.Now().AddDate(0, 0, -daysAgo).Unix()
	_, err := client.CommitWithResponse(ctx, RepoName, branch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "commit event", Date: &commitTimeSeconds})
	if err != nil {
		t.Fatalf("Commit event: %s", err)
	}
}

func TestUnifiedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	ctx := context.Background()
	prepareForUnifiedGC(t, ctx)
	committedCreateEvents := []objectEvent{
		{
			key:    "file_1",
			branch: "main",
		},
		{
			key:    "file_2",
			branch: "main",
		},
		{
			key:    "file_5",
			branch: "dev",
		},
		{
			key:    "file_6",
			branch: "dev",
		},
		{
			key:    "file_8",
			branch: "dev2",
		},
		{
			key:    "file_9",
			branch: "dev2",
		},
	}
	committedDeleteEvents := []objectEvent{
		{
			key:           "file_2",
			branch:        "main",
			commitDaysAgo: 12,
		},
		{
			key:           "file_1",
			branch:        "main",
			commitDaysAgo: 8,
		},
		{
			key:           "file_5",
			branch:        "dev",
			commitDaysAgo: 8,
		},
		{
			key:           "file_9",
			branch:        "dev2",
			commitDaysAgo: 8,
		},
		{
			key:           "file_8",
			branch:        "dev2",
			commitDaysAgo: 6,
		},
		{
			key:           "file_6",
			branch:        "dev",
			commitDaysAgo: 4,
		},
	}
	uncommittedCreateEvents := []objectEvent{
		{
			key:    "file_3",
			branch: "main",
		},
		{
			key:    "file_4",
			branch: "main",
		},
		{
			key:    "file_7",
			branch: "dev",
		},
		{
			key:    "file_10",
			branch: "dev2",
		},
	}
	uncommittedDeleteEvents := []objectEvent{
		{
			key:           "file_4",
			branch:        "main",
			commitDaysAgo: -1,
		},
	}
	presignedURLs := map[string]string{}
	for _, e := range committedCreateEvents {
		presignedURLs[e.key] = createObject(t, ctx, e.branch, e.key)
		commitBranch(t, ctx, e.branch, 14) // creations are always committed 14 days ago for this test
	}
	for _, e := range committedDeleteEvents {
		_, err := client.DeleteObjectWithResponse(ctx, RepoName, e.branch, &api.DeleteObjectParams{Path: e.key})
		if err != nil {
			t.Fatalf("delete file %s: %s", e.key, err)
		}
		commitBranch(t, ctx, e.branch, e.commitDaysAgo)
	}
	for _, e := range uncommittedCreateEvents {
		presignedURLs[e.key] = createObject(t, ctx, e.branch, e.key)
	}
	for _, e := range uncommittedDeleteEvents {
		_, err := client.DeleteObjectWithResponse(ctx, RepoName, e.branch, &api.DeleteObjectParams{Path: e.key})
		if err != nil {
			t.Fatalf("delete file %s: %s", e.key, err)
		}
	}

	_, err := client.DeleteBranchWithResponse(ctx, RepoName, "dev2")
	if err != nil {
		t.Fatalf("delete dev2 branch: %s", err)
	}
	_, err = client.ResetBranchWithResponse(ctx, RepoName, "dev", api.ResetBranchJSONRequestBody{Type: "reset"})
	if err != nil {
		t.Fatalf("reset dev branch: %s", err)
	}
	err = runSparkSubmit(&sparkSubmitConfig{
		sparkVersion:    sparkImageTag,
		localJar:        metaClientJarPath,
		entryPoint:      "io.treeverse.gc.GarbageCollection",
		programArgs:     []string{RepoName, "us-east-1"},
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		logSource:       fmt.Sprintf("gc-%s", RepoName),
	})
	if err != nil {
		t.Fatalf("run gc job: %s", err)
	}
	expectedExisting := map[string]bool{
		"file_1":  true,
		"file_2":  false,
		"file_3":  true,
		"file_4":  false,
		"file_5":  false,
		"file_6":  true,
		"file_7":  false,
		"file_8":  true,
		"file_9":  false,
		"file_10": false,
	}

	for file, expected := range expectedExisting {
		r, err := http.Get(presignedURLs[file])
		if err != nil {
			t.Fatalf("http request to presigned url: %s", err)
		}
		println(file)
		println(presignedURLs[file])
		println(r.StatusCode)
		if r.StatusCode > 299 && r.StatusCode != 404 {
			t.Fatalf("unexpected status code in http request: %d", r.StatusCode)
		}
		if r.StatusCode >= 200 && r.StatusCode <= 299 && !expected {
			t.Fatalf("didn't expect %s to exist, but it did", file)
		}
		if r.StatusCode == 404 && expected {
			t.Fatalf("expected %s to exist, but it didn't", file)
		}
	}
}

func prepareForUnifiedGC(t *testing.T, ctx context.Context) {
	repo := createRepositoryByName(ctx, t, RepoName)
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
}
