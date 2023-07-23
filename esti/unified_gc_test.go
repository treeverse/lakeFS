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

type objectEvent struct {
	key           string
	branch        string
	commitDaysAgo int // if set to -1, do not commit
}

func doObjectEvent(t *testing.T, ctx context.Context, repo string, e objectEvent, isCreate bool) string {
	var presignedURL string
	if isCreate {
		_, _ = uploadFileRandomData(ctx, t, repo, e.branch, e.key, false)
		res, err := client.StatObjectWithResponse(ctx, repo, e.branch, &api.StatObjectParams{
			Path:    e.key,
			Presign: swag.Bool(true),
		})
		if err != nil {
			t.Fatalf("stats object after upload: %s", err)
		}
		presignedURL = res.JSON200.PhysicalAddress
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
	return presignedURL
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
		},
		{
			key:           "file_2",
			branch:        "main",
			commitDaysAgo: 14,
		},
		{
			key:           "file_3",
			branch:        "main",
			commitDaysAgo: -1,
		},
		{
			key:           "file_4",
			branch:        "main",
			commitDaysAgo: -1,
		},
		{
			key:           "file_5",
			branch:        "dev",
			commitDaysAgo: 14,
		},
		{
			key:           "file_6",
			branch:        "dev",
			commitDaysAgo: 14,
		},
		{
			key:           "file_7",
			branch:        "dev",
			commitDaysAgo: -1,
		},
		{
			key:           "file_8",
			branch:        "dev2",
			commitDaysAgo: 14,
		},
		{
			key:           "file_9",
			branch:        "dev2",
			commitDaysAgo: 14,
		},
		{
			key:           "file_10",
			branch:        "dev2",
			commitDaysAgo: -1,
		},
	}
	deleteEvents := []objectEvent{
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
		{
			key:           "file_4",
			branch:        "dev",
			commitDaysAgo: -1,
		},
	}
	presignedURLs := map[string]string{}
	for _, e := range createEvents {
		presignedURL := doObjectEvent(t, ctx, "repo1", e, true)
		presignedURLs[e.key] = presignedURL
	}
	for _, e := range deleteEvents {
		doObjectEvent(t, ctx, "repo1", e, false)
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
		sparkVersion:    sparkImageTag,
		localJar:        metaClientJarPath,
		entryPoint:      "io.treeverse.gc.GarbageCollection",
		programArgs:     []string{"repo1", "us-east-1"},
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		logSource:       fmt.Sprintf("gc-%s", "repo1"),
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
		if r.StatusCode > 200 && r.StatusCode <= 299 && !expected {
			t.Fatalf("didn't expect %s to exist, but it did", file)
		}
		if r.StatusCode == 404 && expected {
			t.Fatalf("expected %s to exist, but it didn't", file)
		}
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
}
