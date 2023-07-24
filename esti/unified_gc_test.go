package esti

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const RepoName = "repo1"

type objectEvent struct {
	key           string
	branch        string
	commitDaysAgo int // if set to -1, do not commit
}

func gcTestCreateObject(t *testing.T, ctx context.Context, branch string, key string) string {
	t.Helper()
	_, _ = uploadFileRandomData(ctx, t, RepoName, branch, key, false)
	res, err := client.StatObjectWithResponse(ctx, RepoName, branch, &api.StatObjectParams{
		Path:    key,
		Presign: swag.Bool(true),
	})
	testutil.MustDo(t, fmt.Sprintf("Stats object %s after upload", key), err)
	require.Falsef(t, res.StatusCode() != 200, "Unexpected status code %d in stats object after upload", res.StatusCode())
	return res.JSON200.PhysicalAddress
}
func gcTestDeleteObject(t *testing.T, ctx context.Context, branch string, key string) {
	t.Helper()
	res, err := client.DeleteObjectWithResponse(ctx, RepoName, branch, &api.DeleteObjectParams{Path: key})
	testutil.MustDo(t, fmt.Sprintf("Delete %s", key), err)
	require.Falsef(t, res.StatusCode() > 299, "Unexpected status code %d in delete object", res.StatusCode())
}

func gcTestCommit(t *testing.T, ctx context.Context, branch string, daysAgo int) {
	t.Helper()
	commitTimeSeconds := time.Now().AddDate(0, 0, -daysAgo).Unix()
	res, err := client.CommitWithResponse(ctx, RepoName, branch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "commit event", Date: &commitTimeSeconds})
	testutil.MustDo(t, fmt.Sprintf("Commit branch %s", branch), err)
	require.Falsef(t, res.StatusCode() > 299, "Unexpected status code %d in commit", res.StatusCode())
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
		presignedURLs[e.key] = gcTestCreateObject(t, ctx, e.branch, e.key)
		gcTestCommit(t, ctx, e.branch, 14) // creations are always committed 14 days ago for this test
	}
	for _, e := range committedDeleteEvents {
		gcTestDeleteObject(t, ctx, e.branch, e.key)
		gcTestCommit(t, ctx, e.branch, e.commitDaysAgo)
	}
	for _, e := range uncommittedCreateEvents {
		presignedURLs[e.key] = gcTestCreateObject(t, ctx, e.branch, e.key)
	}
	for _, e := range uncommittedDeleteEvents {
		gcTestDeleteObject(t, ctx, e.branch, e.key)
	}
	deleteRes, err := client.DeleteBranchWithResponse(ctx, RepoName, "dev2")
	testutil.MustDo(t, "Delete dev2 branch", err)
	require.Falsef(t, deleteRes.StatusCode() > 299, "Unexpected status code %d in delete branch dev2", deleteRes.StatusCode())
	revertRes, err := client.ResetBranchWithResponse(ctx, RepoName, "dev", api.ResetBranchJSONRequestBody{Type: "reset"})
	require.Falsef(t, revertRes.StatusCode() > 299, "Unexpected status code %d in revert branch dev", revertRes.StatusCode())
	testutil.MustDo(t, "Revert changes in dev branch", err)
	err = runSparkSubmit(&sparkSubmitConfig{
		sparkVersion:    sparkImageTag,
		localJar:        metaClientJarPath,
		entryPoint:      "io.treeverse.gc.GarbageCollection",
		programArgs:     []string{RepoName, "us-east-1"},
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		logSource:       fmt.Sprintf("gc-%s", RepoName),
	})
	testutil.MustDo(t, "Run GC job", err)
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
		testutil.MustDo(t, "Http request to presigned url", err)
		if r.StatusCode > 299 && r.StatusCode != 404 {
			t.Fatalf("Unexpected status code in http request: %d", r.StatusCode)
		}
		if r.StatusCode >= 200 && r.StatusCode <= 299 && !expected {
			t.Fatalf("Didn't expect %s to exist, but it did", file)
		}
		if r.StatusCode == 404 && expected {
			t.Fatalf("Expected %s to exist, but it didn't", file)
		}
	}
}

func prepareForUnifiedGC(t *testing.T, ctx context.Context) {
	repo := createRepositoryByName(ctx, t, RepoName)
	createBranchRes, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: "dev", Source: mainBranch})
	testutil.MustDo(t, "Create branch dev", err)
	require.False(t, createBranchRes.StatusCode() > 299, "Create branch dev")
	createBranchRes, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: "dev2", Source: mainBranch})
	testutil.MustDo(t, "Create branch dev2", err)
	require.False(t, createBranchRes.StatusCode() > 299, "Create branch dev2")
	setGCRulesRes, err := client.SetGarbageCollectionRulesWithResponse(ctx, repo, api.SetGarbageCollectionRulesJSONRequestBody{Branches: []api.GarbageCollectionRule{
		{BranchId: "main", RetentionDays: 10}, {BranchId: "dev", RetentionDays: 7}, {BranchId: "dev", RetentionDays: 7},
	}, DefaultRetentionDays: 7})
	testutil.MustDo(t, "Set gc rules", err)
	require.False(t, setGCRulesRes.StatusCode() > 299, "Set gc rules")
}
