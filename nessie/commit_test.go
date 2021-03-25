package nessie

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/gen/client/commits"
	"github.com/treeverse/lakefs/pkg/api/gen/client/objects"
	"github.com/treeverse/lakefs/pkg/api/gen/models"
)

func TestCommitSingle(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, objContent := uploadFileRandomData(ctx, t, repo, masterBranch, objPath)
	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:singleCommit"),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	var b bytes.Buffer
	_, err = client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).WithRepository(repo).WithRef(masterBranch).WithPath(objPath), nil, &b)
	require.NoError(t, err, "failed to get object")

	require.Equal(t, objContent, b.String(), fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, b.String()))
}

// genNames generates n consecutive filenames starting with prefix.
func genNames(n int, prefix string) []string {
	ret := make([]string, n)
	for i := range ret {
		ret[i] = fmt.Sprint(prefix, i+1)
	}
	return ret
}

type Upload struct {
	Repo, Branch, Path string
}

// upload uploads random file data for uploads.
func upload(ctx context.Context, uploads chan Upload) error {
	for u := range uploads {
		_, _, err := uploadFileRandomDataAndReport(ctx, u.Repo, u.Branch, u.Path)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestCommitInMixedOrder(t *testing.T) {
	const (
		parallelism = 5
		size        = 100
	)
	ctx, _, repo := setupTest(t)

	names1 := genNames(size, "run2/foo")
	uploads := make(chan Upload, size)
	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			if err := upload(ctx, uploads); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	for _, name := range names1 {
		uploads <- Upload{Repo: repo, Branch: masterBranch, Path: name}
	}
	close(uploads)
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:mixedOrderCommit1"),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	names2 := genNames(size, "run1/foo")
	uploads = make(chan Upload, size)
	wg = sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			if err := upload(ctx, uploads); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	for _, name := range names2 {
		uploads <- Upload{Repo: repo, Branch: masterBranch, Path: name}
	}
	close(uploads)
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:mixedOrderCommit2"),
	}), nil)
	require.NoError(t, err, "failed to commit second set of changes")
}
