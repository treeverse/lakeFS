package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DiffUncommitted_Changes(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// commit files
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file"+strconv.Itoa(i), nil, "")
	}
	_, err := c.Commit(ctx, repository, "master", "commit to master", "tester", nil)
	testutil.MustDo(t, "commit to master", err)

	// delete, create and change
	const newFilename = "/file5"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", newFilename, nil, "seed1")
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", delFilename))
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", overFilename, nil, "seed1")

	// verify that diff uncommitted show the above change
	differences, err := c.DiffUncommitted(ctx, repository, "master")
	if err != nil {
		t.Fatalf("DiffUncommitted err = %s, expected none", err)
	}

	changes := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	}
	if !changes.Equal(differences) {
		t.Fatalf("DiffUncommitted differences = %s, expected = %s", spew.Sdump(differences), spew.Sdump(changes))
	}
}

func TestCataloger_DiffUncommitted_NoChance(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// commit files
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file"+strconv.Itoa(i), nil, "")
	}
	_, err := c.Commit(ctx, repository, "master", "commit to master", "tester", nil)
	testutil.MustDo(t, "commit to master", err)

	// verify that diff uncommitted show the above change
	differences, err := c.DiffUncommitted(ctx, repository, "master")
	if err != nil {
		t.Fatalf("DiffUncommitted err = %s, expected none", err)
	}

	changes := Differences{}
	if !changes.Equal(differences) {
		t.Fatalf("DiffUncommitted differences = %s, expected = %s", spew.Sdump(differences), spew.Sdump(changes))
	}
}
