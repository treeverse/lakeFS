package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DiffUncommitted_Pagination(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	const numOfFiles = 10
	var expectedDifferences Differences
	for i := 0; i < numOfFiles; i++ {
		p := "/file" + strconv.Itoa(i)
		testCatalogerCreateEntry(t, ctx, c, repository, "master", p, nil, "")
		expectedDifferences = append(expectedDifferences, Difference{Type: DifferenceTypeAdded, Entry: Entry{Path: p}})
	}
	const changesPerPage = 3
	var differences Differences
	var after string
	for {
		res, hasMore, err := c.DiffUncommitted(ctx, repository, "master", changesPerPage, after)
		testutil.MustDo(t, "diff uncommitted changes", err)
		if err != nil {
			t.Fatalf("DiffUncommitted err=%s, expected none", err)
		}
		if len(res) > changesPerPage {
			t.Fatalf("DiffUncommitted() result length %d, expected equal or less than %d", len(res), changesPerPage)
		}
		differences = append(differences, res...)
		if !hasMore {
			break
		}
		after = res[len(res)-1].Path
	}
	if diff := deep.Equal(differences, expectedDifferences); diff != nil {
		t.Fatal("DiffUncommitted", diff)
	}

	// check the case where we ask for 0 amount
	res, hasMore, err := c.DiffUncommitted(ctx, repository, "master", 0, "")
	testutil.MustDo(t, "diff uncommitted with 0 limit", err)
	if !hasMore {
		t.Error("DiffUncommitted() has more should be true")
	}
	if len(res) != 0 {
		t.Errorf("DiffUncommitted() has %d items in result when expected none", len(res))
	}

}

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
	differences, _, err := c.DiffUncommitted(ctx, repository, "master", -1, "")
	if err != nil {
		t.Fatalf("DiffUncommitted err = %s, expected none", err)
	}

	changes := Differences{
		Difference{Type: DifferenceTypeRemoved, Entry: Entry{Path: "/file1"}},
		Difference{Type: DifferenceTypeChanged, Entry: Entry{Path: "/file2"}},
		Difference{Type: DifferenceTypeAdded, Entry: Entry{Path: "/file5"}},
	}
	if diff := deep.Equal(differences, changes); diff != nil {
		t.Fatal("DiffUncommitted", diff)
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
	differences, hasMore, err := c.DiffUncommitted(ctx, repository, "master", -1, "")
	if err != nil {
		t.Fatalf("DiffUncommitted err = %s, expected none", err)
	}

	if len(differences) != 0 {
		t.Fatalf("DiffUncommitted differences len=%d, expected 0", len(differences))
	}
	if hasMore {
		t.Fatal("DiffUncommitted hadMore is true, expected false")
	}
}
