package catalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/treeverse/lakefs/testutil"
)

func timeDifference(a, b time.Time) time.Duration {
	diff := a.Sub(b)
	if diff < time.Duration(0) {
		return -diff
	}
	return diff
}

func TestCataloger_Commit(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	meta := Metadata{"key1": "val1", "key2": "val2"}
	for i := 0; i < 3; i++ {
		fileName := "/file" + strconv.Itoa(i)
		fileAddr := "/addr" + strconv.Itoa(i)
		if err := c.CreateEntry(ctx, repository, "master", Entry{
			Path:            fileName,
			Checksum:        "ff",
			PhysicalAddress: fileAddr,
			Size:            int64(i) + 1,
			Metadata:        meta,
			CreationDate:    time.Now(),
		}, CreateEntryParams{}); err != nil {
			t.Fatal("create entry for testing", fileName, err)
		}
	}

	type args struct {
		repository string
		branch     string
		message    string
		committer  string
		metadata   map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    *CommitLog
		wantErr bool
	}{
		{
			name: "simple",
			args: args{repository: repository, branch: "master", message: "Simple commit", committer: "tester", metadata: meta},
			want: &CommitLog{
				Reference:    "~KJ8Wd1Rs96Z",
				Committer:    "tester",
				Message:      "Simple commit",
				CreationDate: time.Now(),
				Metadata:     meta,
				Parents:      []string{"~KJ8Wd1Rs96Y"},
			},
			wantErr: false,
		},
		{
			name:    "no repository",
			args:    args{repository: "repoX", branch: "master", message: "commit message", committer: "tester", metadata: meta},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no branch",
			args:    args{repository: repository, branch: "shifu", message: "commit message", committer: "tester", metadata: meta},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no message",
			args:    args{repository: repository, branch: "master", message: "", committer: "tester", metadata: meta},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no committer",
			args:    args{repository: repository, branch: "master", message: "commit message", committer: "", metadata: meta},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			got, err := c.Commit(ctx, tt.args.repository, tt.args.branch, tt.args.message, tt.args.committer, tt.args.metadata)
			if (err != nil) != tt.wantErr {
				t.Errorf("Commit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				if timeDifference(got.CreationDate, now) > 10*time.Second {
					t.Errorf("expected creation time %s, got very different %s", got.CreationDate, now)
				}
				if tt.want != nil {
					got.CreationDate = tt.want.CreationDate
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Commit() got = %s, want = %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}

func TestCataloger_Commit_Scenario(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	t.Run("nothing", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		_, err := c.Commit(ctx, repository, "master", "in a bottle", "tester1", nil)
		if !errors.Is(err, ErrNothingToCommit) {
			t.Fatal("Expect nothing to commit error, got", err)
		}
	})

	t.Run("same file more than once", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		var previousCommitID CommitID
		for i := 0; i < 3; i++ {
			if err := c.CreateEntry(ctx, repository, "master", Entry{
				Path:            "/file1",
				Checksum:        strings.Repeat("ff", i),
				PhysicalAddress: "/addr" + strconv.Itoa(i+1),
				Size:            int64(i) + 1,
			}, CreateEntryParams{}); err != nil {
				t.Error("create entry for commit twice", err)
				return
			}
			commitLog, err := c.Commit(ctx, repository, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}

			// parse commit log and check that the commit id goes up
			r, err := ParseRef(commitLog.Reference)
			testutil.Must(t, err)
			if r.CommitID <= previousCommitID {
				t.Fatalf("Commit ID should go up - %d, previous was %d", r.CommitID, previousCommitID)
			}
			previousCommitID = r.CommitID

			// verify that committed data is found
			ent, err := c.GetEntry(ctx, repository, "master:HEAD", "/file1", GetEntryParams{})
			testutil.MustDo(t, "Get entry we just committed", err)
			if ent.Size != int64(i+1) {
				t.Errorf("Committed file size %d, expected %d", ent.Size, i+1)
			}
		}
	})

	t.Run("file per commit", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		var previousCommitID CommitID
		for i := 0; i < 3; i++ {
			fileName := fmt.Sprintf("/file%d", i+1)
			addrName := fmt.Sprintf("/addr%d", i+1)
			if err := c.CreateEntry(ctx, repository, "master", Entry{
				Path:            fileName,
				Checksum:        "ff",
				PhysicalAddress: addrName,
				Size:            42,
			}, CreateEntryParams{}); err != nil {
				t.Error("create entry for file per commit", err)
				return
			}
			commitLog, err := c.Commit(ctx, repository, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}

			// check that commit id goes up
			ref, err := ParseRef(commitLog.Reference)
			testutil.Must(t, err)
			if ref.CommitID <= previousCommitID {
				t.Fatalf("Commit new commit ID %d, should go up - previous %d", ref.CommitID, previousCommitID)
			}

			ent, _, err := c.ListEntries(ctx, repository, "master", "", "", "", -1)
			if err != nil {
				t.Errorf("List committed data failed on iterations %d: %s", i+1, err)
				return
			}
			if len(ent) != i+1 {
				t.Errorf("List committed files got %d entries, expected %d", len(ent), i+1)
			}
		}
	})

	t.Run("delete on a committed file same branch", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		if err := c.CreateEntry(ctx, repository, "master", Entry{
			Path:            "/file5",
			Checksum:        "ffff",
			PhysicalAddress: "/addr5",
			Size:            55,
		}, CreateEntryParams{}); err != nil {
			t.Fatal("create entry for file per commit", err)
			return
		}
		_, err := c.Commit(ctx, repository, "master", "commit one file", "tester", nil)
		if err != nil {
			t.Fatal("Commit expected to succeed error:", err)
		}
		// make sure we see one file
		entries, _, err := c.ListEntries(ctx, repository, "master", "", "", "", -1)
		testutil.Must(t, err)
		if len(entries) != 1 {
			t.Fatalf("List should find 1 file, got %d", len(entries))
		}

		err = c.DeleteEntry(ctx, repository, "master", "/file5")
		if err != nil {
			t.Fatal("Delete expected to succeed, got err", err)
		}
		// make sure we see no file uncommitted
		entries, _, err = c.ListEntries(ctx, repository, "master", "", "", "", -1)
		testutil.Must(t, err)
		if len(entries) != 0 {
			t.Fatalf("List should find no files, got %d", len(entries))
		}
		// make sure we see one file committed
		entries, _, err = c.ListEntries(ctx, repository, "master:HEAD", "", "", "", -1)
		testutil.Must(t, err)
		if len(entries) != 1 {
			t.Fatalf("List should find 1 file, got %d", len(entries))
		}
		_, err = c.Commit(ctx, repository, "master", "delete one file", "tester", nil)
		if err != nil {
			t.Fatal("Commit expected to succeed error:", err)
		}
		// make sure we don't see the file after we commit the change
		entries, _, err = c.ListEntries(ctx, repository, "master:HEAD", "", "", "", -1)
		testutil.Must(t, err)
		if len(entries) != 0 {
			t.Errorf("Delete should left no entries, got %d", len(entries))
		}
	})

}
