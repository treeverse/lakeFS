package catalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Commit(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	meta := Metadata{"key1": "val1", "key2": "val2"}
	for i := 0; i < 3; i++ {
		fileName := "/file" + strconv.Itoa(i)
		fileAddr := "/addr" + strconv.Itoa(i)
		if err := c.CreateEntry(ctx, repository, "master", fileName, "ff", fileAddr, i+1, meta); err != nil {
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
		want    int
		wantErr bool
	}{
		{
			name:    "simple",
			args:    args{repository: repository, branch: "master", message: "merge to master", committer: "tester", metadata: meta},
			want:    1,
			wantErr: false,
		},
		{
			name:    "no repository",
			args:    args{repository: "repoX", branch: "master", message: "merge to master", committer: "tester", metadata: meta},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no branch",
			args:    args{repository: repository, branch: "shifu", message: "merge to shifu", committer: "tester", metadata: meta},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no message",
			args:    args{repository: repository, branch: "master", message: "", committer: "tester", metadata: meta},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no committer",
			args:    args{repository: repository, branch: "master", message: "merge to master", committer: "", metadata: meta},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Commit(ctx, tt.args.repository, tt.args.branch, tt.args.message, tt.args.committer, tt.args.metadata)
			if (err != nil) != tt.wantErr {
				t.Errorf("Commit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Commit() got = %v, want %v", got, tt.want)
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
		for i := 0; i < 3; i++ {
			if err := c.CreateEntry(ctx, repository, "master", "/file1", strings.Repeat("ff", i), "/addr"+strconv.Itoa(i+1), i+1, nil); err != nil {
				t.Error("create entry for commit twice", err)
				return
			}
			commitID, err := c.Commit(ctx, repository, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}
			if commitID != i+1 {
				t.Errorf("Commit got ID %d, expected %d", commitID, i+1)
				return
			}
			ent, _, err := c.ListEntries(ctx, repository, "master", "", "", -1, false)
			if err != nil {
				t.Errorf("List committed data failed on iterations %d: %s", i+1, err)
				return
			}
			if len(ent) != 1 {
				t.Error("List committed data should list one element")
				return
			}
			if ent[0].Size != int64(i+1) {
				t.Errorf("Commited file size %d, expected %d", ent[0].Size, i+1)
			}
		}
	})

	t.Run("file per commit", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		for i := 0; i < 3; i++ {
			fileName := fmt.Sprintf("/file%d", i+1)
			addrName := fmt.Sprintf("/addr%d", i+1)
			if err := c.CreateEntry(ctx, repository, "master", fileName, "ff", addrName, 42, nil); err != nil {
				t.Error("create entry for file per commit", err)
				return
			}
			commitID, err := c.Commit(ctx, repository, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}
			if commitID != i+1 {
				t.Errorf("Commit got ID %d, expected %d", commitID, i+1)
				return
			}
			ent, _, err := c.ListEntries(ctx, repository, "master", "", "", -1, false)
			if err != nil {
				t.Errorf("List committed data failed on iterations %d: %s", i+1, err)
				return
			}
			if len(ent) != i+1 {
				t.Errorf("List commited files got %d entries, expected %d", len(ent), i+1)
			}
		}
	})

	t.Run("delete on a committed file same branch", func(t *testing.T) {
		repository := testCatalogerRepo(t, ctx, c, "repository", "master")
		if err := c.CreateEntry(ctx, repository, "master", "/file5", "ffff", "/addr5", 55, nil); err != nil {
			t.Fatal("create entry for file per commit", err)
			return
		}
		_, err := c.Commit(ctx, repository, "master", "commit one file", "tester", nil)
		if err != nil {
			t.Fatal("Commit expected to succeed error:", err)
		}
		// make sure we see one file
		entries, _, err := c.ListEntries(ctx, repository, "master", "", "", -1, true)
		testutil.Must(t, err)
		if len(entries) != 1 {
			t.Fatalf("List should find 1 file, got %d", len(entries))
		}

		err = c.DeleteEntry(ctx, repository, "master", "/file5")
		if err != nil {
			t.Fatal("Delete expected to succeed, got err", err)
		}
		// make sure we see no file uncommitted
		entries, _, err = c.ListEntries(ctx, repository, "master", "", "", -1, true)
		testutil.Must(t, err)
		if len(entries) != 0 {
			t.Fatalf("List should find no files, got %d", len(entries))
		}
		// make sure we see one file committed
		entries, _, err = c.ListEntries(ctx, repository, "master", "", "", -1, false)
		testutil.Must(t, err)
		if len(entries) != 1 {
			t.Fatalf("List should find 1 file, got %d", len(entries))
		}
		_, err = c.Commit(ctx, repository, "master", "delete one file", "tester", nil)
		if err != nil {
			t.Fatal("Commit expected to succeed error:", err)
		}
		// make sure we don't see the file after we commit the change
		entries, _, err = c.ListEntries(ctx, repository, "master", "", "", -1, false)
		testutil.Must(t, err)
		if len(entries) != 0 {
			t.Errorf("Delete should left no entries, got %d", len(entries))
		}
	})

}
