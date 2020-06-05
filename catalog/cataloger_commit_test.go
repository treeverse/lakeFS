package catalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestCataloger_Commit(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)
	repo := setupCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 3; i++ {
		fileName := "/file" + strconv.Itoa(i)
		fileAddr := "/addr" + strconv.Itoa(i)
		if err := c.WriteEntry(ctx, repo, "master", fileName, "ff", fileAddr, i+1, nil); err != nil {
			t.Fatal("write entry for testing", fileName, err)
		}
	}

	type args struct {
		repo      string
		branch    string
		message   string
		committer string
		metadata  map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name:    "simple",
			args:    args{repo: repo, branch: "master", message: "merge to master", committer: "tester", metadata: nil},
			want:    1,
			wantErr: false,
		},
		{
			name:    "no repo",
			args:    args{repo: "repoX", branch: "master", message: "merge to master", committer: "tester", metadata: nil},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no branch",
			args:    args{repo: repo, branch: "shifu", message: "merge to shifu", committer: "tester", metadata: nil},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no message",
			args:    args{repo: repo, branch: "master", message: "", committer: "tester", metadata: nil},
			want:    0,
			wantErr: true,
		},
		{
			name:    "no committer",
			args:    args{repo: repo, branch: "master", message: "merge to master", committer: "", metadata: nil},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Commit(ctx, tt.args.repo, tt.args.branch, tt.args.message, tt.args.committer, tt.args.metadata)
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
	c := setupCatalogerForTesting(t)

	t.Run("nothing", func(t *testing.T) {
		repo := setupCatalogerRepo(t, ctx, c, "repo", "master")
		_, err := c.Commit(ctx, repo, "master", "in a bottle", "tester1", nil)
		if !errors.Is(err, ErrNothingToCommit) {
			t.Fatal("Expect nothing to commit error, got", err)
		}
	})

	t.Run("same file more than once", func(t *testing.T) {
		repo := setupCatalogerRepo(t, ctx, c, "repo", "master")
		for i := 0; i < 3; i++ {
			if err := c.WriteEntry(ctx, repo, "master", "/file1", strings.Repeat("ff", i), "/addr"+strconv.Itoa(i+1), i+1, nil); err != nil {
				t.Error("write entry for commit twice", err)
				return
			}
			commitID, err := c.Commit(ctx, repo, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}
			if commitID != i+1 {
				t.Errorf("Commit got ID %d, expected %d", commitID, i+1)
				return
			}
			ent, _, err := c.ListEntriesByPrefix(ctx, repo, "master", "", "", -1, false, false)
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
		repo := setupCatalogerRepo(t, ctx, c, "repo", "master")
		for i := 0; i < 3; i++ {
			fileName := fmt.Sprintf("/file%d", i+1)
			addrName := fmt.Sprintf("/addr%d", i+1)
			if err := c.WriteEntry(ctx, repo, "master", fileName, "ff", addrName, 42, nil); err != nil {
				t.Error("write entry for file per commit", err)
				return
			}
			commitID, err := c.Commit(ctx, repo, "master", "commit"+strconv.Itoa(i+1), "tester", nil)
			if err != nil {
				t.Errorf("Commit got error on iteration %d: %s", i+1, err)
				return
			}
			if commitID != i+1 {
				t.Errorf("Commit got ID %d, expected %d", commitID, i+1)
				return
			}
			ent, _, err := c.ListEntriesByPrefix(ctx, repo, "master", "", "", -1, false, false)
			if err != nil {
				t.Errorf("List committed data failed on iterations %d: %s", i+1, err)
				return
			}
			if len(ent) != i+1 {
				t.Errorf("List commited files got %d entries, expected %d", len(ent), i+1)
			}
		}
	})

}
