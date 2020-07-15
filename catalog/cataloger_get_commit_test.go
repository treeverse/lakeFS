package catalog

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetCommit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Round(time.Minute)
	mockClock := clock.NewMock()
	mockClock.Set(now)
	c := testCataloger(t, WithClock(mockClock))
	defer func() { _ = c.Close() }()

	// test data
	const testBranch = "master"
	repository := testCatalogerRepo(t, ctx, c, "repo", testBranch)
	const testCommitsLen = 2
	refs := make([]string, testCommitsLen)
	for i := 0; i < testCommitsLen; i++ {
		n := strconv.Itoa(i)
		testPath := "/file" + n
		meta := Metadata{"k" + n: "v" + n}
		msg := "Commit" + n
		committer := "tester" + n
		testCatalogerCreateEntry(t, ctx, c, repository, testBranch, testPath, meta, "")
		commitLog, err := c.Commit(ctx, repository, testBranch, msg, committer, meta)
		testutil.MustDo(t, "commit "+msg, err)
		refs[i] = commitLog.Reference
	}

	tests := []struct {
		name      string
		reference string
		want      *CommitLog
		wantErr   bool
	}{
		{
			name:      "first",
			reference: "~KJ8Wd1Rs96Z",
			want:      &CommitLog{Reference: "~KJ8Wd1Rs96Z", Committer: "tester0", Message: "Commit0", CreationDate: now, Metadata: Metadata{"k0": "v0"}},
			wantErr:   false,
		},
		{
			name:      "second",
			reference: "~KJ8Wd1Rs96a",
			want:      &CommitLog{Reference: "~KJ8Wd1Rs96a", Committer: "tester1", Message: "Commit1", CreationDate: now, Metadata: Metadata{"k1": "v1"}},
			wantErr:   false,
		},
		{
			name:      "unknown",
			reference: "~unknown",
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "empty",
			reference: "",
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "branch",
			reference: "master",
			want:      nil,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetCommit(ctx, repository, tt.reference)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCommit() error = %s, wantErr %t", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCommit() got = %s, want %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}
