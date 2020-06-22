package catalog

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"code.cloudfoundry.org/clock/fakeclock"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetCommit(t *testing.T) {
	ctx := context.Background()
	db, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	now := time.Now().Round(time.Minute)
	c := &cataloger{
		Clock: fakeclock.NewFakeClock(now),
		log:   logging.Default().WithField("service_name", "cataloger"),
		db:    db,
	}

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
		var err error
		refs[i], err = c.Commit(ctx, repository, testBranch, msg, committer, meta)
		if err != nil {
			t.Fatalf("GetCommit commit msg='%s' failed with error: %s", msg, err)
		}
	}

	tests := []struct {
		name      string
		reference string
		want      *CommitLog
		wantErr   bool
	}{
		{
			name:      "first",
			reference: "~KJ8Wd1Rs96Y",
			want:      &CommitLog{Reference: "~KJ8Wd1Rs96Y", Committer: "tester0", Message: "Commit0", CreationDate: now, Metadata: Metadata{"k0": "v0"}},
			wantErr:   false,
		},
		{
			name:      "second",
			reference: "~KJ8Wd1Rs96Z",
			want:      &CommitLog{Reference: "~KJ8Wd1Rs96Z", Committer: "tester1", Message: "Commit1", CreationDate: now, Metadata: Metadata{"k1": "v1"}},
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
				t.Errorf("GetCommit() got = %v, want %v", got, tt.want)
			}
		})
	}
}
