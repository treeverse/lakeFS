package catalog

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Commit(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	for i := 0; i < 3; i++ {
		fileName := "/file" + strconv.Itoa(i)
		fileAddr := "/addr" + strconv.Itoa(i)
		if err := c.WriteEntry(ctx, "repo1", "master", fileName, "ff", fileAddr, i+1, nil); err != nil {
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
			name: "commit",
			args: args{
				repo:      "repo1",
				branch:    "master",
				message:   "merge to master",
				committer: "tester",
				metadata:  nil,
			},
			want:    1,
			wantErr: false,
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
