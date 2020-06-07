package catalog

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

func TestCataloger_ListBranches(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	const numOfBranches = 3
	for i := numOfBranches; i > 0; i-- {
		if _, err := c.CreateBranch(ctx, "repo1", "b"+strconv.Itoa(i), "master"); err != nil {
			t.Fatal("create branch for testing", err)
		}
	}
	for i := numOfBranches; i > 0; i-- {
		if _, err := c.CreateBranch(ctx, "repo1", "z"+strconv.Itoa(i), "master"); err != nil {
			t.Fatal("create branch for testing", err)
		}
	}

	type args struct {
		repo   string
		prefix string
		limit  int
		after  string
	}
	tests := []struct {
		name         string
		args         args
		wantBranches []string
		wantMore     bool
		wantErr      bool
	}{
		{
			name:         "all",
			args:         args{repo: "repo1", prefix: "", limit: -1, after: ""},
			wantBranches: []string{"b1", "b2", "b3", "master", "z1", "z2", "z3"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "z prefix",
			args:         args{repo: "repo1", prefix: "z", limit: -1, after: ""},
			wantBranches: []string{"z1", "z2", "z3"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "first 3",
			args:         args{repo: "repo1", prefix: "", limit: 3, after: ""},
			wantBranches: []string{"b1", "b2", "b3"},
			wantMore:     true,
			wantErr:      false,
		},
		{
			name:         "after master",
			args:         args{repo: "repo1", prefix: "", limit: 3, after: "master"},
			wantBranches: []string{"z1", "z2", "z3"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "no items with prefix",
			args:         args{repo: "repo1", prefix: "zzz", limit: -1, after: ""},
			wantBranches: nil,
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "unknown repo",
			args:         args{repo: "repo2", prefix: "", limit: 5, after: ""},
			wantBranches: nil,
			wantMore:     false,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListBranches(ctx, tt.args.repo, tt.args.prefix, tt.args.limit, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListBranches() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			var gotBranches []string
			for i := range got {
				gotBranches = append(gotBranches, got[i].Name)
			}
			if !reflect.DeepEqual(tt.wantBranches, gotBranches) {
				t.Errorf("ListBranches() got = %v, want %v", gotBranches, tt.wantBranches)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListBranches() got1 = %v, want %v", gotMore, tt.wantMore)
			}
		})
	}
}
