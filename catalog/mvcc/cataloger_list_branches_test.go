package mvcc

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/catalog"
)

func TestCataloger_ListBranches(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	if _, err := c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"); err != nil {
		t.Fatal("create repository for testing", err)
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
		repository string
		prefix     string
		limit      int
		after      string
	}
	tests := []struct {
		name         string
		args         args
		wantBranches []string
		wantRefs     []string
		wantMore     bool
		wantErr      bool
	}{
		{
			name:         "all",
			args:         args{repository: "repo1", prefix: "", limit: -1, after: ""},
			wantBranches: []string{"b1", "b2", "b3", catalog.DefaultImportBranchName, "master", "z1", "z2", "z3"},
			wantRefs:     []string{"~3WaKeL", "~3Waf8F", "~3WazcA", "~3BJEPkwHYMdLDpaBrwnpTPmfarYET3yz", "~KJ8Wd1Rs96Z", "~48A2Qf", "~48AMta", "~48AhNV"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "z prefix",
			args:         args{repository: "repo1", prefix: "z", limit: -1, after: ""},
			wantBranches: []string{"z1", "z2", "z3"},
			wantRefs:     []string{"~48A2Qf", "~48AMta", "~48AhNV"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "first 3",
			args:         args{repository: "repo1", prefix: "", limit: 3, after: ""},
			wantBranches: []string{"b1", "b2", "b3"},
			wantRefs:     []string{"~3WaKeL", "~3Waf8F", "~3WazcA"},
			wantMore:     true,
			wantErr:      false,
		},
		{
			name:         "after master",
			args:         args{repository: "repo1", prefix: "", limit: 3, after: "master"},
			wantBranches: []string{"z1", "z2", "z3"},
			wantRefs:     []string{"~48A2Qf", "~48AMta", "~48AhNV"},
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "no items with prefix",
			args:         args{repository: "repo1", prefix: "zzz", limit: -1, after: ""},
			wantBranches: nil,
			wantMore:     false,
			wantErr:      false,
		},
		{
			name:         "unknown repository",
			args:         args{repository: "repo2", prefix: "", limit: 5, after: ""},
			wantBranches: nil,
			wantMore:     false,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListBranches(ctx, tt.args.repository, tt.args.prefix, tt.args.limit, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListBranches() error = %v, wantErr %v", err, tt.wantErr)
			}
			var gotBranches []string
			var gotRefs []string
			for i := range got {
				gotBranches = append(gotBranches, got[i].Name)
				gotRefs = append(gotRefs, got[i].Reference)
			}
			if diff := deep.Equal(tt.wantBranches, gotBranches); diff != nil {
				t.Error("ListBranches() names found diff:", diff)
			}
			if diff := deep.Equal(tt.wantRefs, gotRefs); diff != nil {
				t.Error("ListBranches() refs found diff:", diff)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListBranches() wantedMore = %v, want %v", gotMore, tt.wantMore)
			}
		})
	}
}
