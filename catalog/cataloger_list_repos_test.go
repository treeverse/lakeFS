package catalog

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListRepos(t *testing.T) {
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	// create test data
	_, err := c.CreateRepo("repo1", "bucket1", "master")
	if err != nil {
		t.Fatal("create repo for testing failed", err)
	}
	_, err = c.CreateRepo("repo2", "bucket2", "master")
	if err != nil {
		t.Fatal("create repo for testing failed", err)
	}
	_, err = c.CreateRepo("repo3", "bucket3", "master")
	if err != nil {
		t.Fatal("create repo for testing failed", err)
	}

	type args struct {
		amount int
		after  int
	}
	tests := []struct {
		name     string
		args     args
		want     []string
		wantMore bool
		wantErr  bool
	}{
		{
			name:     "basic",
			args:     args{amount: 10, after: 0},
			want:     []string{"repo1", "repo2", "repo3"},
			wantMore: false,
			wantErr:  false,
		},
		{
			name:     "small amount",
			args:     args{amount: 1, after: 0},
			want:     []string{"repo1"},
			wantMore: true,
			wantErr:  false,
		},
		{
			name:     "the rest",
			args:     args{amount: 10, after: 1},
			want:     []string{"repo2", "repo3"},
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListRepos(tt.args.amount, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListRepos() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			var names []string
			for _, repo := range got {
				names = append(names, repo.Name)
			}
			if !reflect.DeepEqual(tt.want, names) {
				t.Errorf("ListRepos() got repos = %v, want %v", names, tt.want)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListRepos() got more = %v, want %v", gotMore, tt.wantMore)
			}
		})
	}
}
