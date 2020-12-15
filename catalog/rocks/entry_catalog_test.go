package rocks

import (
	"context"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func Test_entryCatalog_GetEntry(t *testing.T) {
	type args struct {
		repositoryID graveler.RepositoryID
		ref          graveler.Ref
		path         Path
	}
	ctx := context.Background()
	tests := []struct {
		name    string
		db      graveler.Graveler
		args    args
		want    *Entry
		wantErr bool
	}{
		{
			name: "not found",
			db: GravelerMock{
				KeyValue: nil,
				Err:      graveler.ErrNotFound,
			},
			args: args{
				repositoryID: "repo1",
				ref:          "master",
				path:         "path1",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := entryCatalog{store: tt.db}
			got, err := ec.GetEntry(ctx, tt.args.repositoryID, tt.args.ref, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEntry() got = %v, want %v", got, tt.want)
			}
		})
	}
}
