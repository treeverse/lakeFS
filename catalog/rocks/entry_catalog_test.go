package rocks

import (
	"context"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func Test_entryCatalog_GetEntry(t *testing.T) {
	type fields struct {
		db graveler.Graveler
	}
	type args struct {
		ctx          context.Context
		repositoryID graveler.RepositoryID
		ref          graveler.Ref
		path         Path
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Entry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := entryCatalog{
				db: tt.fields.db,
			}
			got, err := e.GetEntry(tt.args.ctx, tt.args.repositoryID, tt.args.ref, tt.args.path)
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
