package committed

import (
	"context"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestMerge(t *testing.T) {
	type args struct {
		ctx        context.Context
		writer     MetaRangeWriter
		baseIter   Iterator
		sourceIter Iterator
		destIter   Iterator
		opts       *ApplyOptions
	}
	tests := []struct {
		name    string
		args    args
		want    graveler.DiffSummary
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Merge(tt.args.ctx, tt.args.writer, tt.args.baseIter, tt.args.sourceIter, tt.args.destIter, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("Merge() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Merge() got = %v, want %v", got, tt.want)
			}
		})
	}
}
