package mvcc

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func Test_paginateSlice(t *testing.T) {
	type args struct {
		s     []string
		limit int
	}
	tests := []struct {
		name        string
		args        args
		want        []string
		wantHasMore bool
	}{
		{
			name: "no page",
			args: args{
				s:     []string{"one", "two", "three"},
				limit: 3,
			},
			want:        []string{"one", "two", "three"},
			wantHasMore: false,
		},
		{
			name: "page",
			args: args{
				s:     []string{"one", "two", "three"},
				limit: 2,
			},
			want:        []string{"one", "two"},
			wantHasMore: true,
		},
		{
			name: "no data",
			args: args{
				s:     []string{},
				limit: 2,
			},
			want:        []string{},
			wantHasMore: false,
		},
		{
			name: "limitless",
			args: args{
				s:     []string{"one", "two", "three", "four", "five"},
				limit: -1,
			},
			want:        []string{"one", "two", "three", "four", "five"},
			wantHasMore: false,
		},
		{
			name: "zero limit",
			args: args{
				s:     []string{"one", "two", "three"},
				limit: 0,
			},
			want:        []string{},
			wantHasMore: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasMore := paginateSlice(&tt.args.s, tt.args.limit)
			if hasMore != tt.wantHasMore {
				t.Errorf("paginateSlice() hasMore = %v, want %v", hasMore, tt.wantHasMore)
			}
			if !reflect.DeepEqual(tt.args.s, tt.want) {
				t.Errorf("paginateSlice() slice = %s, want %s", spew.Sdump(tt.args.s), spew.Sdump(tt.want))
			}
		})
	}
}
