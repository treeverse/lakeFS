package committed_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/mock"
)

func TestUnmarshalIterator(t *testing.T) {
	tests := []struct {
		Name   string
		Keys   []graveler.Key
		Values []*graveler.Value
	}{
		{
			Name:   "empty",
			Keys:   make([]graveler.Key, 0),
			Values: make([]*graveler.Value, 0),
		},
		{
			Name:   "one",
			Keys:   []graveler.Key{graveler.Key("k1")},
			Values: []*graveler.Value{{Identity: []byte(""), Data: []byte("")}},
		},
		{
			Name: "two",
			Keys: []graveler.Key{
				graveler.Key("k1"),
				graveler.Key("k2"),
			},
			Values: []*graveler.Value{
				{Identity: []byte("id1"), Data: []byte("data1")},
				{Identity: []byte("id2"), Data: []byte("data2")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// we expect the iterator to call our iterator n+1 times and check an error on the last call
			mockIt := mock.NewMockValueIterator(ctrl)
			mockIt.EXPECT().Next().Times(len(tt.Keys)).
				Return(true)
			for i := range tt.Keys {
				mockIt.EXPECT().Value().Times(1).
					Return(&committed.Record{
						Key:   committed.Key(tt.Keys[i]),
						Value: committed.MustMarshalValue(tt.Values[i]),
					})
			}
			mockIt.EXPECT().Next().Times(1).Return(false)
			mockIt.EXPECT().Err().Times(1).Return(nil)

			// use the iterator and collect the values
			keys := make([]graveler.Key, 0)
			values := make([]*graveler.Value, 0)
			it := committed.NewUnmarshalIterator(mockIt)
			for it.Next() {
				v := it.Value()
				if v == nil {
					t.Fatal("Iterator return value is nil after next")
				}
				keys = append(keys, v.Key)
				values = append(values, v.Value)
			}
			if err := it.Err(); err != nil {
				t.Fatal("Iteration ended with error", err)
			}
			// verify keys and values
			if diff := deep.Equal(keys, tt.Keys); diff != nil {
				t.Fatal("Difference found in keys", diff)
			}
			if diff := deep.Equal(values, tt.Values); diff != nil {
				t.Fatal("Difference found in values", diff)
			}
		})
	}
}
