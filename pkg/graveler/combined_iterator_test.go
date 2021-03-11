package graveler_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestCombinedIterator_NextValue(t *testing.T) {
	type fields struct {
		iterA graveler.ValueIterator
		iterB graveler.ValueIterator
		p     graveler.ValueIterator
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue []*graveler.ValueRecord
	}{
		{
			name: "empty iterators",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				p:     nil,
			},
			wantValue: nil,
		},
		{
			name: "only first iterator",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Key: []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				p:     nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Key: []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				}},
		},
		{
			name: "only first iterator",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Key: []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				p:     nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Key: []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				}},
		},
		{
			name: "only second iterator",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Key: []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					}}),
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Key: []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				}},
		},
		{
			name: "one from each",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Key: []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				}},
		},
		{
			name: "value tombstone",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key:   []byte("path/one"),
						Value: nil,
					},
					{
						Key: []byte("path/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("path/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("path/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				}},
		},
		{
			name: "unexpected tombstone",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key:   []byte("path/one"),
						Value: nil,
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("path/two"),
						Value: &graveler.Value{
							Identity: []byte("two"),
							Data:     nil,
						},
					},
				}),
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("path/two"),
					Value: &graveler.Value{
						Identity: []byte("two"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "common prefix tombstone",
			fields: fields{
				iterA: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("path/one"),
						Value: &graveler.Value{
							Identity: []byte("one"),
							Data:     nil,
						},
					},
					{
						Key:   []byte("path/to/remove/one"),
						Value: nil,
					},
					{
						Key:   []byte("path/to/remove/two"),
						Value: nil,
					},
					{
						Key:   []byte("path/to/remove/three"),
						Value: nil,
					},
				}),
				iterB: testutil.NewValueIteratorFake([]graveler.ValueRecord{
					{
						Key: []byte("path/to/remove/one"),
						Value: &graveler.Value{
							Identity: []byte("remove-one"),
							Data:     nil,
						},
					},
					{
						Key: []byte("path/to/remove/two"),
						Value: &graveler.Value{
							Identity: []byte("remove-two"),
							Data:     nil,
						},
					},
					{
						Key: []byte("path/to/remove/three"),
						Value: &graveler.Value{
							Identity: []byte("remove-three"),
							Data:     nil,
						},
					},
					{
						Key: []byte("path/two"),
						Value: &graveler.Value{
							Identity: []byte("two"),
							Data:     nil,
						},
					},
				}),
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("path/one"),
					Value: &graveler.Value{
						Identity: []byte("one"),
						Data:     nil,
					},
				},
				{
					Key: []byte("path/two"),
					Value: &graveler.Value{
						Identity: []byte("two"),
						Data:     nil,
					},
				}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := graveler.NewCombinedIterator(tt.fields.iterA, tt.fields.iterB)
			defer it.Close()

			var got []*graveler.ValueRecord
			for it.Next() {
				got = append(got, it.Value())
			}
			// verify that what we produced is what we got from the iterator
			if diff := deep.Equal(got, tt.wantValue); diff != nil {
				t.Fatal("ValueToEntry iterator found diff:", diff)
			}
		})
	}
}
