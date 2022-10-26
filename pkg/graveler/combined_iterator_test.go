package graveler_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestCombinedIterator_NextValue(t *testing.T) {
	type fields struct {
		iters          []graveler.ValueIterator
		withTombstones bool
		p              graveler.ValueIterator
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue []*graveler.ValueRecord
	}{
		{
			name: "empty iterators",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				},
				p: nil,
			},
			wantValue: nil,
		},
		{
			name: "only first iterator",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
					testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
				},
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
				},
			},
		},
		{
			name: "only one iterator",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
				},
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
				},
			},
		},
		{
			name: "only second iterator",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
				},
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
				},
			},
		},
		{
			name: "one from each",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("iterA/two"),
							Value: &graveler.Value{
								Identity: []byte("id"),
								Data:     nil,
							},
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("iterA/one"),
							Value: &graveler.Value{
								Identity: []byte("id"),
								Data:     nil,
							},
						},
					}),
				},
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
				},
			},
		},
		{
			name: "value tombstone",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/one"),
							Value: &graveler.Value{
								Identity: []byte("id"),
								Data:     nil,
							},
						},
					}),
				},
				p: nil,
			},
			wantValue: []*graveler.ValueRecord{
				{
					Key: []byte("path/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "unexpected tombstone",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key:   []byte("path/one"),
							Value: nil,
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("two"),
								Data:     nil,
							},
						},
					}),
				},
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
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
				},
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
				},
			},
		},
		{
			name: "common prefix tombstone with returned tombstones",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
				},
				p:              nil,
				withTombstones: true,
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
			name: "4 iterators without tombstones",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
								Identity: []byte("a"),
								Data:     nil,
							},
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/one"),
							Value: &graveler.Value{
								Identity: []byte("two"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("b"),
								Data:     nil,
							},
						},
						{
							Key:   []byte("path/three"),
							Value: nil,
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key:   []byte("path/one"),
							Value: nil,
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("c"),
								Data:     nil,
							},
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/one"),
							Value: &graveler.Value{
								Identity: []byte("four"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("d"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/three"),
							Value: &graveler.Value{
								Identity: []byte("a"),
								Data:     nil,
							},
						},
						{
							Key:   []byte("path/four"),
							Value: nil,
						},
					}),
				},
				p:              nil,
				withTombstones: false,
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
						Identity: []byte("a"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "4 iterators with tombstones",
			fields: fields{
				iters: []graveler.ValueIterator{
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
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
								Identity: []byte("a"),
								Data:     nil,
							},
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/one"),
							Value: &graveler.Value{
								Identity: []byte("two"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("b"),
								Data:     nil,
							},
						},
						{
							Key:   []byte("path/three"),
							Value: nil,
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key:   []byte("path/one"),
							Value: nil,
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("c"),
								Data:     nil,
							},
						},
					}),
					testutil.NewValueIteratorFake([]graveler.ValueRecord{
						{
							Key: []byte("path/one"),
							Value: &graveler.Value{
								Identity: []byte("four"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/two"),
							Value: &graveler.Value{
								Identity: []byte("d"),
								Data:     nil,
							},
						},
						{
							Key: []byte("path/three"),
							Value: &graveler.Value{
								Identity: []byte("a"),
								Data:     nil,
							},
						},
						{
							Key:   []byte("path/four"),
							Value: nil,
						},
					}),
				},
				p:              nil,
				withTombstones: true,
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
						Identity: []byte("a"),
						Data:     nil,
					},
				},
				{
					Key:   []byte("path/three"),
					Value: nil,
				},
				{
					Key:   []byte("path/four"),
					Value: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var it graveler.ValueIterator
			it = graveler.NewCombinedIterator(tt.fields.iters...)
			if !tt.fields.withTombstones {
				it = graveler.NewFilterTombstoneIterator(it)
			}
			defer it.Close()

			var got []*graveler.ValueRecord
			for it.Next() {
				got = append(got, it.Value())
			}
			require.NoError(t, it.Err())
			// verify that what we produced is what we got from the iterator
			if diff := deep.Equal(got, tt.wantValue); diff != nil {
				t.Fatal("ValueToEntry iterator found diff:", diff)
			}
		})
	}
}
