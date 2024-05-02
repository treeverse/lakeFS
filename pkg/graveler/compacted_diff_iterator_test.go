package graveler_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestCompactedDiffIterator_NextValue(t *testing.T) {
	type fields struct {
		stageAndCommittedDiffIterator     graveler.DiffIterator
		compactedAndCommittedDiffIterator graveler.DiffIterator
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue []*graveler.Diff
	}{
		{
			name: "empty iterators",
			fields: fields{
				stageAndCommittedDiffIterator:     testutil.NewDiffIter([]graveler.Diff{}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{}),
			},
			wantValue: nil,
		},
		{
			name: "only stage iterator",
			fields: fields{
				stageAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/three"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/four"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{}),
			},
			wantValue: []*graveler.Diff{
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeChanged,
					Key:  []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeRemoved,
					Key:  []byte("iterA/three"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeConflict,
					Key:  []byte("iterA/four"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "only compacted iterator",
			fields: fields{
				stageAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/three"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/four"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
			},
			wantValue: []*graveler.Diff{
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeChanged,
					Key:  []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeRemoved,
					Key:  []byte("iterA/three"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeConflict,
					Key:  []byte("iterA/four"),
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
				stageAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/one"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/two"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
			},
			wantValue: []*graveler.Diff{
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/one"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/two"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "one from each different order",
			fields: fields{
				stageAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
			},
			wantValue: []*graveler.Diff{
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/a"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/b"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
			},
		},
		{
			name: "taking staging before compacted",
			fields: fields{
				stageAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/d"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
				compactedAndCommittedDiffIterator: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("wrong"),
						},
					},
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/c"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
			},
			wantValue: []*graveler.Diff{
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/a"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/b"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/c"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeAdded,
					Key:  []byte("iterA/d"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var it graveler.DiffIterator
			it = graveler.NewCompactedDiffIterator(tt.fields.stageAndCommittedDiffIterator, tt.fields.compactedAndCommittedDiffIterator)
			defer it.Close()

			var got []*graveler.Diff
			for it.Next() {
				got = append(got, it.Value())
			}
			require.NoError(t, it.Err())
			// verify that what we produced is what we got from the iterator
			if diff := deep.Equal(got, tt.wantValue); diff != nil {
				t.Fatal("CompactedDiffIterator iterator found diff:", diff)
			}
		})
	}
}
