package graveler_test

import (
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestJoinedDiffIterator_NextValue(t *testing.T) {
	type fields struct {
		iterA graveler.DiffIterator
		iterB graveler.DiffIterator
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue []*graveler.Diff
	}{
		{
			name: "empty iterators",
			fields: fields{
				iterA: testutil.NewDiffIter([]graveler.Diff{}),
				iterB: testutil.NewDiffIter([]graveler.Diff{}),
			},
			wantValue: nil,
		},
		{
			name: "only first iterator",
			fields: fields{
				iterA: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/c"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/d"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewDiffIter([]graveler.Diff{}),
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
					Type: graveler.DiffTypeChanged,
					Key:  []byte("iterA/b"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeRemoved,
					Key:  []byte("iterA/c"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeConflict,
					Key:  []byte("iterA/d"),
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
				iterA: testutil.NewDiffIter([]graveler.Diff{}),
				iterB: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/c"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/d"),
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
					Key:  []byte("iterA/a"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeChanged,
					Key:  []byte("iterA/b"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeRemoved,
					Key:  []byte("iterA/c"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeConflict,
					Key:  []byte("iterA/d"),
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
				iterA: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
				iterB: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/b"),
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
			name: "one from each different order",
			fields: fields{
				iterA: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
						},
					},
				}),
				iterB: testutil.NewDiffIter([]graveler.Diff{
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
			name: "taking from first iterator before second",
			fields: fields{
				iterA: testutil.NewDiffIter([]graveler.Diff{
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
				iterB: testutil.NewDiffIter([]graveler.Diff{
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
		{
			name: "same values",
			fields: fields{
				iterA: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/c"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/d"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
				}),
				iterB: testutil.NewDiffIter([]graveler.Diff{
					{
						Type: graveler.DiffTypeAdded,
						Key:  []byte("iterA/a"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeChanged,
						Key:  []byte("iterA/b"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeRemoved,
						Key:  []byte("iterA/c"),
						Value: &graveler.Value{
							Identity: []byte("id"),
							Data:     nil,
						},
					},
					{
						Type: graveler.DiffTypeConflict,
						Key:  []byte("iterA/d"),
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
					Key:  []byte("iterA/a"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeChanged,
					Key:  []byte("iterA/b"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeRemoved,
					Key:  []byte("iterA/c"),
					Value: &graveler.Value{
						Identity: []byte("id"),
						Data:     nil,
					},
				},
				{
					Type: graveler.DiffTypeConflict,
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
			it = graveler.NewJoinedDiffIterator(tt.fields.iterA, tt.fields.iterB)
			defer it.Close()

			var got []*graveler.Diff
			for it.Next() {
				got = append(got, it.Value())
			}
			require.NoError(t, it.Err())
			// verify that what we produced is what we got from the iterator
			for i := range got {
				println("got", got[i].Key.String())
			}
			for i := range tt.wantValue {
				println("want", tt.wantValue[i].Key.String())
			}
			if diff := deep.Equal(got, tt.wantValue); diff != nil {
				t.Fatal("JoinedDiffIterator iterator found diff:", diff)
			}
		})
	}
}

func TestJoinedDiffIterator_Error(t *testing.T) {
	tests := []struct {
		name        string
		iterAErr    error
		iterBErr    error
		expectedErr error
	}{
		{
			name:        "iterA error",
			iterAErr:    errors.New("iterA error"),
			iterBErr:    nil,
			expectedErr: errors.New("iterA error"),
		},
		{
			name:        "iterB error",
			iterAErr:    nil,
			iterBErr:    errors.New("iterB error"),
			expectedErr: errors.New("iterB error"),
		},
		{
			name:        "iterA and iterB error",
			iterAErr:    errors.New("iterA error"),
			iterBErr:    errors.New("iterB error"),
			expectedErr: errors.Join(errors.New("iterA error"), errors.New("iterB error")),
		},
		{
			name: "no error",

			iterAErr: nil,
			iterBErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			itA := testutil.NewDiffIter([]graveler.Diff{})
			itB := testutil.NewDiffIter([]graveler.Diff{})
			if tt.iterAErr != nil {
				itA.SetErr(tt.iterAErr)
			}
			if tt.iterBErr != nil {
				itB.SetErr(tt.iterBErr)
			}
			combinedIter := graveler.NewJoinedDiffIterator(itA, itB)

			if tt.expectedErr != nil {
				require.Equal(t, tt.expectedErr, combinedIter.Err())
			} else {
				require.NoError(t, combinedIter.Err())
			}
		})
	}
}
