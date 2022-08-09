package graveler_test

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func sp(s string) *string {
	return &s
}

func TestNewUncommittedDiffIterator(t *testing.T) {
	tests := []struct {
		name                  string
		CommittedKeys         []string
		CommittedIdentities   []*string // nil identities treated as nil values
		UncommittedKeys       []string
		UncommittedIdentities []*string // nil identities treated as nil values
		ExpectedKeys          []string
		ExpectedDiffs         []graveler.DiffType
	}{
		{
			name:                  "added",
			CommittedKeys:         []string{"foo"},
			CommittedIdentities:   []*string{sp("foo_id")},
			UncommittedKeys:       []string{"bar"},
			UncommittedIdentities: []*string{sp("bar_id")},
			ExpectedKeys:          []string{"bar"},
			ExpectedDiffs:         []graveler.DiffType{graveler.DiffTypeAdded},
		},
		{
			name:                  "removed",
			CommittedKeys:         []string{"foo"},
			CommittedIdentities:   []*string{sp("foo_id")},
			UncommittedKeys:       []string{"foo"},
			UncommittedIdentities: []*string{nil},
			ExpectedKeys:          []string{"foo"},
			ExpectedDiffs:         []graveler.DiffType{graveler.DiffTypeRemoved},
		},
		{
			name:                  "changed",
			CommittedKeys:         []string{"foo"},
			CommittedIdentities:   []*string{sp("foo_id")},
			UncommittedKeys:       []string{"foo"},
			UncommittedIdentities: []*string{sp("foo_id_changed")},
			ExpectedKeys:          []string{"foo"},
			ExpectedDiffs:         []graveler.DiffType{graveler.DiffTypeChanged},
		},
		{
			name:                  "No changes",
			CommittedKeys:         []string{"foo"},
			CommittedIdentities:   []*string{sp("foo_id")},
			UncommittedKeys:       []string{},
			UncommittedIdentities: []*string{},
			ExpectedKeys:          []string{},
			ExpectedDiffs:         []graveler.DiffType{},
		},
		{
			name:                  "no changes - identical file",
			CommittedKeys:         []string{"foo"},
			CommittedIdentities:   []*string{sp("foo_id")},
			UncommittedKeys:       []string{"foo"},
			UncommittedIdentities: []*string{sp("foo_id")},
			ExpectedKeys:          []string{},
			ExpectedDiffs:         []graveler.DiffType{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// build commit list
			committedRecords := make([]*graveler.ValueRecord, len(tt.CommittedKeys))
			for i, key := range tt.CommittedKeys {
				identity := tt.CommittedIdentities[i]
				var val *graveler.Value
				if identity != nil {
					val = &graveler.Value{
						Identity: []byte(*identity),
						Data:     nil,
					}
				}
				committedRecords[i] = &graveler.ValueRecord{
					Key:   []byte(key),
					Value: val,
				}
			}
			fakeCommittedList := testutils.NewFakeValueIterator(committedRecords)

			// build uncommitted list
			uncommittedRecords := make([]*graveler.ValueRecord, len(tt.UncommittedKeys))
			for i, key := range tt.UncommittedKeys {
				identity := tt.UncommittedIdentities[i]
				var val *graveler.Value
				if identity != nil {
					val = &graveler.Value{
						Identity: []byte(*identity),
						Data:     nil,
					}
				}
				uncommittedRecords[i] = &graveler.ValueRecord{
					Key:   []byte(key),
					Value: val,
				}
			}
			fakeList := testutils.NewFakeValueIterator(uncommittedRecords)
			diffIT := graveler.NewUncommittedDiffIterator(context.Background(), fakeCommittedList, fakeList)

			// diff results
			diffRes := make([]graveler.DiffType, 0)
			KeyRes := make([]string, 0)
			for diffIT.Next() {
				diffRes = append(diffRes, diffIT.Value().Type)
				KeyRes = append(KeyRes, diffIT.Value().Key.String())
			}
			//validate err
			if diffIT.Err() != nil {
				t.Error(diffIT.Err())
			}
			// could join them both to one
			if diff := deep.Equal(tt.ExpectedKeys, KeyRes); diff != nil {
				t.Errorf("Webhook post Metadata diff=%s", diff)
			}
			if diff := deep.Equal(tt.ExpectedDiffs, diffRes); diff != nil {
				t.Errorf("Webhook post Metadata diff=%s", diff)
			}
		})
	}
}
