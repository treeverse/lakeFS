package tree_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/graveler/committed/tree"
	"github.com/treeverse/lakefs/graveler/testutil"

	"github.com/go-openapi/swag"

	"github.com/treeverse/lakefs/graveler"
)

func TestMergeIterator(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	tests := []struct {
		leftKeys            []string
		leftIdentities      []string
		rightKeys           []string
		rightIdentities     []string
		baseKeys            []string
		baseIdentities      []string
		expectedDiffKeys    []string
		expectedDiffTypes   []graveler.DiffType
		conflictExpectedIdx *int
	}{
		{
			leftKeys:          []string{},
			baseKeys:          []string{},
			rightKeys:         []string{"k1", "k2"},
			rightIdentities:   []string{"i1", "i2"},
			expectedDiffKeys:  []string{"k1", "k2"},
			expectedDiffTypes: []graveler.DiffType{added, added},
		},
		{
			leftKeys:          []string{},
			baseKeys:          []string{"k1"},
			baseIdentities:    []string{"i1"},
			rightKeys:         []string{"k1", "k2"},
			rightIdentities:   []string{"i1", "i2"},
			expectedDiffKeys:  []string{"k2"},
			expectedDiffTypes: []graveler.DiffType{added},
		},
		{
			leftKeys:          []string{"k1"},
			leftIdentities:    []string{"i1"},
			rightKeys:         []string{},
			baseKeys:          []string{"k1"},
			baseIdentities:    []string{"i1"},
			expectedDiffKeys:  []string{"k1"},
			expectedDiffTypes: []graveler.DiffType{removed},
		},
		{
			leftKeys:          []string{"k1"},
			leftIdentities:    []string{"i1"},
			rightKeys:         []string{"k1"},
			rightIdentities:   []string{"i1a"},
			baseKeys:          []string{"k1"},
			baseIdentities:    []string{"i1"},
			expectedDiffKeys:  []string{"k1"},
			expectedDiffTypes: []graveler.DiffType{changed},
		},
		{
			leftKeys:          []string{"k1", "k2"},
			leftIdentities:    []string{"i2", "i2"},
			rightKeys:         []string{"k1", "k2"},
			rightIdentities:   []string{"i1", "i2a"},
			baseKeys:          []string{"k1", "k2"},
			baseIdentities:    []string{"i1", "i2"},
			expectedDiffKeys:  []string{"k2"},
			expectedDiffTypes: []graveler.DiffType{changed},
		},
		{
			leftKeys:            []string{"k1"},
			leftIdentities:      []string{"i1a"},
			baseKeys:            []string{"k1"},
			baseIdentities:      []string{"i1"},
			rightKeys:           []string{"k1", "k2"},
			rightIdentities:     []string{"i1b", "i2"},
			expectedDiffKeys:    []string{},
			expectedDiffTypes:   []graveler.DiffType{},
			conflictExpectedIdx: swag.Int(0),
		},
		{
			leftKeys:            []string{"k2"},
			leftIdentities:      []string{"i2a"},
			baseKeys:            []string{"k2"},
			baseIdentities:      []string{"i2"},
			rightKeys:           []string{"k1", "k2"},
			rightIdentities:     []string{"i1", "i2b"},
			expectedDiffKeys:    []string{"k1"},
			expectedDiffTypes:   []graveler.DiffType{added},
			conflictExpectedIdx: swag.Int(1),
		},
		{
			leftKeys:            []string{},
			leftIdentities:      []string{},
			baseKeys:            []string{"k1"},
			baseIdentities:      []string{"i1"},
			rightKeys:           []string{"k1"},
			rightIdentities:     []string{"i1a"},
			expectedDiffKeys:    []string{},
			expectedDiffTypes:   []graveler.DiffType{},
			conflictExpectedIdx: swag.Int(0),
		},
		{
			leftKeys:            []string{"k1"},
			leftIdentities:      []string{"i1a"},
			baseKeys:            []string{"k1"},
			baseIdentities:      []string{"i1"},
			rightKeys:           []string{},
			rightIdentities:     []string{},
			expectedDiffKeys:    []string{},
			expectedDiffTypes:   []graveler.DiffType{},
			conflictExpectedIdx: swag.Int(0),
		},
		{
			leftKeys:            []string{"k1"},
			leftIdentities:      []string{"i1a"},
			baseKeys:            []string{},
			baseIdentities:      []string{},
			rightKeys:           []string{"k1"},
			rightIdentities:     []string{"i1b"},
			expectedDiffKeys:    []string{},
			expectedDiffTypes:   []graveler.DiffType{},
			conflictExpectedIdx: swag.Int(0),
		},
		{
			leftKeys:            []string{"k1"},
			leftIdentities:      []string{"i1a"},
			baseKeys:            []string{"k0"},
			baseIdentities:      []string{"i0"},
			rightKeys:           []string{"k1"},
			rightIdentities:     []string{"i1b"},
			expectedDiffKeys:    []string{},
			expectedDiffTypes:   []graveler.DiffType{},
			conflictExpectedIdx: swag.Int(0),
		},
	}
	for i, tst := range tests {
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			baseRecords := map[string]*graveler.Value{}
			for i, k := range tst.baseKeys {
				baseRecords[k] = &graveler.Value{Identity: []byte(tst.baseIdentities[i])}
			}
			diffIt := tree.NewDiffIterator(
				newFakeTreeIterator([][]string{tst.leftKeys}, [][]string{tst.leftIdentities}),
				newFakeTreeIterator([][]string{tst.rightKeys}, [][]string{tst.rightIdentities}))
			committedFake := testutil.NewCommittedFake()
			committedFake.ValuesByKey = baseRecords
			it := tree.NewMergeIterator(diffIt, "a", committedFake)
			var values []*graveler.ValueRecord
			idx := 0
			for it.Next() {
				idx++
				values = append(values, it.Value())
			}
			if tst.conflictExpectedIdx != nil {
				if !errors.Is(it.Err(), graveler.ErrConflictFound) {
					t.Fatalf("expected conflict but didn't get one. err=%v", it.Err())
				}
				if *tst.conflictExpectedIdx != idx {
					t.Fatalf("got conflict at unexpected index. expected at=%d, got at=%d", *tst.conflictExpectedIdx, idx)
				}
			} else if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if len(values) != len(tst.expectedDiffKeys) {
				t.Fatalf("actual diff length different than expected. expected=%d, got=%d", len(tst.expectedDiffKeys), len(values))
			}
			for i, d := range values {
				if string(d.Key) != tst.expectedDiffKeys[i] {
					t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key))
				}
				if tst.expectedDiffTypes[i] == removed && d.Value != nil {
					t.Fatalf("expected nil value in diff index %d. got=%s", i, d.Value)
				}
				if tst.expectedDiffTypes[i] != removed && d.Value == nil {
					t.Fatalf("unexpected nil value in diff index %d", i)
				}
				//if string(d.Value.Identity) != tst.expectedDiffIdentities[i] {
				//	t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", i, tst.expectedDiffIdentities[i], string(d.Value.Identity))
				//}
				//if string(d.OldIdentity) != tst.expectedOldIdentities[i] {
				//	t.Fatalf("unexpected old identity in diff index %d. expected=%s, got=%s", i, tst.expectedOldIdentities[i], string(d.OldIdentity))
				//}
			}
		})
	}
}

//
//func newValues(keys, identities []string) []graveler.ValueRecord {
//	var res []graveler.ValueRecord
//	for i, key := range keys {
//		res = append(res, graveler.ValueRecord{
//			Key: []byte(key),
//			Value: &graveler.Value{
//				Identity: []byte(identities[i]),
//				Data:     []byte("some-data"),
//			},
//		})
//	}
//	return res
//}
