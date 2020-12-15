package graveler_test

import (
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func TestDiffIterator(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	tests := []struct {
		leftKeys               []string
		leftIdentities         []string
		rightKeys              []string
		rightIdentities        []string
		expectedDiffKeys       []string
		expectedDiffTypes      []graveler.DiffType
		expectedDiffIdentities []string
		expectedOldIdentities  []string
	}{
		{
			leftKeys:         []string{"k1", "k2", "k3"},
			leftIdentities:   []string{"i1", "i2", "i3"},
			rightKeys:        []string{"k1", "k2", "k3"},
			rightIdentities:  []string{"i1", "i2", "i3"},
			expectedDiffKeys: []string{},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3"},
			leftIdentities:         []string{"i1", "i2", "i3"},
			rightKeys:              []string{"k1", "k2", "k3", "k4"},
			rightIdentities:        []string{"i1", "i2", "i3", "i4"},
			expectedDiffKeys:       []string{"k4"},
			expectedDiffTypes:      []graveler.DiffType{added},
			expectedDiffIdentities: []string{"i4"},
			expectedOldIdentities:  []string{""},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k4"},
			leftIdentities:         []string{"i1", "i2", "i3", "i4"},
			rightKeys:              []string{"k1", "k2", "k3"},
			rightIdentities:        []string{"i1", "i2", "i3"},
			expectedDiffKeys:       []string{"k4"},
			expectedDiffTypes:      []graveler.DiffType{removed},
			expectedDiffIdentities: []string{"i4"},
			expectedOldIdentities:  []string{"i4"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k5"},
			leftIdentities:         []string{"i1", "i2", "i3", "i5"},
			rightKeys:              []string{"k1", "k2", "k3", "k4"},
			rightIdentities:        []string{"i1", "i2", "i3", "i4"},
			expectedDiffKeys:       []string{"k4", "k5"},
			expectedDiffTypes:      []graveler.DiffType{added, removed},
			expectedDiffIdentities: []string{"i4", "i5"},
			expectedOldIdentities:  []string{"", "i5"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3"},
			leftIdentities:         []string{"i1", "i2", "i3"},
			rightKeys:              []string{"k1", "k2", "k3"},
			rightIdentities:        []string{"i1", "i2", "i3a"},
			expectedDiffKeys:       []string{"k3"},
			expectedDiffTypes:      []graveler.DiffType{changed},
			expectedDiffIdentities: []string{"i3a"},
			expectedOldIdentities:  []string{"i3"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3"},
			leftIdentities:         []string{"i1", "i2", "i3"},
			rightKeys:              []string{"k3", "k4", "k5", "k6"},
			rightIdentities:        []string{"i3a", "i4", "i5", "i6"},
			expectedDiffKeys:       []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			expectedDiffTypes:      []graveler.DiffType{removed, removed, changed, added, added, added},
			expectedDiffIdentities: []string{"i1", "i2", "i3a", "i4", "i5", "i6"},
			expectedOldIdentities:  []string{"i1", "i2", "i3", "", "", ""},
		},
		{
			// diff between two empty iterators - should return empty diff
		},
		{
			leftKeys:               []string{},
			leftIdentities:         []string{},
			rightKeys:              []string{"k1", "k2", "k3"},
			rightIdentities:        []string{"i1", "i2", "i3"},
			expectedDiffKeys:       []string{"k1", "k2", "k3"},
			expectedDiffTypes:      []graveler.DiffType{added, added, added},
			expectedDiffIdentities: []string{"i1", "i2", "i3"},
			expectedOldIdentities:  []string{"", "", ""},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			leftIdentities:         []string{"i1", "i2", "i3", "i4", "i5", "i6"},
			rightKeys:              []string{"k3", "k4", "k5", "k6", "k7"},
			rightIdentities:        []string{"i3", "i4", "i5", "i6", "i7"},
			expectedDiffKeys:       []string{"k1", "k2", "k7"},
			expectedDiffTypes:      []graveler.DiffType{removed, removed, added},
			expectedDiffIdentities: []string{"i1", "i2", "i7"},
			expectedOldIdentities:  []string{"i1", "i2", ""},
		},
		{
			leftKeys:               []string{"k3", "k4", "k5"},
			leftIdentities:         []string{"i3", "i4", "i5"},
			rightKeys:              []string{"k1", "k2", "k3", "k4", "k5"},
			rightIdentities:        []string{"i1", "i2", "i3", "i4", "i5"},
			expectedDiffKeys:       []string{"k1", "k2"},
			expectedDiffTypes:      []graveler.DiffType{added, added},
			expectedDiffIdentities: []string{"i1", "i2"},
			expectedOldIdentities:  []string{"", ""},
		},
	}
	for _, tst := range tests {
		it := graveler.NewDiffIterator(
			newMockValueIterator(newValues(tst.leftKeys, tst.leftIdentities)),
			newMockValueIterator(newValues(tst.rightKeys, tst.rightIdentities)))
		var diffs []*graveler.Diff
		for it.Next() {
			diffs = append(diffs, it.Value())
		}
		if it.Err() != nil {
			t.Fatalf("got unexpected error: %v", it.Err())
		}
		if len(diffs) != len(tst.expectedDiffKeys) {
			t.Fatalf("actual diff length different than expected. expected=%d, got=%d", len(tst.expectedDiffKeys), len(diffs))
		}
		for i, d := range diffs {
			if string(d.Key) != tst.expectedDiffKeys[i] {
				t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key))
			}
			if d.Type != tst.expectedDiffTypes[i] {
				t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key))
			}
			if string(d.Value.Identity) != tst.expectedDiffIdentities[i] {
				t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", i, tst.expectedDiffIdentities[i], string(d.Value.Identity))
			}
			if string(d.OldIdentity) != tst.expectedOldIdentities[i] {
				t.Fatalf("unexpected old identity in diff index %d. expected=%s, got=%s", i, tst.expectedOldIdentities[i], string(d.OldIdentity))
			}
		}
	}
}

func newValues(keys, identities []string) []graveler.ValueRecord {
	var res []graveler.ValueRecord
	for i, key := range keys {
		res = append(res, graveler.ValueRecord{
			Key: []byte(key),
			Value: &graveler.Value{
				Identity: []byte(identities[i]),
				Data:     []byte("some-data"),
			},
		})
	}
	return res
}
