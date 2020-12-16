package graveler_test

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func TestDiff(t *testing.T) {
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
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k4"},
			leftIdentities:         []string{"i1", "i2", "i3", "i4"},
			rightKeys:              []string{"k1", "k2", "k3"},
			rightIdentities:        []string{"i1", "i2", "i3"},
			expectedDiffKeys:       []string{"k4"},
			expectedDiffTypes:      []graveler.DiffType{removed},
			expectedDiffIdentities: []string{"i4"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k5"},
			leftIdentities:         []string{"i1", "i2", "i3", "i5"},
			rightKeys:              []string{"k1", "k2", "k3", "k4"},
			rightIdentities:        []string{"i1", "i2", "i3", "i4"},
			expectedDiffKeys:       []string{"k4", "k5"},
			expectedDiffTypes:      []graveler.DiffType{added, removed},
			expectedDiffIdentities: []string{"i4", "i5"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3"},
			leftIdentities:         []string{"i1", "i2", "i3"},
			rightKeys:              []string{"k1", "k2", "k3"},
			rightIdentities:        []string{"i1", "i2", "i3a"},
			expectedDiffKeys:       []string{"k3"},
			expectedDiffTypes:      []graveler.DiffType{changed},
			expectedDiffIdentities: []string{"i3a"},
		},
		{
			leftKeys:               []string{"k1", "k2", "k3"},
			leftIdentities:         []string{"i1", "i2", "i3"},
			rightKeys:              []string{"k3", "k4", "k5", "k6"},
			rightIdentities:        []string{"i3a", "i4", "i5", "i6"},
			expectedDiffKeys:       []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			expectedDiffTypes:      []graveler.DiffType{removed, removed, changed, added, added, added},
			expectedDiffIdentities: []string{"i1", "i2", "i3a", "i4", "i5", "i6"},
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
		},
		{
			leftKeys:               []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			leftIdentities:         []string{"i1", "i2", "i3", "i4", "i5", "i6"},
			rightKeys:              []string{"k3", "k4", "k5", "k6", "k7"},
			rightIdentities:        []string{"i3", "i4", "i5", "i6", "i7"},
			expectedDiffKeys:       []string{"k1", "k2", "k7"},
			expectedDiffTypes:      []graveler.DiffType{removed, removed, added},
			expectedDiffIdentities: []string{"i1", "i2", "i7"},
		},
		{
			leftKeys:               []string{"k3", "k4", "k5"},
			leftIdentities:         []string{"i3", "i4", "i5"},
			rightKeys:              []string{"k1", "k2", "k3", "k4", "k5"},
			rightIdentities:        []string{"i1", "i2", "i3", "i4", "i5"},
			expectedDiffKeys:       []string{"k1", "k2"},
			expectedDiffTypes:      []graveler.DiffType{added, added},
			expectedDiffIdentities: []string{"i1", "i2"},
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
		}
	}
}

func TestDiffSeek(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	left := []string{"k1", "k2", "k4"}
	right := []string{"k1", "k3", "k4"}
	leftIdentities := []string{"i1", "i2", "i4"}
	rightIdentities := []string{"i1", "i3", "i4a"}
	diffTypeByKey := map[string]graveler.DiffType{"k2": removed, "k3": added, "k4": changed}
	diffIdentityByKey := map[string]string{"k2": "i2", "k3": "i3", "k4": "i4a"}

	it := graveler.NewDiffIterator(
		newMockValueIterator(newValues(left, leftIdentities)),
		newMockValueIterator(newValues(right, rightIdentities)))
	var diffs []*graveler.Diff
	tests := []struct {
		seekTo        string
		expectedDiffs []string
	}{
		{
			seekTo:        "k1",
			expectedDiffs: []string{"k2", "k3", "k4"},
		},
		{
			seekTo:        "k2",
			expectedDiffs: []string{"k2", "k3", "k4"},
		},
		{
			seekTo:        "k3",
			expectedDiffs: []string{"k3", "k4"},
		},
		{
			seekTo:        "k4",
			expectedDiffs: []string{"k4"},
		},
		{
			seekTo:        "k5",
			expectedDiffs: []string{},
		},
	}
	for _, tst := range tests {
		it.SeekGE([]byte(tst.seekTo))
		if it.Value() != nil {
			t.Fatalf("value expected to be nil after SeekGE. got=%v", it.Value())
		}
		idx := 0
		for it.Next() {
			key := it.Value().Key.String()
			if key != tst.expectedDiffs[idx] {
				t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", idx, tst.expectedDiffs[idx], key)
			}
			if it.Value().Type != diffTypeByKey[key] {
				t.Fatalf("unexpected diff type in index %d. expected=%d, got=%d", idx, diffTypeByKey[key], it.Value().Type)
			}
			if string(it.Value().Value.Identity) != diffIdentityByKey[key] {
				t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", idx, diffIdentityByKey[key], string(it.Value().Value.Identity))
			}
			diffs = append(diffs, it.Value())
			idx++
		}
		if idx != len(tst.expectedDiffs) {
			t.Fatalf("unexpected diff length. expected=%d, got=%d", len(tst.expectedDiffs), idx)
		}
	}
}

func TestDiffErr(t *testing.T) {
	leftErr := errors.New("error from left")
	leftIt := &mockValueIterator{
		current: -1,
		records: newValues([]string{"k1", "k2"}, []string{"i1", "i2"}),
		err:     leftErr,
	}
	rightIt := &mockValueIterator{
		current: -1,
		records: newValues([]string{"k2"}, []string{"i2a"}),
	}
	it := graveler.NewDiffIterator(leftIt, rightIt)
	if it.Next() {
		t.Fatalf("expected false from iterator with error")
	}
	if !errors.Is(it.Err(), leftErr) {
		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", leftErr, it.Err())
	}
	it.SeekGE([]byte("k2"))
	if it.Err() != nil {
		t.Fatalf("error expected to be nil after SeekGE. got=%v", it.Err())
	}
	if it.Next() {
		t.Fatalf("expected false from iterator with error")
	}
	if !errors.Is(it.Err(), leftErr) {
		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", leftErr, it.Err())
	}
	rightErr := errors.New("error from right")
	leftIt.err = nil
	rightIt.err = rightErr
	it.SeekGE([]byte("k2"))
	if it.Err() != nil {
		t.Fatalf("error expected to be nil after SeekGE. got=%v", it.Err())
	}
	if it.Next() {
		t.Fatalf("expected false from iterator with error")
	}
	if !errors.Is(it.Err(), rightErr) {
		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", rightErr, it.Err())
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
