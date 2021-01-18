package committed_test

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/testutil"
)

func TestDiff(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	tests := map[string]struct {
		leftKeys                  [][]string
		leftIdentities            [][]string
		rightKeys                 [][]string
		rightIdentities           [][]string
		expectedDiffKeys          []string
		expectedDiffTypes         []graveler.DiffType
		expectedDiffIdentities    []string
		expectedLeftReadsByRange  []int
		expectedRightReadsByRange []int
	}{
		"empty diff": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:          []string{},
			expectedLeftReadsByRange:  []int{0, 0},
			expectedRightReadsByRange: []int{0, 0},
		},
		"added in existing rng": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4"}},
			expectedDiffKeys:          []string{"k4"},
			expectedDiffTypes:         []graveler.DiffType{added},
			expectedDiffIdentities:    []string{"i4"},
			expectedLeftReadsByRange:  []int{0, 1},
			expectedRightReadsByRange: []int{0, 2},
		},
		"removed from existing rng": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3", "k4"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3", "i4"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:          []string{"k4"},
			expectedDiffTypes:         []graveler.DiffType{removed},
			expectedDiffIdentities:    []string{"i4"},
			expectedLeftReadsByRange:  []int{0, 2},
			expectedRightReadsByRange: []int{0, 1},
		},
		"added and removed": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3", "k5"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3", "i5"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4"}},
			expectedDiffKeys:          []string{"k4", "k5"},
			expectedDiffTypes:         []graveler.DiffType{added, removed},
			expectedDiffIdentities:    []string{"i4", "i5"},
			expectedLeftReadsByRange:  []int{0, 2},
			expectedRightReadsByRange: []int{0, 2},
		},
		"change in existing rng": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3a"}},
			expectedDiffKeys:          []string{"k3"},
			expectedDiffTypes:         []graveler.DiffType{changed},
			expectedDiffIdentities:    []string{"i3a"},
			expectedLeftReadsByRange:  []int{0, 1},
			expectedRightReadsByRange: []int{0, 1},
		},
		"ranges were split": {
			leftKeys:                  [][]string{{"k1", "k2", "k3"}},
			leftIdentities:            [][]string{{"i1", "i2", "i3"}},
			rightKeys:                 [][]string{{"k3", "k4"}, {"k5", "k6"}},
			rightIdentities:           [][]string{{"i3a", "i4"}, {"i5", "i6"}},
			expectedDiffKeys:          []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			expectedDiffTypes:         []graveler.DiffType{removed, removed, changed, added, added, added},
			expectedDiffIdentities:    []string{"i1", "i2", "i3a", "i4", "i5", "i6"},
			expectedLeftReadsByRange:  []int{3},
			expectedRightReadsByRange: []int{2, 2},
		},
		"diff between empty iterators": {
			expectedDiffKeys: []string{},
		},
		"added on empty": {
			leftKeys:                  [][]string{},
			leftIdentities:            [][]string{},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:          []string{"k1", "k2", "k3"},
			expectedDiffTypes:         []graveler.DiffType{added, added, added},
			expectedDiffIdentities:    []string{"i1", "i2", "i3"},
			expectedLeftReadsByRange:  nil,
			expectedRightReadsByRange: []int{2, 1},
		},
		"whole rng was replaced": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3", "k4", "k5", "k6"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3", "i4", "i5", "i6"}},
			rightKeys:                 [][]string{{"k3", "k4"}, {"k5", "k6", "k7"}},
			rightIdentities:           [][]string{{"i3", "i4"}, {"i5", "i6", "i7"}},
			expectedDiffKeys:          []string{"k1", "k2", "k7"},
			expectedDiffTypes:         []graveler.DiffType{removed, removed, added},
			expectedDiffIdentities:    []string{"i1", "i2", "i7"},
			expectedLeftReadsByRange:  []int{2, 4},
			expectedRightReadsByRange: []int{2, 3},
		},
		"added in beginning of rng": {
			leftKeys:                  [][]string{{"k3", "k4", "k5"}},
			leftIdentities:            [][]string{{"i3", "i4", "i5"}},
			rightKeys:                 [][]string{{"k1", "k2", "k3", "k4", "k5"}},
			rightIdentities:           [][]string{{"i1", "i2", "i3", "i4", "i5"}},
			expectedDiffKeys:          []string{"k1", "k2"},
			expectedDiffTypes:         []graveler.DiffType{added, added},
			expectedDiffIdentities:    []string{"i1", "i2"},
			expectedLeftReadsByRange:  []int{3},
			expectedRightReadsByRange: []int{5},
		},
		"small ranges removed": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}, {"k4"}, {"k5"}, {"k6", "k7"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}, {"i4"}, {"i5"}, {"i6", "i7"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k6", "k7"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i6", "i7"}},
			expectedDiffKeys:          []string{"k3", "k4", "k5"},
			expectedDiffTypes:         []graveler.DiffType{removed, removed, removed},
			expectedDiffIdentities:    []string{"i3", "i4", "i5"},
			expectedLeftReadsByRange:  []int{0, 1, 1, 1, 0},
			expectedRightReadsByRange: []int{0, 0},
		},
		"small ranges merged": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}, {"k4"}, {"k5"}, {"k6", "k7"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}, {"i4"}, {"i5"}, {"i6", "i7"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k4", "k5"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i4", "i5"}},
			expectedDiffKeys:          []string{"k3", "k6", "k7"},
			expectedDiffTypes:         []graveler.DiffType{removed, removed, removed},
			expectedDiffIdentities:    []string{"i3", "i6", "i7"},
			expectedLeftReadsByRange:  []int{0, 1, 1, 1, 2},
			expectedRightReadsByRange: []int{0, 2},
		},
		"empty ranges": {
			leftKeys:                  [][]string{{"k1", "k2"}, {}, {}, {}, {}, {"k3", "k4"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {}, {}, {}, {}, {"i3", "i4"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {}, {}, {"k3", "k4"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {}, {}, {"i3", "i4"}},
			expectedDiffKeys:          []string{},
			expectedDiffTypes:         []graveler.DiffType{},
			expectedDiffIdentities:    []string{},
			expectedLeftReadsByRange:  []int{0, 0, 0, 0, 0, 0},
			expectedRightReadsByRange: []int{0, 0, 0, 0},
		},
		"rng added in the middle": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k5", "k6"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i5", "i6"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}, {"k5", "k6"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4"}, {"i5", "i6"}},
			expectedDiffKeys:          []string{"k3", "k4"},
			expectedDiffTypes:         []graveler.DiffType{added, added},
			expectedDiffIdentities:    []string{"i3", "i4"},
			expectedLeftReadsByRange:  []int{0, 0},
			expectedRightReadsByRange: []int{0, 2, 0},
		},
		"identical ranges in the middle": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3", "k4"}, {"k5", "k6"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3", "i4"}, {"i5", "i6"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}, {"k5", "k6"}},
			rightIdentities:           [][]string{{"i1", "i2a"}, {"i3", "i4"}, {"i5", "i6a"}},
			expectedDiffKeys:          []string{"k2", "k6"},
			expectedDiffTypes:         []graveler.DiffType{changed, changed},
			expectedDiffIdentities:    []string{"i2a", "i6a"},
			expectedLeftReadsByRange:  []int{2, 0, 2},
			expectedRightReadsByRange: []int{2, 0, 2},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			fakeLeft := newFakeMetaRangeIterator(tst.leftKeys, tst.leftIdentities)
			fakeRight := newFakeMetaRangeIterator(tst.rightKeys, tst.rightIdentities)
			it := committed.NewDiffIterator(fakeLeft, fakeRight)
			defer it.Close()
			var diffs []graveler.Diff
			actualDiffKeys := make([]string, 0)
			for it.Next() {
				actualDiffKeys = append(actualDiffKeys, string(it.Value().Key()))
				diffs = append(diffs, it.Value())
			}
			if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedDiffKeys, actualDiffKeys); diff != nil {
				t.Fatalf("keys in diff different than expected. diff=%s", diff)
			}
			for i, d := range diffs {
				if d.Type() != tst.expectedDiffTypes[i] {
					t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key()))
				}
				if string(d.Value().Identity) != tst.expectedDiffIdentities[i] {
					t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", i, tst.expectedDiffIdentities[i], string(d.Value().Identity))
				}
			}
			if diff := deep.Equal(tst.expectedLeftReadsByRange, fakeLeft.ReadsByRange()); diff != nil {
				t.Fatalf("unexpected number of reads on left ranges. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedRightReadsByRange, fakeRight.ReadsByRange()); diff != nil {
				t.Fatalf("unexpected number of reads on right ranges. diff=%s", diff)
			}
		})
	}
}

func TestDiffSeek(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	left := [][]string{{"k1", "k2"}, {"k4", "k5"}, {"k6", "k7"}}
	right := [][]string{{"k1", "k3"}, {"k4", "k5"}, {"k6", "k7"}}
	leftIdentities := [][]string{{"i1", "i2"}, {"i4", "i5"}, {"i6", "i7"}}
	rightIdentities := [][]string{{"i1", "i3"}, {"i4", "i5"}, {"i6", "i7a"}}
	diffTypeByKey := map[string]graveler.DiffType{"k2": removed, "k3": added, "k7": changed}
	diffIdentityByKey := map[string]string{"k2": "i2", "k3": "i3", "k7": "i7a"}

	it := committed.NewDiffIterator(
		newFakeMetaRangeIterator(left, leftIdentities),
		newFakeMetaRangeIterator(right, rightIdentities),
	)
	defer it.Close()

	tests := []struct {
		seekTo           string
		expectedDiffKeys []string
	}{
		{
			seekTo:           "k1",
			expectedDiffKeys: []string{"k2", "k3", "k7"},
		},
		{
			seekTo:           "k2",
			expectedDiffKeys: []string{"k2", "k3", "k7"},
		},
		{
			seekTo:           "k3",
			expectedDiffKeys: []string{"k3", "k7"},
		},
		{
			seekTo:           "k4",
			expectedDiffKeys: []string{"k7"},
		},
		{
			seekTo:           "k8",
			expectedDiffKeys: []string{},
		},
	}
	for _, tst := range tests {
		it.SeekGE([]byte(tst.seekTo))
		if it.Value() != nil {
			t.Fatalf("value expected to be nil after SeekGE. got=%v", it.Value())
		}
		keys := make([]string, 0)
		for it.Next() {
			current := it.Value()
			key := current.Key().String()
			identity := string(current.Value().Identity)
			if current.Type() != diffTypeByKey[key] {
				t.Fatalf("unexpected diff type in index %d. expected=%d, got=%d", len(keys), diffTypeByKey[key], current.Type())
			}
			if identity != diffIdentityByKey[key] {
				t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", len(keys), diffIdentityByKey[key], identity)
			}
			keys = append(keys, key)
		}
		if diff := deep.Equal(keys, tst.expectedDiffKeys); diff != nil {
			t.Fatal("unexpected keys in diff", diff)
		}
	}
}

func TestNextOnClose(t *testing.T) {
	it := committed.NewDiffIterator(
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"i1", "i2"}}),
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"i1a", "i2a"}}))
	if !it.Next() {
		t.Fatal("expected iterator to have value")
	}
	it.Close()
	if it.Next() {
		t.Fatal("expected false from iterator after close")
	}
}

func TestDiffErr(t *testing.T) {
	leftErr := errors.New("error from left")
	leftIt := newFakeMetaRangeIterator([][]string{{"k1"}, {"k2"}}, [][]string{{"i1"}, {"i2"}})
	leftIt.SetErr(leftErr)
	rightIt := newFakeMetaRangeIterator([][]string{{"k2"}}, [][]string{{"i2a"}})
	it := committed.NewDiffIterator(leftIt, rightIt)
	defer it.Close()
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
	leftIt.SetErr(nil)
	rightIt.SetErr(rightErr)
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

func newFakeMetaRangeIterator(rangeKeys [][]string, rangeIdentities [][]string) *testutil.FakeIterator {
	res := testutil.NewFakeIterator()
	for rangeIdx, keys := range rangeKeys {
		identities := rangeIdentities[rangeIdx]
		var b bytes.Buffer
		encoder := gob.NewEncoder(&b)
		_ = encoder.Encode(rangeKeys[rangeIdx])
		_ = encoder.Encode(rangeIdentities[rangeIdx])
		rangeID := hex.EncodeToString(b.Bytes())
		var minKey, maxKey committed.Key
		if len(rangeKeys[rangeIdx]) > 0 {
			minKey = []byte(rangeKeys[rangeIdx][0])
			maxKey = []byte(rangeKeys[rangeIdx][len(rangeKeys[rangeIdx])-1])
		}
		res.AddRange(&committed.Range{ID: committed.ID(rangeID), MinKey: minKey, MaxKey: maxKey})
		rangeValues := make([]*graveler.ValueRecord, 0, len(rangeKeys[rangeIdx]))
		for idx := range keys {
			rangeValues = append(rangeValues, &graveler.ValueRecord{
				Key: []byte(keys[idx]),
				Value: &graveler.Value{
					Identity: []byte(identities[idx]),
					Data:     []byte("some-data"),
				},
			})
		}
		res.AddValueRecords(rangeValues...)
	}
	return res
}
