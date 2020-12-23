package tree_test

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/tree"
)

func TestDiff(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	tests := []struct {
		leftKeys                 [][]string
		leftIdentities           [][]string
		rightKeys                [][]string
		rightIdentities          [][]string
		expectedDiffKeys         []string
		expectedDiffTypes        []graveler.DiffType
		expectedDiffIdentities   []string
		expectedLeftReadsByPart  []int
		expectedRightReadsByPart []int
	}{
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:         []string{},
			expectedLeftReadsByPart:  []int{0, 0},
			expectedRightReadsByPart: []int{0, 0},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3", "k4"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3", "i4"}},
			expectedDiffKeys:         []string{"k4"},
			expectedDiffTypes:        []graveler.DiffType{added},
			expectedDiffIdentities:   []string{"i4"},
			expectedLeftReadsByPart:  []int{0, 1},
			expectedRightReadsByPart: []int{0, 2},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:         []string{"k4"},
			expectedDiffTypes:        []graveler.DiffType{removed},
			expectedDiffIdentities:   []string{"i4"},
			expectedLeftReadsByPart:  []int{0, 2},
			expectedRightReadsByPart: []int{0, 1},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3", "k5"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3", "i5"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3", "k4"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3", "i4"}},
			expectedDiffKeys:         []string{"k4", "k5"},
			expectedDiffTypes:        []graveler.DiffType{added, removed},
			expectedDiffIdentities:   []string{"i4", "i5"},
			expectedLeftReadsByPart:  []int{0, 2},
			expectedRightReadsByPart: []int{0, 2},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3a"}},
			expectedDiffKeys:         []string{"k3"},
			expectedDiffTypes:        []graveler.DiffType{changed},
			expectedDiffIdentities:   []string{"i3a"},
			expectedLeftReadsByPart:  []int{0, 1},
			expectedRightReadsByPart: []int{0, 1},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2", "k3"}},
			leftIdentities:           [][]string{{"i1", "i2", "i3"}},
			rightKeys:                [][]string{{"k3", "k4"}, {"k5", "k6"}},
			rightIdentities:          [][]string{{"i3a", "i4"}, {"i5", "i6"}},
			expectedDiffKeys:         []string{"k1", "k2", "k3", "k4", "k5", "k6"},
			expectedDiffTypes:        []graveler.DiffType{removed, removed, changed, added, added, added},
			expectedDiffIdentities:   []string{"i1", "i2", "i3a", "i4", "i5", "i6"},
			expectedLeftReadsByPart:  []int{3},
			expectedRightReadsByPart: []int{2, 2},
		},
		{
			// diff between two empty iterators - should return empty diff
			expectedDiffKeys:         []string{},
			expectedLeftReadsByPart:  []int{},
			expectedRightReadsByPart: []int{},
		},
		{
			leftKeys:                 [][]string{},
			leftIdentities:           [][]string{},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:         []string{"k1", "k2", "k3"},
			expectedDiffTypes:        []graveler.DiffType{added, added, added},
			expectedDiffIdentities:   []string{"i1", "i2", "i3"},
			expectedLeftReadsByPart:  []int{},
			expectedRightReadsByPart: []int{2, 1},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4", "k5", "k6"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4", "i5", "i6"}},
			rightKeys:                [][]string{{"k3", "k4"}, {"k5", "k6", "k7"}},
			rightIdentities:          [][]string{{"i3", "i4"}, {"i5", "i6", "i7"}},
			expectedDiffKeys:         []string{"k1", "k2", "k7"},
			expectedDiffTypes:        []graveler.DiffType{removed, removed, added},
			expectedDiffIdentities:   []string{"i1", "i2", "i7"},
			expectedLeftReadsByPart:  []int{2, 4},
			expectedRightReadsByPart: []int{2, 3},
		},
		{
			leftKeys:                 [][]string{{"k3", "k4", "k5"}},
			leftIdentities:           [][]string{{"i3", "i4", "i5"}},
			rightKeys:                [][]string{{"k1", "k2", "k3", "k4", "k5"}},
			rightIdentities:          [][]string{{"i1", "i2", "i3", "i4", "i5"}},
			expectedDiffKeys:         []string{"k1", "k2"},
			expectedDiffTypes:        []graveler.DiffType{added, added},
			expectedDiffIdentities:   []string{"i1", "i2"},
			expectedLeftReadsByPart:  []int{3},
			expectedRightReadsByPart: []int{5},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3"}, {"k4"}, {"k5"}, {"k6", "k7"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3"}, {"i4"}, {"i5"}, {"i6", "i7"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k6", "k7"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i6", "i7"}},
			expectedDiffKeys:         []string{"k3", "k4", "k5"},
			expectedDiffTypes:        []graveler.DiffType{removed, removed, removed},
			expectedDiffIdentities:   []string{"i3", "i4", "i5"},
			expectedLeftReadsByPart:  []int{0, 1, 1, 1, 0},
			expectedRightReadsByPart: []int{0, 0},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k3"}, {"k4"}, {"k5"}, {"k6", "k7"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i3"}, {"i4"}, {"i5"}, {"i6", "i7"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k4", "k5"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i4", "i5"}},
			expectedDiffKeys:         []string{"k3", "k6", "k7"},
			expectedDiffTypes:        []graveler.DiffType{removed, removed, removed},
			expectedDiffIdentities:   []string{"i3", "i6", "i7"},
			expectedLeftReadsByPart:  []int{0, 1, 1, 1, 2},
			expectedRightReadsByPart: []int{0, 2},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {}, {}, {}, {}, {"k3", "k4"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {}, {}, {}, {}, {"i3", "i4"}},
			rightKeys:                [][]string{{"k1", "k2"}, {}, {}, {"k3", "k4"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {}, {}, {"i3", "i4"}},
			expectedDiffKeys:         []string{},
			expectedDiffTypes:        []graveler.DiffType{},
			expectedDiffIdentities:   []string{},
			expectedLeftReadsByPart:  []int{0, 0, 0, 0, 0, 0},
			expectedRightReadsByPart: []int{0, 0, 0, 0},
		},
		{
			leftKeys:                 [][]string{{"k1", "k2"}, {"k5", "k6"}},
			leftIdentities:           [][]string{{"i1", "i2"}, {"i5", "i6"}},
			rightKeys:                [][]string{{"k1", "k2"}, {"k3", "k4"}, {"k5", "k6"}},
			rightIdentities:          [][]string{{"i1", "i2"}, {"i3", "i4"}, {"i5", "i6"}},
			expectedDiffKeys:         []string{"k3", "k4"},
			expectedDiffTypes:        []graveler.DiffType{added, added},
			expectedDiffIdentities:   []string{"i3", "i4"},
			expectedLeftReadsByPart:  []int{0, 0},
			expectedRightReadsByPart: []int{0, 2, 0},
		},
	}
	for i, tst := range tests {
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			fakeLeft := newFakeTreeIterator(tst.leftKeys, tst.leftIdentities)
			fakeRight := newFakeTreeIterator(tst.rightKeys, tst.rightIdentities)
			it := tree.NewDiffIterator(fakeLeft, fakeRight)
			var diffs []*graveler.Diff
			actualDiffKeys := make([]string, 0)
			for it.Next() {
				actualDiffKeys = append(actualDiffKeys, string(it.Value().Key))
				diffs = append(diffs, it.Value())
			}
			if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedDiffKeys, actualDiffKeys); diff != nil {
				t.Fatalf("keys in diff different than expected. diff=%s", diff)
			}
			for i, d := range diffs {
				if d.Type != tst.expectedDiffTypes[i] {
					t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key))
				}
				if string(d.Value.Identity) != tst.expectedDiffIdentities[i] {
					t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", i, tst.expectedDiffIdentities[i], string(d.Value.Identity))
				}
			}
			if diff := deep.Equal(tst.expectedLeftReadsByPart, fakeLeft.readsByPart); diff != nil {
				t.Fatalf("unexpected number of reads on left parts. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedRightReadsByPart, fakeRight.readsByPart); diff != nil {
				t.Fatalf("unexpected number of reads on right parts. diff=%s", diff)
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

	it := tree.NewDiffIterator(
		newFakeTreeIterator(left, leftIdentities),
		newFakeTreeIterator(right, rightIdentities))
	var diffs []*graveler.Diff
	tests := []struct {
		seekTo        string
		expectedDiffs []string
	}{
		{
			seekTo:        "k1",
			expectedDiffs: []string{"k2", "k3", "k7"},
		},
		{
			seekTo:        "k2",
			expectedDiffs: []string{"k2", "k3", "k7"},
		},
		{
			seekTo:        "k3",
			expectedDiffs: []string{"k3", "k7"},
		},
		{
			seekTo:        "k4",
			expectedDiffs: []string{"k7"},
		},
		{
			seekTo:        "k8",
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

//func TestDiffErr(t *testing.T) {
//	leftErr := errors.New("error from left")
//	leftIt := testutil.NewValueIteratorFake(newValues([]string{"k1", "k2"}, []string{"i1", "i2"}))
//	leftIt.SetErr(leftErr)
//	rightIt := testutil.NewValueIteratorFake(newValues([]string{"k2"}, []string{"i2a"}))
//	it := committed.NewDiffIterator(leftIt, rightIt)
//	if it.Next() {
//		t.Fatalf("expected false from iterator with error")
//	}
//	if !errors.Is(it.Err(), leftErr) {
//		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", leftErr, it.Err())
//	}
//	it.SeekGE([]byte("k2"))
//	if it.Err() != nil {
//		t.Fatalf("error expected to be nil after SeekGE. got=%v", it.Err())
//	}
//	if it.Next() {
//		t.Fatalf("expected false from iterator with error")
//	}
//	if !errors.Is(it.Err(), leftErr) {
//		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", leftErr, it.Err())
//	}
//	rightErr := errors.New("error from right")
//	leftIt.SetErr(nil)
//	rightIt.SetErr(rightErr)
//	it.SeekGE([]byte("k2"))
//	if it.Err() != nil {
//		t.Fatalf("error expected to be nil after SeekGE. got=%v", it.Err())
//	}
//	if it.Next() {
//		t.Fatalf("expected false from iterator with error")
//	}
//	if !errors.Is(it.Err(), rightErr) {
//		t.Fatalf("unexpected error from iterator. expected=%v, got=%v", rightErr, it.Err())
//	}
//}

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

type fakeTreeIterator struct {
	parts       [][]*graveler.ValueRecord
	readsByPart []int
	currentPart int
	currentIdx  int
	afterSeek   bool
}

func newFakeTreeIterator(partKeys [][]string, partIdentities [][]string) *fakeTreeIterator {
	parts := make([][]*graveler.ValueRecord, len(partKeys))
	for partIdx, keys := range partKeys {
		identities := partIdentities[partIdx]
		for idx := range keys {
			parts[partIdx] = append(parts[partIdx], &graveler.ValueRecord{
				Key: []byte(keys[idx]),
				Value: &graveler.Value{
					Identity: []byte(identities[idx]),
					Data:     []byte("some-data"),
				},
			})
		}
	}
	return &fakeTreeIterator{parts: parts, currentIdx: -1, currentPart: -1, readsByPart: make([]int, len(partKeys))}
}

func (f *fakeTreeIterator) Next() bool {
	if f.afterSeek {
		f.afterSeek = false
		return true
	}
	if f.currentPart == len(f.parts) {
		return false
	}
	f.currentIdx++
	if f.currentPart > -1 && f.currentIdx < len(f.parts[f.currentPart]) {
		f.readsByPart[f.currentPart]++
		return true
	}
	return f.NextPart()
}

func (f *fakeTreeIterator) NextPart() bool {
	f.currentIdx = -1
	f.currentPart++
	return f.currentPart < len(f.parts)
}

func (f *fakeTreeIterator) Value() (*graveler.ValueRecord, *tree.Part) {
	var b bytes.Buffer
	_ = gob.NewEncoder(&b).Encode(f.parts[f.currentPart])
	partName := hex.EncodeToString(b.Bytes())
	var minKey, maxKey graveler.Key
	if len(f.parts[f.currentPart]) > 0 {
		minKey = f.parts[f.currentPart][0].Key
		maxKey = f.parts[f.currentPart][len(f.parts[f.currentPart])-1].Key
	}
	part := &tree.Part{
		Name:   committed.ID(partName),
		MinKey: minKey,
		MaxKey: maxKey,
	}
	if f.currentIdx == -1 {
		return nil, part
	}
	return f.parts[f.currentPart][f.currentIdx], part
}

func (f *fakeTreeIterator) SeekGE(id graveler.Key) {
	f.currentIdx = -1
	f.currentPart = 0
	for f.Next() {
		val, part := f.Value()
		var key = part.MinKey
		if val != nil {
			key = val.Key
		}
		if key == nil {
			continue
		}
		if bytes.Compare(key, id) >= 0 {
			f.afterSeek = true
			return
		}
	}
}

func (f *fakeTreeIterator) Err() error {
	return nil
}

func (f *fakeTreeIterator) Close() {
	f.currentPart = len(f.parts)
	f.currentIdx = -1
}
