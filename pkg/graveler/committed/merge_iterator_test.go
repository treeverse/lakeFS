package committed_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

var baseKeyToIdentity = map[string]string{"k1": "i1", "k2": "i2", "k3": "i3", "k4": "i4", "k6": "i6"}

const (
	added    = graveler.DiffTypeAdded
	removed  = graveler.DiffTypeRemoved
	changed  = graveler.DiffTypeChanged
	conflict = graveler.DiffTypeConflict
)

func testMergeNewDiff(typ graveler.DiffType, key string, newIdentity string, oldIdentity string) graveler.Diff {
	return graveler.Diff{
		Type:         typ,
		Key:          graveler.Key(key),
		Value:        &graveler.Value{Identity: []byte(newIdentity)},
		LeftIdentity: []byte(oldIdentity),
	}
}

func TestMerge(t *testing.T) {
	tests := map[string]struct {
		baseKeys            []string
		diffs               []graveler.Diff
		conflictExpectedIdx int
		expectedKeys        []string
		expectedIdentities  []string
	}{
		"added on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k3", "i3", "")},
			conflictExpectedIdx: -1,
			expectedKeys:        []string{"k3"},
			expectedIdentities:  []string{"i3"},
		},
		"changed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2")},
			conflictExpectedIdx: -1,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{"i2a"},
		},
		"deleted on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: -1,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{""},
		},
		"added on left": {
			baseKeys:            []string{"k1"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: -1,
			expectedIdentities:  nil,
		},
		"removed on left": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2", "")},
			conflictExpectedIdx: -1,
			expectedIdentities:  nil,
		},
		"changed on left": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2", "i2a")},
			conflictExpectedIdx: -1,
			expectedIdentities:  nil,
		},
		"changed on both": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2b", "i2a")},
			conflictExpectedIdx: 0,
		},
		"changed on left, removed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2a", "i2a")},
			conflictExpectedIdx: 0,
		},
		"removed on left, changed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2a", "")},
			conflictExpectedIdx: 0,
		},
		"added on both with different identities": {
			baseKeys:            []string{"k1"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2b")},
			conflictExpectedIdx: 0,
		},
		"multiple add and removes": {
			baseKeys: []string{"k1", "k3"},
			diffs: []graveler.Diff{testMergeNewDiff(removed, "k1", "i1", "i1"),
				testMergeNewDiff(added, "k2", "i2", ""),
				testMergeNewDiff(removed, "k3", "i3", "i3"),
				testMergeNewDiff(added, "k4", "i4", "")},
			expectedKeys:        []string{"k1", "k2", "k3", "k4"},
			expectedIdentities:  []string{"", "i2", "", "i4"},
			conflictExpectedIdx: -1,
		},
		"changes on each side": {
			baseKeys: []string{"k1", "k2", "k3", "k4"},
			diffs: []graveler.Diff{testMergeNewDiff(changed, "k1", "i1a", "i1"),
				testMergeNewDiff(changed, "k2", "i2", "i2a"),
				testMergeNewDiff(changed, "k3", "i3a", "i3"),
				testMergeNewDiff(changed, "k4", "i4", "i4a")},
			expectedKeys:        []string{"k1", "k3"},
			expectedIdentities:  []string{"i1a", "i3a"},
			conflictExpectedIdx: -1,
		},
	}

	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			diffIt := testutil.NewDiffIter(tst.diffs)
			defer diffIt.Close()
			base := makeBaseIterator(tst.baseKeys)
			ctx := context.Background()
			it := committed.NewMergeIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), base)
			var gotValues, gotKeys []string
			idx := 0
			for it.Next() {
				idx++
				val, _ := it.Value()
				gotKeys = append(gotKeys, string(val.Key))
				if val.Value == nil {
					gotValues = append(gotValues, "")
				} else {
					gotValues = append(gotValues, string(val.Identity))
				}
			}
			if tst.conflictExpectedIdx != -1 {
				if !errors.Is(it.Err(), graveler.ErrConflictFound) {
					t.Fatalf("expected conflict but didn't get one. err=%v", it.Err())
				}
				if tst.conflictExpectedIdx != idx {
					t.Fatalf("got conflict at unexpected index. expected at=%d, got at=%d", tst.conflictExpectedIdx, idx)
				}
			} else if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from merge iterator. diff=%s", diff)
			}
		})
	}
}

func makeDV(typ graveler.DiffType, k, id, leftID string) *graveler.Diff {
	res := &graveler.Diff{
		Type:         typ,
		Key:          graveler.Key(k),
		Value:        &graveler.Value{Identity: []byte(id)},
		LeftIdentity: []byte(leftID),
	}

	return res
}

func getMaxKey(keys []string) committed.Key {
	if len(keys) == 0 {
		return nil
	}
	return committed.Key(keys[len(keys)-1])
}

func getMinKey(keys []string) committed.Key {
	if len(keys) == 0 {
		return nil
	}
	return committed.Key(keys[0])
}

func getRecordKey(record *graveler.ValueRecord) string {
	if record == nil {
		return ""
	}
	return string(record.Key)
}

func getRecordID(record *graveler.ValueRecord) string {
	if record == nil || record.Value == nil {
		return ""
	}
	return string(record.Identity)
}

func getRangeID(rng *committed.Range) string {
	if rng == nil {
		return ""
	}
	return string(rng.ID)
}

func TestMergeRange(t *testing.T) {
	tests := map[string]struct {
		baseRangeIds        []string
		baseKeys            [][]string
		baseIdentities      [][]string
		diffRangeIds        []string
		diffRangeTypes      []graveler.DiffType
		diffKeys            [][]string
		diffIdentities      [][]string
		leftIdentities      [][]string
		expectedRanges      []string
		expectedValues      []string
		expectedIDs         []string
		expectedReadByRange []int
		conflictExpectedIdx int
	}{
		"diff added - no base": {
			baseRangeIds:        []string{},
			baseKeys:            [][]string{},
			baseIdentities:      [][]string{},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"", "k3", "k4"},
			expectedIDs:         []string{"", "i3", "i4"},
			conflictExpectedIdx: -1,
		},
		"diff added - before base": {
			baseRangeIds:        []string{"base:k5-k6"},
			baseKeys:            [][]string{{"k5", "k6"}},
			baseIdentities:      [][]string{{"i5", "i6"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"", "k3", "k4"},
			expectedIDs:         []string{"", "i3", "i4"},
			conflictExpectedIdx: -1,
		},
		"diff changed - before base (conflict)": {
			baseRangeIds:        []string{"base:k5-k6"},
			baseKeys:            [][]string{{"k5", "k6"}},
			baseIdentities:      [][]string{{"i5", "i6"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{changed},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"i3", "i4"}},
			expectedRanges:      []string{},
			expectedValues:      []string{},
			expectedIDs:         []string{},
			conflictExpectedIdx: 0,
		},
		"diff added - wrapping base": {
			baseRangeIds:        []string{"base:k1-k5"},
			baseKeys:            [][]string{{"k1", "k5"}},
			baseIdentities:      [][]string{{"i1", "i5"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"", "k3", "k4"},
			expectedIDs:         []string{"", "i3", "i4"},
			conflictExpectedIdx: -1,
		},
		"diff added - wrapping base 2": {
			baseRangeIds:        []string{"base:k1-k6"},
			baseKeys:            [][]string{{"k1", "k6"}},
			baseIdentities:      [][]string{{"i1", "i6"}},
			diffRangeIds:        []string{"source:k2-k3", "source:k4-k5"},
			diffRangeTypes:      []graveler.DiffType{added, added},
			diffKeys:            [][]string{{"k2", "k3"}, {"k4", "k5"}},
			diffIdentities:      [][]string{{"i2", "i3"}, {"i4", "i5"}},
			leftIdentities:      [][]string{{"", ""}, {"", ""}},
			expectedRanges:      []string{"source:k2-k3", "source:k2-k3", "source:k2-k3", "source:k4-k5", "source:k4-k5", "source:k4-k5"},
			expectedValues:      []string{"", "k2", "k3", "", "k4", "k5"},
			expectedIDs:         []string{"", "i2", "i3", "", "i4", "i5"},
			conflictExpectedIdx: -1,
		},
		"diff added - overlapping base": {
			baseRangeIds:        []string{"base:k1-k33"},
			baseKeys:            [][]string{{"k1", "k33"}},
			baseIdentities:      [][]string{{"i1", "i33"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"k3", "k4"},
			expectedIDs:         []string{"i3", "i4"},
			conflictExpectedIdx: -1,
		},
		"diff removed - no base (added by dest) ": {
			baseRangeIds:        []string{},
			baseKeys:            [][]string{},
			baseIdentities:      [][]string{},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{removed},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"i3", "i4"}},
			expectedRanges:      []string{},
			expectedValues:      []string{},
			expectedIDs:         []string{},
			conflictExpectedIdx: -1,
		},
		"diff removed` - before base": {
			baseRangeIds:        []string{"base:k5-k6"},
			baseKeys:            [][]string{{"k5", "k6"}},
			baseIdentities:      [][]string{{"i5", "i6"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{removed},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{},
			expectedValues:      []string{},
			expectedIDs:         []string{},
			conflictExpectedIdx: -1,
		},
		"diff removed` - equal to base - removed": {
			baseRangeIds:        []string{"source:k3-k4"},
			baseKeys:            [][]string{{"k3", "k4"}},
			baseIdentities:      [][]string{{"i3", "i4"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{removed},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"i3", "i4"}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"", "k3", "k4"},
			expectedIDs:         []string{"", "", ""},
			conflictExpectedIdx: -1,
		},
		"diff removed` - overlapping base": {
			baseRangeIds:        []string{"source:k1-k4"},
			baseKeys:            [][]string{{"k1", "k3", "k33", "k4"}},
			baseIdentities:      [][]string{{"i1", "i3", "i33", "i4"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{removed},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"i3", "i4"}},
			expectedRanges:      []string{"source:k3-k4", "source:k3-k4"},
			expectedValues:      []string{"k3", "k4"},
			expectedIDs:         []string{"", ""},
			conflictExpectedIdx: -1,
		},
		"diff with no range added - no base": {
			baseRangeIds:        []string{},
			baseKeys:            [][]string{},
			baseIdentities:      [][]string{},
			diffRangeIds:        []string{""},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"i3", "i4"}},
			expectedRanges:      []string{"", ""},
			expectedValues:      []string{"k3", "k4"},
			expectedIDs:         []string{"i3", "i4"},
			conflictExpectedIdx: -1,
		},
		"diff added - equal to base (removed from source)": {
			baseRangeIds:        []string{"source:k3-k4"},
			baseKeys:            [][]string{{"k3", "k4"}},
			baseIdentities:      [][]string{{"i3", "i4"}},
			diffRangeIds:        []string{"source:k3-k4"},
			diffRangeTypes:      []graveler.DiffType{added},
			diffKeys:            [][]string{{"k3", "k4"}},
			diffIdentities:      [][]string{{"i3", "i4"}},
			leftIdentities:      [][]string{{"", ""}},
			expectedRanges:      []string{},
			expectedValues:      []string{},
			expectedIDs:         []string{},
			conflictExpectedIdx: -1,
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			// build base iterator
			if !(len(tst.baseRangeIds) == len(tst.baseIdentities) && len(tst.baseIdentities) == len(tst.baseKeys)) {
				t.Fatal("base lists should be all the same length")
			}
			baseIt := testutil.NewFakeIterator()
			for i, baseRange := range tst.baseRangeIds {
				var rng *committed.Range
				if baseRange != "" {
					rng = &committed.Range{
						ID:     committed.ID(baseRange),
						MinKey: getMinKey(tst.baseKeys[i]),
						MaxKey: getMaxKey(tst.baseKeys[i]),
						Count:  int64(len(tst.baseKeys)),
					}
				}
				records := make([]*graveler.ValueRecord, 0)
				for j, key := range tst.baseKeys[i] {
					records = append(records, makeV(key, tst.baseIdentities[i][j]))
				}
				baseIt.AddRange(rng).
					AddValueRecords(records...)
			}
			// build diff iterator
			diffIt := testutil.NewFakeDiffIterator()
			for i, diffRange := range tst.diffRangeIds {
				var rng *committed.RangeDiff
				if diffRange != "" {
					rng = &committed.RangeDiff{
						Type: tst.diffRangeTypes[i],
						Range: &committed.Range{
							ID:     committed.ID(diffRange),
							MinKey: getMinKey(tst.diffKeys[i]),
							MaxKey: getMaxKey(tst.diffKeys[i]),
							Count:  int64(len(tst.diffKeys[i])),
						},
					}
				}
				records := make([]*graveler.Diff, 0)
				for j, key := range tst.diffKeys[i] {
					records = append(records, makeDV(tst.diffRangeTypes[i], key, tst.diffIdentities[i][j], tst.leftIdentities[i][j]))
				}
				diffIt.AddRange(rng).
					AddValueRecords(records...)
			}

			// test merge iterator
			ctx := context.Background()
			it := committed.NewMergeIterator(ctx, diffIt, baseIt)
			gotKeys := make([]string, 0)
			gotIDs := make([]string, 0)
			gotRangesIDs := make([]string, 0)
			idx := 0
			for it.Next() {
				idx++
				val, rng := it.Value()
				gotKeys = append(gotKeys, getRecordKey(val))
				gotIDs = append(gotIDs, getRecordID(val))
				gotRangesIDs = append(gotRangesIDs, getRangeID(rng))
			}
			if tst.conflictExpectedIdx != -1 {
				if !errors.Is(it.Err(), graveler.ErrConflictFound) {
					t.Fatalf("expected conflict but didn't get one. err=%v", it.Err())
				}
				if tst.conflictExpectedIdx != idx {
					t.Fatalf("got conflict at unexpected index. expected at=%d, got at=%d", tst.conflictExpectedIdx, idx)
				}
			} else if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}

			if diff := deep.Equal(tst.expectedRanges, gotRangesIDs); diff != nil {
				t.Errorf("got unexpected ranges from compare iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedValues, gotKeys); diff != nil {
				t.Errorf("got unexpected values from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIDs, gotIDs); diff != nil {
				t.Errorf("got unexpected values from merge iterator. diff=%s", diff)
			}
		})
	}
}
func TestMergeNextRange(t *testing.T) {
	baseIt := testutil.NewFakeIterator()

	diffIt := testutil.NewFakeDiffIterator()

	rangeDiff1 := &committed.RangeDiff{
		Type: added,
		Range: &committed.Range{
			ID:     "diff:k1-k3",
			MinKey: committed.Key("k1"),
			MaxKey: committed.Key("k3"),
			Count:  2,
		},
	}
	diffIt.AddRange(rangeDiff1)

	diffIt.AddValueRecords(makeDV(added, "k1", "i1", ""), makeDV(added, "k3", "i3", ""))

	rangeDiff2 := &committed.RangeDiff{
		Type: added,
		Range: &committed.Range{
			ID:     "diff:k4-k5",
			MinKey: committed.Key("k4"),
			MaxKey: committed.Key("k5"),
			Count:  2,
		},
	}
	diffIt.AddRange(rangeDiff2)

	diffIt.AddValueRecords(makeDV(added, "k4", "i4", ""), makeDV(added, "k5", "i5", ""))

	ctx := context.Background()
	it := committed.NewMergeIterator(ctx, diffIt, baseIt)

	if !it.Next() {
		t.Fatalf("expected it.Next() to return true (error:%v)", it.Err())
	}
	val, rng := it.Value()
	if diff := deep.Equal(rng, rangeDiff1.Range); diff != nil {
		t.Errorf("got unexpected ranges from compare iterator. diff=%s", diff)
	}
	if val != nil {
		t.Errorf("expected val to be nil got %v", val)
	}

	if !it.NextRange() {
		t.Fatalf("expected it.NextRange() to return true (error:%v)", it.Err())
	}
	val, rng = it.Value()
	if diff := deep.Equal(rng, rangeDiff2.Range); diff != nil {
		t.Errorf("got unexpected ranges from compare iterator. diff=%s", diff)
	}
	if val != nil {
		t.Errorf("expected val to be nil got %v", val)
	}

	if !it.Next() {
		t.Fatalf("expected it.Next() to return true (error:%v)", it.Err())
	}
	val, rng = it.Value()
	if diff := deep.Equal(rng, rangeDiff2.Range); diff != nil {
		t.Errorf("got unexpected ranges from compare iterator. diff=%s", diff)
	}
	if val == nil || string(val.Identity) != "i4" {
		t.Errorf("expected identity to be i4 got %v", val)
	}
	if it.NextRange() {
		t.Fatal("expected it.NextRange() to return false")
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestMergeCancelContext(t *testing.T) {
	diffs := []graveler.Diff{testMergeNewDiff(added, "k3", "i3", "")}
	diffIt := testutil.NewDiffIter(diffs)
	defer diffIt.Close()
	baseKeys := []string{"k1", "k2"}
	base := makeBaseIterator(baseKeys)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it := committed.NewMergeIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), base)
	if it.Next() {
		t.Fatal("Next() should return false")
	}
	if err := it.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() returned %v, expected context.Canceled", err)
	}
}

func TestMergeSeek(t *testing.T) {
	diffs := []graveler.Diff{
		testMergeNewDiff(added, "k1", "i1", ""),
		testMergeNewDiff(removed, "k2", "i2", "i2"),
		testMergeNewDiff(changed, "k3", "i3a", "i3"),
		testMergeNewDiff(added, "k4", "i4", ""),
		testMergeNewDiff(removed, "k5", "i5", "i5"),
		testMergeNewDiff(changed, "k6", "i6", "i6a"),
		testMergeNewDiff(added, "k7", "i7", ""),
		testMergeNewDiff(removed, "k8", "i8", "i8"),
		testMergeNewDiff(changed, "k9", "i9a", "i9"),
	}
	diffIt := testutil.NewDiffIter(diffs)
	baseKeys := []string{"k2", "k3", "k4", "k6"}
	base := makeBaseIterator(baseKeys)
	ctx := context.Background()
	it := committed.NewMergeIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), base)
	// expected diffs, +k1, -k2, Chng:k3,+k7, Conf:k9,
	defer it.Close()
	tests := []struct {
		seekTo              string
		expectedKeys        []string
		expectedIdentities  []string
		conflictExpectedIdx int
	}{
		{
			seekTo:              "k1",
			expectedKeys:        []string{"k1", "k2", "k3", "k7"},
			expectedIdentities:  []string{"i1", "", "i3a", "i7"},
			conflictExpectedIdx: 4,
		},
		{
			seekTo:              "k2",
			expectedKeys:        []string{"k2", "k3", "k7"},
			expectedIdentities:  []string{"", "i3a", "i7"},
			conflictExpectedIdx: 3,
		},

		{
			seekTo:              "k3",
			expectedKeys:        []string{"k3", "k7"},
			expectedIdentities:  []string{"i3a", "i7"},
			conflictExpectedIdx: 2,
		},

		{
			seekTo:              "k4",
			expectedKeys:        []string{"k7"},
			expectedIdentities:  []string{"i7"},
			conflictExpectedIdx: 1,
		},
		{
			seekTo:              "k8",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: 0,
		},
		{
			seekTo:              "k9",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: 0,
		},
		{
			seekTo:              "k99",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: -1,
		},
	}
	for _, tst := range tests {
		t.Run(fmt.Sprintf("seek to %s", tst.seekTo), func(t *testing.T) {
			it.SeekGE([]byte(tst.seekTo))
			val, _ := it.Value()
			if val != nil {
				t.Fatalf("value expected to be nil after SeekGE. got=%v", val)
			}
			idx := 0
			var gotValues, gotKeys []string
			for it.Next() {
				val, _ := it.Value()
				idx++
				gotKeys = append(gotKeys, string(val.Key))
				if val.Value == nil {
					gotValues = append(gotValues, "")
				} else {
					gotValues = append(gotValues, string(val.Identity))
				}
			}
			if tst.conflictExpectedIdx != -1 {
				if !errors.Is(it.Err(), graveler.ErrConflictFound) {
					t.Fatalf("expected conflict but didn't get one. err=%v", it.Err())
				}
				if tst.conflictExpectedIdx != idx {
					t.Fatalf("got conflict at unexpected index. expected at=%d, got at=%d", tst.conflictExpectedIdx, idx)
				}
			} else if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from merge iterator. diff=%s", diff)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	tests := map[string]struct {
		baseKeys           []string
		diffs              []graveler.Diff
		expectedKeys       []string
		expectedIdentities []string
		expectedDiffTypes  []graveler.DiffType
	}{
		"added on right": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(added, "k3", "i3", "")},
			expectedKeys:       []string{"k3"},
			expectedIdentities: []string{"i3"},
			expectedDiffTypes:  []graveler.DiffType{added},
		},
		"changed on right": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2")},
			expectedKeys:       []string{"k2"},
			expectedIdentities: []string{"i2a"},
			expectedDiffTypes:  []graveler.DiffType{changed},
		},
		"deleted on right": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			expectedKeys:       []string{"k2"},
			expectedIdentities: []string{"i2"},
			expectedDiffTypes:  []graveler.DiffType{removed},
		},
		"added on left": {
			baseKeys:           []string{"k1"},
			diffs:              []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			expectedIdentities: nil,
		},
		"removed on left": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(added, "k2", "i2", "")},
			expectedIdentities: nil,
		},
		"changed on left": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(changed, "k2", "i2", "i2a")},
			expectedIdentities: nil,
		},
		"changed on both": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(changed, "k2", "i2b", "i2a")},
			expectedKeys:       []string{"k2"},
			expectedIdentities: []string{"i2b"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		"changed on left, removed on right": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(removed, "k2", "i2a", "i2a")},
			expectedKeys:       []string{"k2"},
			expectedIdentities: []string{"i2a"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		"removed on left, changed on right": {
			baseKeys:           []string{"k1", "k2"},
			diffs:              []graveler.Diff{testMergeNewDiff(added, "k2", "i2a", "")},
			expectedIdentities: []string{"i2a"},
			expectedKeys:       []string{"k2"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		"added on both with different identities": {
			baseKeys:           []string{"k1"},
			diffs:              []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2b")},
			expectedIdentities: []string{"i2a"},
			expectedKeys:       []string{"k2"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		"multiple add and removes": {
			baseKeys: []string{"k1", "k3"},
			diffs: []graveler.Diff{testMergeNewDiff(removed, "k1", "i1", "i1"),
				testMergeNewDiff(added, "k2", "i2", ""),
				testMergeNewDiff(removed, "k3", "i3", "i3"),
				testMergeNewDiff(added, "k4", "i4", "")},
			expectedKeys:       []string{"k1", "k2", "k3", "k4"},
			expectedIdentities: []string{"i1", "i2", "i3", "i4"},
			expectedDiffTypes:  []graveler.DiffType{removed, added, removed, added},
		},
		"changes on each side": {
			baseKeys: []string{"k1", "k2", "k3", "k4"},
			diffs: []graveler.Diff{testMergeNewDiff(changed, "k1", "i1a", "i1"),
				testMergeNewDiff(changed, "k2", "i2", "i2a"),
				testMergeNewDiff(changed, "k3", "i3a", "i3"),
				testMergeNewDiff(changed, "k4", "i4", "i4a")},
			expectedKeys:       []string{"k1", "k3"},
			expectedIdentities: []string{"i1a", "i3a"},
			expectedDiffTypes:  []graveler.DiffType{changed, changed},
		},
	}

	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			diffIt := testutil.NewDiffIter(tst.diffs)
			defer diffIt.Close()
			base := makeBaseIterator(tst.baseKeys)
			ctx := context.Background()
			it := committed.NewCompareValueIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), base)
			var gotValues, gotKeys []string
			var gotDiffTypes []graveler.DiffType
			idx := 0
			for it.Next() {
				idx++
				gotKeys = append(gotKeys, string(it.Value().Key))
				gotValues = append(gotValues, string(it.Value().Value.Identity))
				gotDiffTypes = append(gotDiffTypes, it.Value().Type)

			}
			if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedDiffTypes, gotDiffTypes); diff != nil {
				t.Fatalf("got unexpected diff types from merge iterator. diff=%s", diff)
			}
		})
	}
}

func TestMergeSeekWithRange(t *testing.T) {
	diffIt := testutil.NewFakeDiffIterator()
	diffIt.AddRange(&committed.RangeDiff{
		Type: added,
		Range: &committed.Range{
			ID:        "add:k1-k3",
			MinKey:    committed.Key("k1"),
			MaxKey:    committed.Key("k3"),
			Count:     3,
			Tombstone: false,
		},
	}).
		AddValueRecords(makeDV(added, "k1", "i1", ""), makeDV(added, "k2", "i2", ""), makeDV(added, "k3", "i3", "")).
		AddRange(&committed.RangeDiff{
			Type: removed,
			Range: &committed.Range{
				ID:        "remove:k4-k6",
				MinKey:    committed.Key("k4"),
				MaxKey:    committed.Key("k6"),
				Count:     3,
				Tombstone: false,
			},
		}).
		AddValueRecords(makeDV(removed, "k4", "i4", "i4"), makeDV(removed, "k5", "i5", "i5"), makeDV(removed, "k6", "i6", "i6")).
		AddRange(nil).
		AddValueRecords(makeDV(added, "k7", "i7", ""), makeDV(removed, "k8", "i8", "i8"), makeDV(changed, "k9", "i9a", "i9"))

	baseIt := testutil.NewFakeIterator()
	baseIt.AddRange(&committed.Range{
		ID:        "remove:k4-k6",
		MinKey:    committed.Key("k4"),
		MaxKey:    committed.Key("k6"),
		Count:     3,
		Tombstone: false,
	}).
		AddValueRecords(makeV("k4", "i4"), makeV("k5", "i5"), makeV("k6", "i6")).
		AddRange(&committed.Range{
			ID:        "k7-k9",
			MinKey:    committed.Key("k4"),
			MaxKey:    committed.Key("k6"),
			Count:     2,
			Tombstone: false,
		}).
		AddValueRecords(makeV("k8", "i8"), makeV("k9", "i9"))
	ctx := context.Background()
	it := committed.NewMergeIterator(ctx, diffIt, baseIt)
	// expected diffs, +k1, -k2, Chng:k3,+k7, Conf:k9,
	defer it.Close()
	tests := []struct {
		seekTo             string
		expectedKeys       []string
		expectedIdentities []string
		expectedRanges     []string
	}{
		{
			seekTo:             "k1",
			expectedKeys:       []string{"", "", "k7", "k8", "k9"},
			expectedIdentities: []string{"", "", "i7", "", "i9a"},
			expectedRanges:     []string{"add:k1-k3", "remove:k4-k6", "", "", ""},
		},
		{
			seekTo:             "k2",
			expectedKeys:       []string{"k2", "k3", "", "k7", "k8", "k9"},
			expectedIdentities: []string{"i2", "i3", "", "i7", "", "i9a"},
			expectedRanges:     []string{"add:k1-k3", "add:k1-k3", "remove:k4-k6", "", "", ""},
		},

		{
			seekTo:             "k3",
			expectedKeys:       []string{"k3", "", "k7", "k8", "k9"},
			expectedIdentities: []string{"i3", "", "i7", "", "i9a"},
			expectedRanges:     []string{"add:k1-k3", "remove:k4-k6", "", "", ""},
		},

		{
			seekTo:             "k4",
			expectedKeys:       []string{"", "k7", "k8", "k9"},
			expectedIdentities: []string{"", "i7", "", "i9a"},
			expectedRanges:     []string{"remove:k4-k6", "", "", ""},
		},
		{
			seekTo:             "k5",
			expectedKeys:       []string{"k5", "k6", "k7", "k8", "k9"},
			expectedIdentities: []string{"", "", "i7", "", "i9a"},
			expectedRanges:     []string{"remove:k4-k6", "remove:k4-k6", "", "", ""},
		},
		{
			seekTo:             "k8",
			expectedKeys:       []string{"k8", "k9"},
			expectedIdentities: []string{"", "i9a"},
			expectedRanges:     []string{"", ""},
		},
		{
			seekTo:             "k9",
			expectedKeys:       []string{"k9"},
			expectedIdentities: []string{"i9a"},
			expectedRanges:     []string{""},
		},
		{
			seekTo:             "k99",
			expectedKeys:       nil,
			expectedIdentities: nil,
		},
	}
	for _, tst := range tests {
		t.Run(fmt.Sprintf("seek to %s", tst.seekTo), func(t *testing.T) {
			it.SeekGE([]byte(tst.seekTo))
			val, _ := it.Value()
			if val != nil {
				t.Fatalf("value expected to be nil after SeekGE. got=%v", val)
			}
			idx := 0
			var gotValues, gotKeys, gotRanges []string

			hasNext := it.Next()
			for hasNext {
				val, rng := it.Value()
				gotRanges = append(gotRanges, getRangeID(rng))
				idx++
				gotKeys = append(gotKeys, getRecordKey(val))
				gotValues = append(gotValues, getRecordID(val))

				if val == nil {
					hasNext = it.NextRange()
				} else {
					hasNext = it.Next()
				}
			}

			if it.Err() != nil {
				t.Errorf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Errorf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Errorf("got unexpected values from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedRanges, gotRanges); diff != nil {
				t.Errorf("got unexpected ranges from merge iterator. diff=%s", diff)
			}
		})
	}
}

func TestCompareSeek(t *testing.T) {
	diffs := []graveler.Diff{
		testMergeNewDiff(added, "k1", "i1", ""),
		testMergeNewDiff(removed, "k2", "i2", "i2"),
		testMergeNewDiff(changed, "k3", "i3a", "i3"),
		testMergeNewDiff(added, "k4", "i4", ""),
		testMergeNewDiff(removed, "k5", "i5", "i5"),
		testMergeNewDiff(changed, "k6", "i6", "i6a"),
		testMergeNewDiff(added, "k7", "i7", ""),
		testMergeNewDiff(removed, "k8", "i8", "i8"),
		testMergeNewDiff(changed, "k9", "i9a", "i9"),
	}
	diffIt := testutil.NewDiffIter(diffs)
	baseKeys := []string{"k2", "k3", "k4", "k6"}
	base := makeBaseIterator(baseKeys)
	ctx := context.Background()
	it := committed.NewCompareValueIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), base)
	// expected diffs, +k1, -k2, Chng:k3,+k7, Conf:k9,
	defer it.Close()
	tests := []struct {
		seekTo             string
		expectedKeys       []string
		expectedIdentities []string
		expectedDiffTypes  []graveler.DiffType
	}{
		{
			seekTo:             "k1",
			expectedKeys:       []string{"k1", "k2", "k3", "k7", "k9"},
			expectedIdentities: []string{"i1", "i2", "i3a", "i7", "i9a"},
			expectedDiffTypes:  []graveler.DiffType{added, removed, changed, added, conflict},
		},
		{
			seekTo:             "k2",
			expectedKeys:       []string{"k2", "k3", "k7", "k9"},
			expectedIdentities: []string{"i2", "i3a", "i7", "i9a"},
			expectedDiffTypes:  []graveler.DiffType{removed, changed, added, conflict},
		},

		{
			seekTo:             "k3",
			expectedKeys:       []string{"k3", "k7", "k9"},
			expectedIdentities: []string{"i3a", "i7", "i9a"},
			expectedDiffTypes:  []graveler.DiffType{changed, added, conflict},
		},

		{
			seekTo:             "k4",
			expectedKeys:       []string{"k7", "k9"},
			expectedIdentities: []string{"i7", "i9a"},
			expectedDiffTypes:  []graveler.DiffType{added, conflict},
		},
		{
			seekTo:             "k8",
			expectedKeys:       []string{"k9"},
			expectedIdentities: []string{"i9a"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		{
			seekTo:             "k9",
			expectedKeys:       []string{"k9"},
			expectedIdentities: []string{"i9a"},
			expectedDiffTypes:  []graveler.DiffType{conflict},
		},
		{
			seekTo:             "k99",
			expectedKeys:       nil,
			expectedIdentities: nil,
		},
	}
	for _, tst := range tests {
		t.Run(fmt.Sprintf("seek to %s", tst.seekTo), func(t *testing.T) {
			it.SeekGE([]byte(tst.seekTo))
			if it.Value() != nil {
				t.Fatalf("value expected to be nil after SeekGE. got=%v", it.Value())
			}
			idx := 0
			var gotValues, gotKeys []string
			var gotDiffTypes []graveler.DiffType
			for it.Next() {
				idx++
				gotKeys = append(gotKeys, string(it.Value().Key))
				gotDiffTypes = append(gotDiffTypes, it.Value().Type)
				gotValues = append(gotValues, string(it.Value().Value.Identity))
			}
			if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from compare iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from compare iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedDiffTypes, gotDiffTypes); diff != nil {
				t.Fatalf("got unexpected diff types from compare iterator. diff=%s", diff)
			}
		})
	}
}

func makeBaseIterator(keys []string) *testutil.FakeIterator {
	base := testutil.NewFakeIterator()
	if len(keys) == 0 {
		return base
	}
	var baseRecords []*graveler.ValueRecord
	for _, key := range keys {
		baseRecords = append(baseRecords, &graveler.ValueRecord{
			Key:   []byte(key),
			Value: &graveler.Value{Identity: []byte(baseKeyToIdentity[key])},
		})
	}
	base.AddRange(&committed.Range{
		ID:     "range",
		MinKey: []byte(keys[0]),
	})
	base.AddValueRecords(baseRecords...)
	return base
}
