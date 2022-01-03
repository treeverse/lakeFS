package committed_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

type diffTestRange struct {
	MinKey string
	MaxKey string
	Count  int64
}

func newDiffTestRange(p *committed.RangeDiff) *diffTestRange {
	if p == nil || p.Range == nil {
		return nil
	}
	return &diffTestRange{string(p.Range.MinKey), string(p.Range.MaxKey), p.Range.Count}
}

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
		expectedRanges            []*diffTestRange
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
			expectedRanges:            []*diffTestRange{},
			expectedLeftReadsByRange:  []int{0, 0},
			expectedRightReadsByRange: []int{0, 0},
		},
		"added in existing rng": {
			leftKeys:                  [][]string{{"k1", "k2"}, {"k3"}},
			leftIdentities:            [][]string{{"i1", "i2"}, {"i3"}},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3", "k4"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3", "i4"}},
			expectedDiffKeys:          []string{"k4"},
			expectedRanges:            []*diffTestRange{nil},
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
			expectedRanges:            []*diffTestRange{nil},
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
			expectedRanges:            []*diffTestRange{nil, nil},
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
			expectedRanges:            []*diffTestRange{{"k3", "k3", 1}, nil},
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
			expectedRanges:            []*diffTestRange{nil, nil, nil, nil, {"k5", "k6", 2}, {"k5", "k6", 2}, {"k5", "k6", 2}},
			expectedDiffTypes:         []graveler.DiffType{removed, removed, changed, added, added, added},
			expectedDiffIdentities:    []string{"i1", "i2", "i3a", "i4", "i5", "i6"},
			expectedLeftReadsByRange:  []int{3},
			expectedRightReadsByRange: []int{2, 2},
		},
		"diff between empty iterators": {
			expectedDiffKeys: []string{},
			expectedRanges:   []*diffTestRange{},
		},
		"added on empty": {
			leftKeys:                  [][]string{},
			leftIdentities:            [][]string{},
			rightKeys:                 [][]string{{"k1", "k2"}, {"k3"}},
			rightIdentities:           [][]string{{"i1", "i2"}, {"i3"}},
			expectedDiffKeys:          []string{"k1", "k2", "k3"},
			expectedRanges:            []*diffTestRange{{"k1", "k2", 2}, {"k1", "k2", 2}, {"k1", "k2", 2}, {"k3", "k3", 1}, {"k3", "k3", 1}},
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
			expectedRanges:            []*diffTestRange{{"k1", "k2", 2}, {"k1", "k2", 2}, {"k1", "k2", 2}, nil},
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
			expectedRanges:            []*diffTestRange{nil, nil},
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
			expectedRanges:            []*diffTestRange{{"k3", "k3", 1}, {"k3", "k3", 1}, {"k4", "k4", 1}, {"k4", "k4", 1}, {"k5", "k5", 1}, {"k5", "k5", 1}},
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
			expectedRanges:            []*diffTestRange{{"k3", "k3", 1}, {"k3", "k3", 1}, {"k6", "k7", 2}, {"k6", "k7", 2}, {"k6", "k7", 2}},
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
			expectedRanges:            []*diffTestRange{{"", "", 0}, {"", "", 0}},
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
			expectedRanges:            []*diffTestRange{{"k3", "k4", 2}, {"k3", "k4", 2}, {"k3", "k4", 2}},
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
			expectedRanges:            []*diffTestRange{{"k1", "k2", 2}, nil, {"k5", "k6", 2}, nil},
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
			ctx := context.Background()
			it := committed.NewDiffIterator(ctx, fakeLeft, fakeRight)
			defer it.Close()
			var diffs []*graveler.Diff
			actualDiffKeys := make([]string, 0)
			actualDiffRanges := make([]*diffTestRange, 0)
			for it.Next() {
				diff, rng := it.Value()
				actualDiffRanges = append(actualDiffRanges, newDiffTestRange(rng))
				if diff != nil {
					actualDiffKeys = append(actualDiffKeys, string(diff.Key))
					diffs = append(diffs, diff)
				}
			}
			if it.Err() != nil {
				t.Fatalf("got unexpected error: %v", it.Err())
			}
			if diff := deep.Equal(tst.expectedDiffKeys, actualDiffKeys); diff != nil {
				t.Fatalf("keys in diff different than expected. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedRanges, actualDiffRanges); diff != nil {
				t.Fatalf("ranges in diff different than expected. diff=%s", diff)
			}
			for i, d := range diffs {
				if d.Type != tst.expectedDiffTypes[i] {
					t.Fatalf("unexpected key in diff index %d. expected=%s, got=%s", i, tst.expectedDiffKeys[i], string(d.Key))
				}
				if string(d.Value.Identity) != tst.expectedDiffIdentities[i] {
					t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", i, tst.expectedDiffIdentities[i], string(d.Value.Identity))
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

func TestDiffCancelContext(t *testing.T) {
	left := newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"v1", "v2"}})
	right := newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"v1", "v2"}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it := committed.NewDiffValueIterator(ctx, left, right)
	defer it.Close()
	if it.Next() {
		t.Fatal("Next() should return false")
	}
	if err := it.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() returned %v, should return context.Canceled", err)
	}
}

// TODO(Guys): add test for range changed
func TestNextRange(t *testing.T) {
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx,
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}, {"k3", "k4"}}, [][]string{{"i1", "i2"}, {"i3", "i4"}}),
		newFakeMetaRangeIterator([][]string{{"k3", "k4", "k5"}, {"k6", "k7"}}, [][]string{{"i3a", "i4a", "i5a"}, {"i6", "i7"}}))

	t.Run("next range - in middle of range", func(t *testing.T) {
		if !it.Next() { // move to range k1-k2
			t.Fatal("expected iterator to have value")
		}
		record, rng := it.Value()
		if record != nil {
			t.Errorf("expected record to be nil got %v", record)
		}
		if !it.Next() { // move to k1
			t.Fatalf("expected it.Next() to return true (err %v)", it.Err())
		}
		record, rng = it.Value()
		if record == nil || string(record.Key) != "k1" {
			t.Errorf("expected record with key=k1, got record %v", record)
		}
		if !it.NextRange() { // move to k3 (moves to end of current range, but can't start a new range because k3 is part of two different ranges)
			t.Fatalf("expected it.NextRange() to return true (err %v)", it.Err())
		}
		record, rng = it.Value()
		if rng != nil {
			t.Errorf("expected range to be nil got range %v", rng)
		}
		if record == nil || string(record.Key) != "k3" {
			t.Errorf("expected record with key=k3, got record %v", record)
		}
		if !it.Next() { // move to k4
			t.Fatalf("expected it.Next() to return true (err %v)", it.Err())
		}
		if !it.Next() { // move to k5
			t.Fatalf("expected it.Next() to return true (err %v)", it.Err())
		}
		if !it.Next() { // move to range k6-k7
			t.Fatalf("expected it.Next() to return true (err %v)", it.Err())
		}
		record, rng = it.Value()
		if record != nil {
			t.Errorf("expected record to be nil got record %v", record)
		}
		if !it.Next() { // move to k6
			t.Fatalf("expected it.Next() to return true (err %v)", it.Err())
		}
		record, rng = it.Value()
		if record == nil || string(record.Key) != "k6" {
			t.Errorf("expected record with key=k6, got record %v", record)
		}
		if it.NextRange() { // move to end
			t.Fatal("expected it.NextRange() to return false")
		}
		if err := it.Err(); err != nil {
			t.Fatalf("unexpected error:%v", err)
		}
	})
	t.Run("call next range with no range", func(t *testing.T) {
		it.SeekGE(graveler.Key("k3"))
		if !it.Next() {
			t.Fatal("expected iterator to have value")
		}
		record, rng := it.Value()
		if record == nil || string(record.Key) != "k3" {
			t.Fatal("expected record to have a value equal to k3")
		}
		if rng != nil {
			t.Fatal("expected range to not have value")
		}
		if it.NextRange() {
			t.Fatal("expected false from iterator after close")
		}
		if err := it.Err(); err != committed.ErrNoRange {
			t.Fatalf("expected to get err=%s, got: %s", committed.ErrNoRange, err)
		}
	})
}

func TestNextRangeChange(t *testing.T) {
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx,
		newFakeMetaRangeIterator([][]string{{"k1", "k3"}, {"k5", "k6"}}, [][]string{{"i1", "i3"}, {"i5", "i6"}}),
		newFakeMetaRangeIterator([][]string{{"k1", "k2", "k3"}, {"k5", "k6"}}, [][]string{{"i1", "i2", "i3"}, {"i5", "i6"}}))
	if !it.Next() {
		t.Fatal("expected iterator to have value")
	}
	record, rng := it.Value()
	if record != nil {
		t.Errorf("expected record to be nil got record %v", record)
	}
	if rng.Type != changed {
		t.Errorf("expected range diff type to be changed got %v", rng.Type)
	}
	if it.NextRange() {
		t.Fatal("expected next range to return false")
	}
}

func TestNextErr(t *testing.T) {
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx,
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"i1", "i2"}}),
		newFakeMetaRangeIterator([][]string{{"k1", "k2", "k3"}}, [][]string{{"i1a", "i2a", "i3a"}}))
	if !it.Next() {
		t.Fatalf("unexptected result from it.Next(), expected true, got false with err=%s", it.Err())
	}
	if it.NextRange() {
		val, rng := it.Value()
		t.Fatalf("unexptected result from it.NextRange(), expected false, got true with value=%v , rng=%v", val, rng)
	}
	if err := it.Err(); err != committed.ErrNoRange {
		t.Fatalf("expected to get err=%s, got: %s", committed.ErrNoRange, err)
	}
}

func TestSameBounds(t *testing.T) {
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx,
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}, {"k3", "k4"}}, [][]string{{"i1", "i2"}, {"i3", "i4"}}),
		newFakeMetaRangeIterator([][]string{{"k1", "k2"}, {"k3", "k4"}}, [][]string{{"i1a", "i2a"}, {"i3a", "i4a"}}))
	if !it.Next() {
		t.Fatalf("unexptected result from it.Next(), expected true, got false with err=%s", it.Err())
	}
	if !it.NextRange() {
		t.Fatalf("unexptected result from it.Next(), expected true, got false with err=%s", it.Err())
	}
	val, _ := it.Value()
	if val != nil {
		t.Errorf("unexptected value after it.NextRange(), expected nil, got=%v", val)
	}
	if !it.Next() {
		t.Fatalf("unexptected result from it.Next(), expected true, got false with err=%s", it.Err())
	}
	val, _ = it.Value()
	if val == nil || string(val.Value.Identity) != "i3a" {
		t.Errorf("unexptected value after it.Next(), expected i3a, got=%v", val)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected error, got: %s", err)
	}
}

func TestDiffSeek(t *testing.T) {
	const (
		added   = graveler.DiffTypeAdded
		removed = graveler.DiffTypeRemoved
		changed = graveler.DiffTypeChanged
	)
	left := [][]string{{"k1", "k2"}, {"k4", "k5"}, {"k6", "k7"}}
	right := [][]string{{"k1", "k3"}, {"k3a", "k3b"}, {"k4", "k5"}, {"k6", "k7"}}
	leftIdentities := [][]string{{"i1", "i2"}, {"i4", "i5"}, {"i6", "i7"}}
	rightIdentities := [][]string{{"i1", "i3"}, {"i2a", "i2b"}, {"i4", "i5"}, {"i6", "i7a"}}
	diffTypeByKey := map[string]graveler.DiffType{"k2": removed, "k3a": added, "k3b": added, "k3": added, "k7": changed}
	diffIdentityByKey := map[string]string{"k2": "i2", "k3a": "i2a", "k3b": "i2b", "k3": "i3", "k7": "i7a"}
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx, newFakeMetaRangeIterator(left, leftIdentities), newFakeMetaRangeIterator(right, rightIdentities))
	defer it.Close()

	tests := []struct {
		seekTo             string
		expectedDiffKeys   []string
		expectedDiffRanges []*diffTestRange
	}{
		{
			seekTo:             "k1",
			expectedDiffKeys:   []string{"k2", "k3", "k3a", "k3b", "k7"},
			expectedDiffRanges: []*diffTestRange{nil, nil, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k6", "k7", 2}, nil},
		},
		{
			seekTo:             "k2",
			expectedDiffKeys:   []string{"k2", "k3", "k3a", "k3b", "k7"},
			expectedDiffRanges: []*diffTestRange{nil, nil, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k6", "k7", 2}, nil},
		},
		{
			seekTo:             "k3",
			expectedDiffKeys:   []string{"k3", "k3a", "k3b", "k7"},
			expectedDiffRanges: []*diffTestRange{nil, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k3a", "k3b", 2}, {"k6", "k7", 2}, nil},
		},
		{
			seekTo:             "k4",
			expectedDiffKeys:   []string{"k7"},
			expectedDiffRanges: []*diffTestRange{{"k6", "k7", 2}, nil},
		},
		{
			seekTo:             "k8",
			expectedDiffKeys:   []string{},
			expectedDiffRanges: []*diffTestRange{},
		},
	}
	for _, tst := range tests {
		t.Run(tst.seekTo, func(t *testing.T) {
			it.SeekGE([]byte(tst.seekTo))
			diff, rangeDiff := it.Value()
			if diff != nil || rangeDiff != nil {
				t.Fatalf("value expected to be nil after SeekGE. got diff=%v rangeDiff=%v", diff, rangeDiff)
			}
			keys := make([]string, 0)
			ranges := make([]*diffTestRange, 0)
			for it.Next() {
				currentDiff, currentRangeDiff := it.Value()
				ranges = append(ranges, newDiffTestRange(currentRangeDiff))
				if currentDiff != nil {
					key := currentDiff.Key.String()
					identity := string(currentDiff.Value.Identity)
					if currentDiff.Type != diffTypeByKey[key] {
						t.Fatalf("unexpected diff type in index %d. expected=%d, got=%d", len(keys), diffTypeByKey[key], currentDiff.Type)
					}
					if identity != diffIdentityByKey[key] {
						t.Fatalf("unexpected identity in diff index %d. expected=%s, got=%s", len(keys), diffIdentityByKey[key], identity)
					}
					keys = append(keys, key)
				}
			}
			if diff := deep.Equal(keys, tst.expectedDiffKeys); diff != nil {
				t.Fatal("unexpected keys in diff", diff)
			}
			if diff := deep.Equal(tst.expectedDiffRanges, ranges); diff != nil {
				t.Fatalf("ranges in diff different than expected. diff=%s", diff)
			}
		})

	}
}

func TestNextOnClose(t *testing.T) {
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx, newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"i1", "i2"}}), newFakeMetaRangeIterator([][]string{{"k1", "k2"}}, [][]string{{"i1a", "i2a"}}))
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
	ctx := context.Background()
	it := committed.NewDiffIterator(ctx, leftIt, rightIt)
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
		var minKey, maxKey committed.Key
		if len(rangeKeys[rangeIdx]) > 0 {
			minKey = []byte(rangeKeys[rangeIdx][0])
			maxKey = []byte(rangeKeys[rangeIdx][len(rangeKeys[rangeIdx])-1])
		}
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
		res.
			AddRange(&committed.Range{ID: committed.ID(strings.Join(identities, "-")), MinKey: minKey, MaxKey: maxKey, Count: int64(len(rangeKeys[rangeIdx]))}).
			AddValueRecords(rangeValues...)
	}
	return res
}
