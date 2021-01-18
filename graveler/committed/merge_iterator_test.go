package committed_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/testutil"
)

var baseKeyToIdentity = map[string]string{"k1": "i1", "k2": "i2", "k3": "i3", "k4": "i4", "k6": "i6"}

const (
	added   = graveler.DiffTypeAdded
	removed = graveler.DiffTypeRemoved
	changed = graveler.DiffTypeChanged
)

func testMergeNewDiff(typ graveler.DiffType, key string, newIdentity string, oldIdentity string) graveler.Diff {
	return graveler.NewDiffResult(typ, graveler.Key(key), &graveler.Value{Identity: []byte(newIdentity)}, []byte(oldIdentity))
}

func TestMerge(t *testing.T) {
	tests := map[string]struct {
		baseKeys            []string
		diffs               []graveler.Diff
		conflictExpectedIdx *int
		expectedKeys        []string
		expectedIdentities  []string
	}{
		"added on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k3", "i3", "")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k3"},
			expectedIdentities:  []string{"i3"},
		},
		"changed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{"i2a"},
		},
		"deleted on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{""},
		},
		"added on left": {
			baseKeys:            []string{"k1"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"removed on left": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2", "")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"changed on left": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2", "i2a")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"changed on both": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2b", "i2a")},
			conflictExpectedIdx: swag.Int(0),
		},
		"changed on left, removed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2a", "i2a")},
			conflictExpectedIdx: swag.Int(0),
		},
		"removed on left, changed on right": {
			baseKeys:            []string{"k1", "k2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2a", "")},
			conflictExpectedIdx: swag.Int(0),
		},
		"added on both with different identities": {
			baseKeys:            []string{"k1"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2b")},
			conflictExpectedIdx: swag.Int(0),
		},
		"multiple add and removes": {
			baseKeys: []string{"k1", "k3"},
			diffs: []graveler.Diff{testMergeNewDiff(removed, "k1", "i1", "i1"),
				testMergeNewDiff(added, "k2", "i2", ""),
				testMergeNewDiff(removed, "k3", "i3", "i3"),
				testMergeNewDiff(added, "k4", "i4", "")},
			expectedKeys:       []string{"k1", "k2", "k3", "k4"},
			expectedIdentities: []string{"", "i2", "", "i4"},
		},
		"changes on each side": {
			baseKeys: []string{"k1", "k2", "k3", "k4"},
			diffs: []graveler.Diff{testMergeNewDiff(changed, "k1", "i1a", "i1"),
				testMergeNewDiff(changed, "k2", "i2", "i2a"),
				testMergeNewDiff(changed, "k3", "i3a", "i3"),
				testMergeNewDiff(changed, "k4", "i4", "i4a")},
			expectedKeys:       []string{"k1", "k3"},
			expectedIdentities: []string{"i1a", "i3a"},
		},
	}

	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			diffIt := testutil.NewDiffIter(tst.diffs)
			defer diffIt.Close()
			base := makeBaseIterator(tst.baseKeys)
			it, err := committed.NewMergeIterator(diffIt, base)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			var gotValues, gotKeys []string
			idx := 0
			for it.Next() {
				idx++
				gotKeys = append(gotKeys, string(it.Value().Key))
				if it.Value().Value == nil {
					gotValues = append(gotValues, "")
				} else {
					gotValues = append(gotValues, string(it.Value().Identity))
				}
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
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from merge iterator. diff=%s", diff)
			}
		})
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
	it, err := committed.NewMergeIterator(diffIt, base)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// expected diffs, +k1, -k2, Chng:k3,+k7, Conf:k9,
	defer it.Close()
	tests := []struct {
		seekTo              string
		expectedKeys        []string
		expectedIdentities  []string
		conflictExpectedIdx *int
	}{
		{
			seekTo:              "k1",
			expectedKeys:        []string{"k1", "k2", "k3", "k7"},
			expectedIdentities:  []string{"i1", "", "i3a", "i7"},
			conflictExpectedIdx: swag.Int(4),
		},
		{
			seekTo:              "k2",
			expectedKeys:        []string{"k2", "k3", "k7"},
			expectedIdentities:  []string{"", "i3a", "i7"},
			conflictExpectedIdx: swag.Int(3),
		},

		{
			seekTo:              "k3",
			expectedKeys:        []string{"k3", "k7"},
			expectedIdentities:  []string{"i3a", "i7"},
			conflictExpectedIdx: swag.Int(2),
		},

		{
			seekTo:              "k4",
			expectedKeys:        []string{"k7"},
			expectedIdentities:  []string{"i7"},
			conflictExpectedIdx: swag.Int(1),
		},
		{
			seekTo:              "k8",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: swag.Int(0),
		},
		{
			seekTo:              "k9",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: swag.Int(0),
		},
		{
			seekTo:              "k99",
			expectedKeys:        nil,
			expectedIdentities:  nil,
			conflictExpectedIdx: nil,
		},
	}
	for i, tst := range tests {
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			it.SeekGE([]byte(tst.seekTo))
			if it.Value() != nil {
				t.Fatalf("value expected to be nil after SeekGE. got=%v", it.Value())
			}
			idx := 0
			var gotValues, gotKeys []string
			for it.Next() {
				idx++
				gotKeys = append(gotKeys, string(it.Value().Key))
				if it.Value().Value == nil {
					gotValues = append(gotValues, "")
				} else {
					gotValues = append(gotValues, string(it.Value().Identity))
				}
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
			if diff := deep.Equal(tst.expectedKeys, gotKeys); diff != nil {
				t.Fatalf("got unexpected keys from merge iterator. diff=%s", diff)
			}
			if diff := deep.Equal(tst.expectedIdentities, gotValues); diff != nil {
				t.Fatalf("got unexpected values from merge iterator. diff=%s", diff)
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
