package committed_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/testutil"
)

const (
	added   = graveler.DiffTypeAdded
	removed = graveler.DiffTypeRemoved
	changed = graveler.DiffTypeChanged
)

func TestMergeIterator(t *testing.T) {
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
			leftIt := newFakeMetaRangeIterator([][]string{tst.leftKeys}, [][]string{tst.leftIdentities})
			rightIt := newFakeMetaRangeIterator([][]string{tst.rightKeys}, [][]string{tst.rightIdentities})
			committedFake := &testutil.CommittedFake{
				ValuesByKey:  baseRecords,
				DiffIterator: committed.NewDiffIterator(leftIt, rightIt),
			}

			committedFake.ValuesByKey = baseRecords
			it, err := committed.NewMergeIterator(context.Background(), "a", "b", "c", "d", committedFake)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
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
			}
		})
	}
}

func testMergeNewDiff(typ graveler.DiffType, key string, newIdentity string, oldIdentity string) graveler.Diff {
	return graveler.Diff{
		Type:        typ,
		Key:         graveler.Key(key),
		Value:       &graveler.Value{Identity: []byte(newIdentity)},
		OldIdentity: []byte(oldIdentity),
	}
}

func TestMerge(t *testing.T) {
	tests := map[string]struct {
		baseKeyToIdentity   map[string]string
		diffs               []graveler.Diff
		conflictExpectedIdx *int
		expectedKeys        []string
		expectedIdentities  []string
	}{
		"added on right": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k3", "i3", "")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k3"},
			expectedIdentities:  []string{"i3"},
		},
		"changed on right": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{"i2a"},
		},
		"deleted on right": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: nil,
			expectedKeys:        []string{"k2"},
			expectedIdentities:  []string{""},
		},
		"added on left": {
			baseKeyToIdentity:   map[string]string{"k1": "i1"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2", "i2")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"removed on left": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2", "")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"changed on left": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2", "i2a")},
			conflictExpectedIdx: nil,
			expectedIdentities:  nil,
		},
		"changed on both": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2b", "i2a")},
			conflictExpectedIdx: swag.Int(0),
		},
		"changed on left, removed on right": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(removed, "k2", "i2a", "i2a")},
			conflictExpectedIdx: swag.Int(0),
		},
		"removed on left, changed on right": {
			baseKeyToIdentity:   map[string]string{"k1": "i1", "k2": "i2"},
			diffs:               []graveler.Diff{testMergeNewDiff(added, "k2", "i2a", "")},
			conflictExpectedIdx: swag.Int(0),
		},
		"added on both with different identities": {
			baseKeyToIdentity:   map[string]string{"k1": "i1"},
			diffs:               []graveler.Diff{testMergeNewDiff(changed, "k2", "i2a", "i2b")},
			conflictExpectedIdx: swag.Int(0),
		},
		"multiple add and removes": {
			baseKeyToIdentity: map[string]string{"k1": "i1", "k3": "i3"},
			diffs: []graveler.Diff{testMergeNewDiff(removed, "k1", "i1", "i1"),
				testMergeNewDiff(added, "k2", "i2", ""),
				testMergeNewDiff(removed, "k3", "i3", "i3"),
				testMergeNewDiff(added, "k4", "i4", "")},
			expectedKeys:       []string{"k1", "k2", "k3", "k4"},
			expectedIdentities: []string{"", "i2", "", "i4"},
		},
		"changes on each side": {
			baseKeyToIdentity: map[string]string{"k1": "i1", "k2": "i2", "k3": "i3", "k4": "i4"},
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
			committedFake := testutil.NewCommittedFake()
			committedFake.DiffIterator = diffIt
			baseRecords := map[string]*graveler.Value{}
			for k, identity := range tst.baseKeyToIdentity {
				baseRecords[k] = &graveler.Value{Identity: []byte(identity)}
			}
			committedFake.ValuesByKey = baseRecords
			it, err := committed.NewMergeIterator(context.Background(), "a", "b", "c", "d", committedFake)
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
		testMergeNewDiff(changed, "k6", "i6a", "i6"),
		testMergeNewDiff(added, "k7", "i7", ""),
		testMergeNewDiff(removed, "k8", "i8", "i8"),
		testMergeNewDiff(changed, "k9", "i9a", "i9"),
	}
	diffIt := testutil.NewDiffIter(diffs)
	committedFake := testutil.NewCommittedFake()
	committedFake.DiffIterator = diffIt
	baseKeyToIdentity := map[string]string{"k2": "i2", "k3": "i3", "k4": "i4", "k6": "i6a"}
	baseRecords := map[string]*graveler.Value{}
	for k, identity := range baseKeyToIdentity {
		baseRecords[k] = &graveler.Value{Identity: []byte(identity)}
	}
	committedFake.ValuesByKey = baseRecords
	it, err := committed.NewMergeIterator(context.Background(), "a", "b", "c", "d", committedFake)
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
