package committed_test

import (
	"context"
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
			diffs: []graveler.Diff{
				testMergeNewDiff(removed, "k1", "i1", "i1"),
				testMergeNewDiff(added, "k2", "i2", ""),
				testMergeNewDiff(removed, "k3", "i3", "i3"),
				testMergeNewDiff(added, "k4", "i4", ""),
			},
			expectedKeys:       []string{"k1", "k2", "k3", "k4"},
			expectedIdentities: []string{"i1", "i2", "i3", "i4"},
			expectedDiffTypes:  []graveler.DiffType{removed, added, removed, added},
		},
		"changes on each side": {
			baseKeys: []string{"k1", "k2", "k3", "k4"},
			diffs: []graveler.Diff{
				testMergeNewDiff(changed, "k1", "i1a", "i1"),
				testMergeNewDiff(changed, "k2", "i2", "i2a"),
				testMergeNewDiff(changed, "k3", "i3a", "i3"),
				testMergeNewDiff(changed, "k4", "i4", "i4a"),
			},
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
			defer it.Close()

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
