package graveler_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
)

func val(key string, id string) *graveler.ValueRecord {
	val := &graveler.Value{
		Identity: []byte(id),
	}
	if id == "" {
		val = nil
	}
	return &graveler.ValueRecord{
		Key:   graveler.Key(key),
		Value: val,
	}
}

func diffVal(key string, leftID string, rightID string) *graveler.Diff {
	valueId := []byte(rightID)
	typ := graveler.DiffTypeChanged
	leftIdentity := []byte(leftID)
	if leftID == "" {
		typ = graveler.DiffTypeAdded
		leftIdentity = nil
	} else if rightID == "" {
		typ = graveler.DiffTypeRemoved
		valueId = []byte(leftID)
	}
	return &graveler.Diff{
		Type: typ,
		Key:  graveler.Key(key),
		Value: &graveler.Value{
			Identity: valueId,
		},
		LeftIdentity: leftIdentity,
	}
}

type testValue struct {
	key          string
	leftValue    string
	rightValue   string
	stagingValue *string // nil for nothing, "" for tombstone
}

func TestCombinedDiffIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	tests := map[string]struct {
		testValues    []testValue
		expectedDiffs []*graveler.Diff
	}{
		"staging_same_as_left": {
			testValues: []testValue{
				{key: "a", leftValue: "a", rightValue: "a", stagingValue: swag.String("a")},
				{key: "b", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b")},
				{key: "c", leftValue: "c", rightValue: "", stagingValue: swag.String("c")},
				{key: "d", leftValue: "", rightValue: "d", stagingValue: swag.String("")},
			},
			expectedDiffs: nil,
		},
		"staging_same_as_right": {
			testValues: []testValue{
				{key: "b", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b1")},
				{key: "c", leftValue: "c", rightValue: "", stagingValue: swag.String("")},
				{key: "d", leftValue: "", rightValue: "d", stagingValue: swag.String("d")},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("b", "b", "b1"),
				diffVal("c", "c", ""),
				diffVal("d", "", "d"),
			},
		},
		"nothing_on_staging": {
			testValues: []testValue{
				{key: "a", leftValue: "a", rightValue: "a"},
				{key: "b", leftValue: "b", rightValue: "b1"},
				{key: "c", leftValue: "c", rightValue: ""},
				{key: "d", leftValue: "", rightValue: "d"},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("b", "b", "b1"),
				diffVal("c", "c", ""),
				diffVal("d", "", "d"),
			},
		},
		"staging_different_from_both": {
			testValues: []testValue{
				{key: "a", leftValue: "a", rightValue: "a", stagingValue: swag.String("a1")},
				{key: "b", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b2")},
				{key: "c", leftValue: "c", rightValue: "", stagingValue: swag.String("c1")},
				{key: "d", leftValue: "", rightValue: "d", stagingValue: swag.String("d1")},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("a", "a", "a1"),
				diffVal("b", "b", "b2"),
				diffVal("c", "c", "c1"),
				diffVal("d", "", "d1"),
			},
		},
		"tombstones_on_staging": {
			testValues: []testValue{
				{key: "a", leftValue: "a", rightValue: "a", stagingValue: swag.String("")},
				{key: "b", leftValue: "b", rightValue: "b1", stagingValue: swag.String("")},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("a", "a", ""),
				diffVal("b", "b", ""),
			},
		},
		"staging_ends_after_diff": {
			testValues: []testValue{
				{key: "a", leftValue: "a", rightValue: "a", stagingValue: swag.String("a1")},
				{key: "b", leftValue: "b", rightValue: "b1"},
				{key: "c", leftValue: "c", rightValue: "c", stagingValue: swag.String("c1")},
				{key: "d", leftValue: "", rightValue: "", stagingValue: swag.String("d")},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("a", "a", "a1"),
				diffVal("b", "b", "b1"),
				diffVal("c", "c", "c1"),
				diffVal("d", "", "d"),
			},
		},
		"staging_not_on_left": {
			testValues: []testValue{
				{key: "a", leftValue: "", rightValue: "a", stagingValue: swag.String("a1")},
				{key: "b", leftValue: "b", rightValue: "b1"},
				{key: "c", leftValue: "", rightValue: "c", stagingValue: swag.String("c1")},
			},
			expectedDiffs: []*graveler.Diff{
				diffVal("a", "", "a1"),
				diffVal("b", "b", "b1"),
				diffVal("c", "", "c1"),
			},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			diffIterator, stagingIterator, leftIterator := prepareIterators(ctrl, tst.testValues, "")
			diffIterator.EXPECT().Err().AnyTimes()
			stagingIterator.EXPECT().Err().AnyTimes()
			c := graveler.NewCombinedDiffIterator(diffIterator, leftIterator, stagingIterator)
			var actualDiffs []*graveler.Diff
			for c.Next() {
				actualDiffs = append(actualDiffs, c.Value())
			}
			if c.Err() != nil {
				t.Fatalf("unexpected error from combined-diff iterator: %v", c.Err())
			}
			if diff := deep.Equal(tst.expectedDiffs, actualDiffs); diff != nil {
				t.Fatal("unexpected result from combined-diff iterator", diff)
			}
		})
	}
}

func TestCombinedDiffIterator_Seek(t *testing.T) {
	ctrl := gomock.NewController(t)
	testValues := []testValue{
		{key: "k01", leftValue: "a", rightValue: "a", stagingValue: swag.String("a")},
		{key: "k02", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b")},
		{key: "k03", leftValue: "c", rightValue: "", stagingValue: swag.String("c")},
		{key: "k04", leftValue: "", rightValue: "d", stagingValue: swag.String("")},
		{key: "k05", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b1")},
		{key: "k06", leftValue: "c", rightValue: "", stagingValue: swag.String("")},
		{key: "k07", leftValue: "", rightValue: "d", stagingValue: swag.String("d")},
		{key: "k08", leftValue: "a", rightValue: "a"},
		{key: "k09", leftValue: "b", rightValue: "b1"},
		{key: "k10", leftValue: "c", rightValue: ""},
		{key: "k11", leftValue: "", rightValue: "d"},
		{key: "k12", leftValue: "a", rightValue: "a", stagingValue: swag.String("a1")},
		{key: "k13", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b2")},
		{key: "k14", leftValue: "c", rightValue: "", stagingValue: swag.String("c1")},
		{key: "k15", leftValue: "", rightValue: "d", stagingValue: swag.String("d1")},
		{key: "k16", leftValue: "a", rightValue: "a", stagingValue: swag.String("")},
		{key: "k17", leftValue: "b", rightValue: "b1", stagingValue: swag.String("")},
	}
	expectedDiffs := []*graveler.Diff{
		diffVal("k05", "b", "b1"),
		diffVal("k06", "c", ""),
		diffVal("k07", "", "d"),
		diffVal("k09", "b", "b1"),
		diffVal("k10", "c", ""),
		diffVal("k11", "", "d"),
		diffVal("k12", "a", "a1"),
		diffVal("k13", "b", "b2"),
		diffVal("k14", "c", "c1"),
		diffVal("k15", "", "d1"),
		diffVal("k16", "a", ""),
		diffVal("k17", "b", ""),
	}
	tests := []string{"k", "k0", "k05", "k06", "k1", "k08", "k19"}
	for _, seekTo := range tests {
		t.Run(fmt.Sprintf("seek_to_%s", seekTo), func(t *testing.T) {
			var currentExpectedDiffs []*graveler.Diff
			for i := range expectedDiffs {
				if bytes.Compare(expectedDiffs[i].Key, []byte(seekTo)) >= 0 {
					currentExpectedDiffs = expectedDiffs[i:]
					break
				}
			}
			diffIterator, stagingIterator, leftIterator := prepareIterators(ctrl, testValues, seekTo)
			diffIterator.EXPECT().Err().AnyTimes()
			stagingIterator.EXPECT().Err().AnyTimes()

			diffIterator.EXPECT().SeekGE([]byte(seekTo))
			stagingIterator.EXPECT().SeekGE([]byte(seekTo))
			c := graveler.NewCombinedDiffIterator(diffIterator, leftIterator, stagingIterator)
			c.SeekGE(graveler.Key(seekTo))
			var actualDiffs []*graveler.Diff
			for c.Next() {
				actualDiffs = append(actualDiffs, c.Value())
			}
			if c.Err() != nil {
				t.Fatalf("unexpected error from combined-diff iterator: %v", c.Err())
			}
			if diff := deep.Equal(currentExpectedDiffs, actualDiffs); diff != nil {
				t.Fatal("unexpected result from combined-diff iterator", diff)
			}
		})
	}
}

func TestCombinedDiffIterator_ErrorOnStaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	testValues := []testValue{
		{key: "k05", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b1")},
		{key: "k06", leftValue: "c", rightValue: "", stagingValue: swag.String("")},
		{key: "k07", leftValue: "", rightValue: "d", stagingValue: swag.String("d")},
		{key: "k08", leftValue: "a", rightValue: "a"},
	}
	diffIterator, stagingIterator, leftIterator := prepareIterators(ctrl, testValues, "")
	diffIterator.EXPECT().Err().AnyTimes()
	expectedErr := fmt.Errorf("error from staging")
	stagingIterator.EXPECT().Err().Times(2).Return(expectedErr)
	c := graveler.NewCombinedDiffIterator(diffIterator, leftIterator, stagingIterator)
	for c.Next() {
	}
	if !errors.Is(c.Err(), expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, c.Err())
	}
}

func TestCombinedDiffIterator_ErrorOnCommitted(t *testing.T) {
	ctrl := gomock.NewController(t)
	testValues := []testValue{
		{key: "k05", leftValue: "b", rightValue: "b1", stagingValue: swag.String("b1")},
		{key: "k06", leftValue: "c", rightValue: "", stagingValue: swag.String("")},
		{key: "k07", leftValue: "", rightValue: "d", stagingValue: swag.String("d")},
		{key: "k08", leftValue: "a", rightValue: "a"},
	}
	diffIterator, stagingIterator, leftIterator := prepareIterators(ctrl, testValues, "")
	stagingIterator.EXPECT().Err().AnyTimes()
	expectedErr := fmt.Errorf("error from committed")
	diffIterator.EXPECT().Err().Times(2).Return(expectedErr)
	c := graveler.NewCombinedDiffIterator(diffIterator, leftIterator, stagingIterator)
	for c.Next() {
	}
	if !errors.Is(c.Err(), expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, c.Err())
	}
}

func TestCombinedDiffIterator_ErrorOnLeft(t *testing.T) {
	ctrl := gomock.NewController(t)
	testValues := []testValue{
		{key: "k05", leftValue: "a", rightValue: "a", stagingValue: swag.String("a1")},
	}
	diffIterator, stagingIterator, leftIterator := prepareIterators(ctrl, testValues, "")
	stagingIterator.EXPECT().Err().AnyTimes()
	diffIterator.EXPECT().Err().AnyTimes()
	expectedErr := fmt.Errorf("error from left iterator")
	leftIterator.Error = expectedErr
	c := graveler.NewCombinedDiffIterator(diffIterator, leftIterator, stagingIterator)
	for c.Next() {
	}
	if !errors.Is(c.Err(), expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, c.Err())
	}
}

func prepareIterators(ctrl *gomock.Controller, testValues []testValue, seekTo string) (*mock.MockDiffIterator, *mock.MockValueIterator, *testutils.FakeValueIterator) {
	var committedDiffs []*graveler.Diff
	var stagingValues []*graveler.ValueRecord
	var leftValues []*graveler.ValueRecord
	for _, testVal := range testValues {
		if testVal.leftValue != testVal.rightValue {
			committedDiffs = append(committedDiffs, diffVal(testVal.key, testVal.leftValue, testVal.rightValue))
		}
		if testVal.stagingValue != nil {
			stagingValues = append(stagingValues, val(testVal.key, *testVal.stagingValue))
		}
		if testVal.leftValue != "" {
			leftValues = append(leftValues, val(testVal.key, testVal.leftValue))
		}
	}
	diffIterator := mock.NewMockDiffIterator(ctrl)
	for _, committedDiff := range committedDiffs {
		if bytes.Compare(committedDiff.Key, []byte(seekTo)) < 0 {
			continue
		}
		diffIterator.EXPECT().Next().Return(true)
		diffIterator.EXPECT().Value().Return(committedDiff)
	}
	diffIterator.EXPECT().Next().Return(false)
	stagingIterator := mock.NewMockValueIterator(ctrl)
	for _, stagingValue := range stagingValues {
		if bytes.Compare(stagingValue.Key, []byte(seekTo)) < 0 {
			continue
		}
		stagingIterator.EXPECT().Next().Return(true)
		stagingIterator.EXPECT().Value().Return(stagingValue)
	}
	stagingIterator.EXPECT().Next().Return(false)
	leftIterator := testutils.NewFakeValueIterator(leftValues)
	return diffIterator, stagingIterator, leftIterator
}
