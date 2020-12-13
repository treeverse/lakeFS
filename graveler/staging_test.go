package graveler_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func newTestStagingManager(t *testing.T) graveler.StagingManager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return graveler.NewStagingManager(conn)
}

func TestSetGet(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	value := newTestValue("identity1", "value1")
	err = s.Set(context.Background(), "t1", []byte("a/b/c/"), value)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity1" {
		t.Errorf("got wrong value. expected=%s, got=%s", "identity1", string(e.Identity))
	}
}

func TestMultiToken(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.Set(context.Background(), "t1", []byte("a/b/c/"), newTestValue("identity1", "value1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity1" {
		t.Errorf("got wrong identity. expected=%s, got=%s", "identity1", string(e.Identity))
	}
	err = s.Set(context.Background(), "t2", []byte("a/b/c/"), newTestValue("identity2", "value2"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity1" {
		t.Errorf("got wrong value identity. expected=%s, got=%s", "identity1", string(e.Identity))
	}
	e, err = s.Get(context.Background(), "t2", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity2" {
		t.Errorf("got wrong value identity. expected=%s, got=%s", "identity2", string(e.Identity))
		t.Errorf("got wrong value identity. expected=%s, got=%s", "identity2", string(e.Identity))
	}
}

func TestDrop(t *testing.T) {
	s := newTestStagingManager(t)
	numOfValues := 1400
	for i := 0; i < numOfValues; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("identity%d", i), fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		err = s.Set(context.Background(), "t2", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("identity%d", i), fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	err := s.Drop(context.Background(), "t1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	v, err := s.Get(context.Background(), "t1", []byte("key0000"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("after dropping staging area, expected ErrNotFound in Get. got err=%v, got value=%v", err, v)
	}
	it, _ := s.List(context.Background(), "t1")
	if it.Next() {
		t.Fatal("expected staging area with token t1 to be empty, got non-empty iterator")
	}
	it.Close()
	it, _ = s.List(context.Background(), "t2")
	count := 0
	for it.Next() {
		if string(it.Value().Data) != fmt.Sprintf("value%d", count) {
			t.Fatalf("unexpected value returned from List at index %d. expected=%s, got=%s", count, fmt.Sprintf("value%d", count), string(it.Value().Data))
		}
		count++
	}
	it.Close()
	if count != numOfValues {
		t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfValues, count)
	}
}

func TestDropByPrefix(t *testing.T) {
	s := newTestStagingManager(t)
	numOfValues := 2400
	for i := 0; i < numOfValues; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("identity%d", i), fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		err = s.Set(context.Background(), "t2", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("identity%d", i), fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	err := s.DropByPrefix(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	v, err := s.Get(context.Background(), "t1", []byte("key1000"))
	if !errors.Is(err, db.ErrNotFound) {
		// key1000 starts with the deleted prefix - should have been deleted
		t.Fatalf("after dropping staging area, expected ErrNotFound in Get. got err=%v, got value=%v", err, v)
	}
	v, err = s.Get(context.Background(), "t1", []byte("key0000"))
	if err != nil {
		// key0000 does not start with the deleted prefix - should be returned
		t.Fatalf("got unexpected error: %v", err)
	}
	it, _ := s.List(context.Background(), "t1")
	count := 0
	for it.Next() {
		count++
	}
	it.Close()
	if count != numOfValues-1000 {
		t.Errorf("got unexpected number of results after drop. expected=%d, got=%d", numOfValues-1000, count)
	}
	it, _ = s.List(context.Background(), "t2")
	count = 0
	for it.Next() {
		count++
	}
	it.Close()
	if count != numOfValues {
		t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfValues, count)
	}
}

func TestDropPrefixBytes(t *testing.T) {
	s := newTestStagingManager(t)
	tests := []struct {
		keys                    []graveler.Key
		prefix                  graveler.Key
		expectedLengthAfterDrop int
	}{
		{
			keys:                    []graveler.Key{{255, 255, 254, 254}, {255, 255, 254, 255}, {255, 255, 255, 253}, {255, 255, 255, 254}, {255, 255, 255, 255}},
			prefix:                  graveler.Key{255, 255, 255},
			expectedLengthAfterDrop: 2,
		},
		{
			keys:                    []graveler.Key{{0, 0, 0, 0}, {0, 0, 0, 255}, {0, 0, 1, 0}, {0, 0, 1, 1}},
			prefix:                  graveler.Key{0, 0, 0},
			expectedLengthAfterDrop: 2,
		},
		{
			keys:                    []graveler.Key{{0, 0, 0, 0}, {0, 0, 0, 255}, {0, 0, 1, 0}, {0, 0, 1, 1}},
			prefix:                  graveler.Key{0, 0},
			expectedLengthAfterDrop: 0,
		},
		{
			keys:                    []graveler.Key{{1, 0, 0, 0}, {1, 0, 0, 255}, {1, 0, 255, 255}, {1, 255, 255, 255}},
			prefix:                  graveler.Key{1},
			expectedLengthAfterDrop: 0,
		},
		{
			keys:                    []graveler.Key{{1, 0, 0, 0}, {1, 0, 0, 255}, {1, 0, 255, 255}, {1, 255, 255, 255}},
			prefix:                  graveler.Key{1, 0},
			expectedLengthAfterDrop: 1,
		},
		{
			keys:                    []graveler.Key{{1, 0, 0, 0}, {1, 0, 0, 255}, {1, 0, 255, 255}, {1, 255, 255, 255}},
			prefix:                  graveler.Key{1, 0, 0},
			expectedLengthAfterDrop: 2,
		},
		{
			keys:                    []graveler.Key{{1, 0, 0, 0}, {1, 0, 0, 255}, {1, 0, 255, 255}, {1, 255, 255, 255}},
			prefix:                  graveler.Key{1, 0, 0, 0},
			expectedLengthAfterDrop: 3,
		},
		{
			keys:                    []graveler.Key{{1, 0}, {1, 1}, {1, 0, 1}, {1, 1, 1}, {1, 1, 1, 255}, {1, 1, 255, 1}, {1, 1, 1, 1, 1}, {1, 1, 1, 255, 1, 1, 1, 1, 1, 1, 1, 1}},
			prefix:                  graveler.Key{1, 1, 1},
			expectedLengthAfterDrop: 4,
		},
		{
			keys:                    []graveler.Key{{1, 0}, {1, 1}, {1, 0, 1}, {1, 1, 1}, {1, 1, 1, 255}, {1, 1, 255, 1}, {1, 1, 1, 1, 1}, {1, 1, 1, 255, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 255, 255, 255, 255}},
			prefix:                  graveler.Key{1, 1, 1, 1},
			expectedLengthAfterDrop: 7,
		},
		{
			keys:                    []graveler.Key{{1, 0}, {1, 1}, {1, 0, 1}, {1, 1, 1}, {1, 1, 1, 255}, {1, 1, 255, 1}, {1, 1, 1, 1, 1}, {1, 1, 1, 255, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 255, 255, 255, 255}},
			prefix:                  graveler.Key{},
			expectedLengthAfterDrop: 0,
		},
		{
			keys:                    []graveler.Key{{1, 0}, {1, 1}, {1, 0, 1}, {1, 1, 1}, {1, 1, 1, 255}, {1, 1, 255, 1}, {1, 1, 1, 1, 1}, {1, 1, 1, 255, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 255, 255, 255, 255}},
			prefix:                  graveler.Key{0, 255, 255, 255},
			expectedLengthAfterDrop: 9,
		},
		{
			keys:                    []graveler.Key{{1, 0}, {1, 1}, {1, 0, 1}, {1, 1, 1}, {1, 1, 1, 255}, {1, 1, 255, 1}, {1, 1, 1, 1, 1}, {1, 1, 1, 255, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 255, 255, 255, 255}},
			prefix:                  graveler.Key{1, 255},
			expectedLengthAfterDrop: 9,
		},
		{
			keys:                    []graveler.Key{{1, 254, 255, 255}, {1, 255}, {1, 255, 255}, {1, 255, 255, 255}},
			prefix:                  graveler.Key{1, 255, 255},
			expectedLengthAfterDrop: 2,
		},
	}
	for i, tst := range tests {
		st := graveler.StagingToken(fmt.Sprintf("t%d", i))
		for _, k := range tst.keys {
			err := s.Set(context.Background(), st, k, graveler.Value{
				Identity: []byte{0, 0, 0, 0, 0, 0},
				Data:     []byte{0, 0, 0, 0, 0, 0},
			})
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
		}
		err := s.DropByPrefix(context.Background(), st, tst.prefix)
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		it, err := s.List(context.Background(), st)
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		count := 0
		for it.Next() {
			count++
		}
		if count != tst.expectedLengthAfterDrop {
			t.Fatalf("unexpected number of values after drop in test %d. expected=%d, got=%d", i, tst.expectedLengthAfterDrop, count)
		}
		if it.Err() != nil {
			t.Fatalf("got unexpected error: %v", it.Err())
		}
		it.Close()
	}
}

func TestList(t *testing.T) {
	s := newTestStagingManager(t)
	for _, numOfValues := range []int{1, 100, 1000, 1500, 2500} {
		token := graveler.StagingToken(fmt.Sprintf("t_%d", numOfValues))
		for i := 0; i < numOfValues; i++ {
			err := s.Set(context.Background(), token, []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("identity%d", i), fmt.Sprintf("value%d", i)))
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
		}
		res := make([]*graveler.ValueRecord, 0, numOfValues)
		it, _ := s.List(context.Background(), token)
		for it.Next() {
			res = append(res, it.Value())
		}
		if it.Err() != nil {
			t.Fatalf("got unexpected error from list: %v", it.Err())
		}
		it.Close()
		if len(res) != numOfValues {
			t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfValues, len(res))
		}
		for i, e := range res {
			if !bytes.Equal(e.Key, []byte(fmt.Sprintf("key%04d", i))) {
				t.Fatalf("got unexpected key from List at index %d: expected: key%04d, got: %s", i, i, string(e.Key))
			}
			if string(e.Data) != fmt.Sprintf("value%d", i) {
				t.Fatalf("unexpected value returned from List at index %d. expected=%s, got=%s", i, fmt.Sprintf("value%d", i), string(e.Data))
			}
		}
	}
}

func TestSeek(t *testing.T) {
	s := newTestStagingManager(t)
	numOfValues := 100
	for i := 0; i < numOfValues; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("key%04d", i)), newTestValue("identity1", "value1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	it, _ := s.List(context.Background(), "t1")
	if it.SeekGE([]byte("key0050")); !it.Next() {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("key0050")) {
		t.Fatalf("got unexpected key after iterator seek. expected=key0050, got=%s", string(it.Value().Key))
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("key0051")) {
		t.Fatalf("got unexpected key after iterator seek. expected=key0051, got=%s", string(it.Value().Key))
	}
	if it.SeekGE([]byte("key1000")); it.Next() {
		t.Fatal("iterator seek expected to return false, got true")
	}
	if it.SeekGE([]byte("key0060a")); !it.Next() {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("key0061")) {
		t.Fatalf("got unexpected key after iterator seek. expected=key0061, got=%s", string(it.Value().Key))
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
	it.Close()
}

func TestNilIdentity(t *testing.T) {
	s := newTestStagingManager(t)
	err := s.Set(context.Background(), "t1", []byte("key1"), newTestValue("identity1", "value1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	err = s.Set(context.Background(), "t1", []byte("key1"), graveler.Value{
		Identity: nil,
		Data:     []byte("value1"),
	})
	if !errors.Is(err, graveler.ErrInvalidValue) {
		t.Fatalf("got unexpected error. expected=%v, got=%v", graveler.ErrInvalidValue, err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity1" {
		t.Errorf("got wrong identity. expected=%s, got=%s", "identity1", string(e.Identity))
	}

}

func TestDeleteAndTombstone(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("key1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	tombstoneValues := []graveler.Value{
		{
			Identity: []byte("identity1"),
			Data:     make([]byte, 0),
		},
		{
			Identity: []byte("identity1"),
			Data:     nil,
		},
	}
	for _, val := range tombstoneValues {
		err = s.Set(context.Background(), "t1", []byte("key1"), val)
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		e, err := s.Get(context.Background(), "t1", []byte("key1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		if len(e.Data) != 0 {
			t.Fatalf("expected empty data, got: %v", e.Data)
		}
		if string(e.Identity) != "identity1" {
			t.Fatalf("got unexpected value identity. expected=%s, got=%s", "identity1", string(e.Identity))
		}
		it, err := s.List(context.Background(), "t1")
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		if !it.Next() {
			t.Fatalf("expected to get key from list")
		}
		if it.Err() != nil {
			t.Fatalf("unexpected error from iterator: %v", it.Err())
		}
		if len(it.Value().Value.Data) != 0 {
			t.Fatalf("expected empty value data from iterator, got: %v", it.Value().Value.Data)
		}
		it.Close()
	}
	err = s.Set(context.Background(), "t1", []byte("key1"), newTestValue("identity3", "value3"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if string(e.Identity) != "identity3" {
		t.Fatalf("got unexpected value identity. expected=%s, got=%s", "identity3", string(e.Identity))
	}
	err = s.DropKey(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	_, err = s.Get(context.Background(), "t1", []byte("key1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
}

func newTestValue(identity, data string) graveler.Value {
	return graveler.Value{
		Identity: []byte(identity),
		Data:     []byte(data),
	}
}
