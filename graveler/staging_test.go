package graveler

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func newTestStagingManager(t *testing.T) StagingManager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return NewStagingManager(conn)
}

func TestSetGet(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	value := newTestValue("value1")
	err = s.Set(context.Background(), "t1", []byte("a/b/c/"), value)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "9946687e5fa0dab5993ededddb398d2e" {
		t.Errorf("got wrong value. expected=%s, got=%s", "9946687e5fa0dab5993ededddb398d2e", hex.EncodeToString(e.Identity))
	}
}

func TestMultiToken(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.Set(context.Background(), "t1", []byte("a/b/c/"), newTestValue("value1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "9946687e5fa0dab5993ededddb398d2e" {
		t.Errorf("got wrong identity. expected=%s, got=%s", "9946687e5fa0dab5993ededddb398d2e", string(e.Identity))
	}
	err = s.Set(context.Background(), "t2", []byte("a/b/c/"), newTestValue("value2"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.Get(context.Background(), "t1", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "9946687e5fa0dab5993ededddb398d2e" {
		t.Errorf("got wrong value identity. expected=%s, got=%s", "9946687e5fa0dab5993ededddb398d2e", hex.EncodeToString(e.Identity))
	}
	e, err = s.Get(context.Background(), "t2", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "f066ce9385512ee02afc6e14d627e9f2" {
		t.Errorf("got wrong value identity. expected=%s, got=%s", "f066ce9385512ee02afc6e14d627e9f2", hex.EncodeToString(e.Identity))
	}
}

func TestDrop(t *testing.T) {
	s := newTestStagingManager(t)
	numOfValues := 1400
	for i := 0; i < numOfValues; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	for i := 0; i < numOfValues; i++ {
		err := s.Set(context.Background(), "t2", []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("value%d", i)))
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

func TestList(t *testing.T) {
	s := newTestStagingManager(t)
	for _, numOfValues := range []int{1, 100, 1000, 1500, 2500} {
		token := StagingToken(fmt.Sprintf("t_%d", numOfValues))
		for i := 0; i < numOfValues; i++ {
			err := s.Set(context.Background(), token, []byte(fmt.Sprintf("key%04d", i)), newTestValue(fmt.Sprintf("value%d", i)))
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
		}
		res := make([]*ValueRecord, 0, numOfValues)
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
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("key%04d", i)), newTestValue("value1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	it, _ := s.List(context.Background(), "t1")
	if !it.SeekGE([]byte("key0050")) {
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
	if it.SeekGE([]byte("key1000")) {
		t.Fatal("iterator seek expected to return false, got true")
	}
	if !it.SeekGE([]byte("key0060a")) {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("key0061")) {
		t.Fatalf("got unexpected key after iterator seek. expected=key0061, got=%s", string(it.Value().Key))
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
}

func TestDeleteAndTombstone(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("key1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.Set(context.Background(), "t1", []byte("key1"), nil)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e != nil {
		t.Fatalf("expected nil but got key: %v", e)
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
	if it.Value().Value != nil {
		t.Fatalf("expected nil value from iterator, got: %v", it.Value().Value)
	}
	it.Close()
	err = s.Set(context.Background(), "t1", []byte("key1"), newTestValue("value3"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.Get(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "039da699d091de4f1240ae50570abed9" {
		t.Fatalf("got unexpected value identity. expected=%s, got=%s", "039da699d091de4f1240ae50570abed9", hex.EncodeToString(e.Identity))
	}
	err = s.Delete(context.Background(), "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	_, err = s.Get(context.Background(), "t1", []byte("key1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
}

func newTestValue(data string) *Value {
	hash := md5.Sum([]byte(data))
	return &Value{
		Identity: hash[:],
		Data:     []byte(data),
	}
}
