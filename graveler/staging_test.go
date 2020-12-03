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
	entry := newTestValue("value1")
	err = s.Set(context.Background(), "t1", []byte("a/b/c/"), entry)
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
		t.Errorf("got wrong entry address. expected=%s, got=%s", "9946687e5fa0dab5993ededddb398d2e", string(e.Identity))
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
		t.Errorf("got wrong entry address. expected=%s, got=%s", "9946687e5fa0dab5993ededddb398d2e", hex.EncodeToString(e.Identity))
	}
	e, err = s.Get(context.Background(), "t2", []byte("a/b/c/"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "f066ce9385512ee02afc6e14d627e9f2" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "f066ce9385512ee02afc6e14d627e9f2", hex.EncodeToString(e.Identity))
	}
}

func TestDrop(t *testing.T) {
	s := newTestStagingManager(t)
	numOfEntries := 1400
	for i := 0; i < numOfEntries; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("entry%04d", i)), newTestValue("value1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	for i := 0; i < numOfEntries; i++ {
		err := s.Set(context.Background(), "t2", []byte(fmt.Sprintf("entry%04d", i)), newTestValue("value1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	err := s.Drop(context.Background(), "t1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("entry0000"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("after dropping staging area, expected ErrNotFound in Get. got err=%v, got entry=%v", err, e)
	}
	it, _ := s.List(context.Background(), "t1")
	if it.Next() {
		t.Fatal("expected staging area with token t1 to be empty, got non-empty iterator")
	}
	it.Close()
	it, _ = s.List(context.Background(), "t2")
	res := make([]*ValueRecord, 0, numOfEntries)
	for it.Next() {
		res = append(res, it.Value())
	}
	it.Close()
	if len(res) != numOfEntries {
		t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfEntries, len(res))
	}

}

func TestList(t *testing.T) {
	s := newTestStagingManager(t)
	for _, numOfEntries := range []int{1, 100, 1000, 1500, 2500} {
		token := StagingToken(fmt.Sprintf("t_%d", numOfEntries))
		for i := 0; i < numOfEntries; i++ {
			err := s.Set(context.Background(), token, []byte(fmt.Sprintf("entry%04d", i)), newTestValue("value1"))
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
		}
		res := make([]*ValueRecord, 0, numOfEntries)
		it, _ := s.List(context.Background(), token)
		for it.Next() {
			res = append(res, it.Value())
		}
		if it.Err() != nil {
			t.Fatalf("got unexpected error from list: %v", it.Err())
		}
		if len(res) != numOfEntries {
			t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfEntries, len(res))
		}
		for i, e := range res {
			if !bytes.Equal(e.Key, []byte(fmt.Sprintf("entry%04d", i))) {
				t.Fatalf("got unexpected entry from list at index %d: expected entry%04d, got: %s", i, i, string(e.Key))
			}
		}
	}
}

func TestSeek(t *testing.T) {
	s := newTestStagingManager(t)
	numOfEntries := 100
	for i := 0; i < numOfEntries; i++ {
		err := s.Set(context.Background(), "t1", []byte(fmt.Sprintf("entry%04d", i)), newTestValue("value1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	it, _ := s.List(context.Background(), "t1")
	if !it.SeekGE([]byte("entry0050")) {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("entry0050")) {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0050, got=%s", string(it.Value().Key))
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("entry0051")) {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0051, got=%s", string(it.Value().Key))
	}
	if it.SeekGE([]byte("entry1000")) {
		t.Fatal("iterator seek expected to return false, got true")
	}
	if !it.SeekGE([]byte("entry0060a")) {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if !bytes.Equal(it.Value().Key, []byte("entry0061")) {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0061, got=%s", string(it.Value().Key))
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
}

func TestDeleteAndTombstone(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.Get(context.Background(), "t1", []byte("entry1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.Set(context.Background(), "t1", []byte("entry1"), nil)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.Get(context.Background(), "t1", []byte("entry1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e != nil {
		t.Fatalf("expected nil but got entry: %v", e)
	}
	it, err := s.List(context.Background(), "t1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if !it.Next() {
		t.Fatalf("expected to get entry from list")
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error from iterator: %v", it.Err())
	}
	if it.Value().Value != nil {
		t.Fatalf("expected nil entry from iterator, got: %v", it.Value().Value)
	}
	it.Close()
	err = s.Set(context.Background(), "t1", []byte("entry1"), newTestValue("value3"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.Get(context.Background(), "t1", []byte("entry1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if hex.EncodeToString(e.Identity) != "039da699d091de4f1240ae50570abed9" {
		t.Fatalf("got unexpected entry address. expected=%s, got=%s", "039da699d091de4f1240ae50570abed9", hex.EncodeToString(e.Identity))
	}
	err = s.Delete(context.Background(), "t1", []byte("entry1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	_, err = s.Get(context.Background(), "t1", []byte("entry1"))
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
