package rocks

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func newTestStagingManager(t *testing.T) StagingManager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return NewStageManager(conn)
}

func TestSetGet(t *testing.T) {
	entryMetadata := map[string]string{"metadata_field": "metadata_value"}
	s := newTestStagingManager(t)
	_, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	entry := newTestEntry("addr1")
	entry.Metadata = entryMetadata
	err = s.SetEntry(context.Background(), "t1", "a/b/c", entry)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e.Address != "addr1" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "addr1", e.Address)
	}
	if e.Size != 1000 {
		t.Errorf("got wrong entry size. expected=1000, got=%d", e.Size)
	}
	if e.Metadata == nil || e.Metadata["metadata_field"] != "metadata_value" {
		t.Errorf("got wrong entry metadata. expected=%s, got=%s", entryMetadata, e.Metadata)
	}
}

func TestMultiToken(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.SetEntry(context.Background(), "t1", "a/b/c", newTestEntry("addr1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e.Address != "addr1" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "addr1", e.Address)
	}
	err = s.SetEntry(context.Background(), "t2", "a/b/c", newTestEntry("addr2"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.GetEntry(context.Background(), "t1", "a/b/c")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e.Address != "addr1" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "addr1", e.Address)
	}
	e, err = s.GetEntry(context.Background(), "t2", "a/b/c")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e.Address != "addr2" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "addr1", e.Address)
	}
}

func TestDrop(t *testing.T) {
	s := newTestStagingManager(t)
	numOfEntries := 1400
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t1", Path(fmt.Sprintf("entry%04d", i)), newTestEntry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t2", Path(fmt.Sprintf("entry%04d", i)), newTestEntry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	err := s.Drop(context.Background(), "t1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.GetEntry(context.Background(), "t1", "entry0000")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("after dropping staging area, expected ErrNotFound in GetEntry. got err=%v, got entry=%v", err, e)
	}
	it, _ := s.ListEntries(context.Background(), "t1")
	if it.Next() {
		t.Fatal("expected staging area with token t1 to be empty, got non-empty iterator")
	}
	it.Close()
	it, _ = s.ListEntries(context.Background(), "t2")
	res := make([]*EntryRecord, 0, numOfEntries)
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
			err := s.SetEntry(context.Background(), token, Path(fmt.Sprintf("entry%04d", i)), newTestEntry("addr1"))
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
		}
		res := make([]*EntryRecord, 0, numOfEntries)
		it, _ := s.ListEntries(context.Background(), token)
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
			if e.Path != Path(fmt.Sprintf("entry%04d", i)) {
				t.Fatalf("got unexpected entry from list at index %d: expected entry%04d, got: %s", i, i, e.Path)
			}
		}
	}
}

func TestSeek(t *testing.T) {
	s := newTestStagingManager(t)
	numOfEntries := 100
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t1", Path(fmt.Sprintf("entry%04d", i)), newTestEntry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	it, _ := s.ListEntries(context.Background(), "t1")
	if !it.SeekGE("entry0050") {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if it.Value().Path != "entry0050" {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0050, got=%s", it.Value().Path)
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
	if it.Value().Path != "entry0051" {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0051, got=%s", it.Value().Path)
	}
	if it.SeekGE("entry1000") {
		t.Fatal("iterator seek expected to return false, got true")
	}
	if !it.SeekGE("entry0060a") {
		t.Fatal("iterator seek expected to return true, got false")
	}
	if it.Value().Path != "entry0061" {
		t.Fatalf("got unexpected entry after iterator seek. expected=entry0061, got=%s", it.Value().Path)
	}
	if !it.Next() {
		t.Fatal("iterator next expected to return true, got false")
	}
}

func TestDeleteAndTombstone(t *testing.T) {
	s := newTestStagingManager(t)
	_, err := s.GetEntry(context.Background(), "t1", "entry1")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.SetEntry(context.Background(), "t1", "entry1", nil)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err := s.GetEntry(context.Background(), "t1", "entry1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e != nil {
		t.Fatalf("expected nil but got entry: %v", e)
	}
	it, err := s.ListEntries(context.Background(), "t1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if !it.Next() {
		t.Fatalf("expected to get entry from list")
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error from iterator: %v", it.Err())
	}
	if it.Value().Entry != nil {
		t.Fatalf("expected nil entry from iterator, got: %v", it.Value().Entry)
	}
	it.Close()
	err = s.SetEntry(context.Background(), "t1", "entry1", newTestEntry("addr3"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	e, err = s.GetEntry(context.Background(), "t1", "entry1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if e.Address != "addr3" {
		t.Fatalf("got unexpected entry address. expected=%s, got=%s", "addr3", e.Address)
	}
	err = s.DeleteEntry(context.Background(), "t1", "entry1")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	_, err = s.GetEntry(context.Background(), "t1", "entry1")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
}

func newTestEntry(addr string) *Entry {
	return &Entry{
		LastModified: time.Now(),
		Address:      addr,
		ETag:         "abcdefghijklmnop",
		Size:         1000,
	}
}
