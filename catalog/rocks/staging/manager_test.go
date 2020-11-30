package staging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/testutil"
)

func prepareMgr(t *testing.T) rocks.StagingManager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return NewManager(conn)
}

func TestSetGet(t *testing.T) {
	s := prepareMgr(t)
	_, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.SetEntry(context.Background(), "t1", "a/b/c", entry("addr1"))
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	entry, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if entry.Address != "addr1" {
		t.Errorf("got wrong entry address. expected=%s, got=%s", "addr1", entry.Address)
	}
}

func TestMultiToken(t *testing.T) {
	s := prepareMgr(t)
	_, err := s.GetEntry(context.Background(), "t1", "a/b/c")
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("error different than expected. expected=%v, got=%v", db.ErrNotFound, err)
	}
	err = s.SetEntry(context.Background(), "t1", "a/b/c", entry("addr1"))
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
	err = s.SetEntry(context.Background(), "t2", "a/b/c", entry("addr2"))
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
	s := prepareMgr(t)
	numOfEntries := 1400
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t1", rocks.Path(fmt.Sprintf("entry%04d", i)), entry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t2", rocks.Path(fmt.Sprintf("entry%04d", i)), entry("addr1"))
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
	it, _ := s.ListEntries(context.Background(), "t1", "")
	if it.Next() {
		t.Fatal("expected staging area with token t1 to be empty, got non-empty iterator")
	}
	it, _ = s.ListEntries(context.Background(), "t2", "")
	res := make([]*rocks.EntryRecord, 0, numOfEntries)
	for it.Next() {
		res = append(res, it.Value())
	}
	if len(res) != numOfEntries {
		t.Errorf("got unexpected number of results. expected=%d, got=%d", numOfEntries, len(res))
	}
}

func TestList(t *testing.T) {
	s := prepareMgr(t)
	numOfEntries := 100
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t1", rocks.Path(fmt.Sprintf("entry%04d", i)), entry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	res := make([]*rocks.EntryRecord, 0, numOfEntries)
	it, _ := s.ListEntries(context.Background(), "t1", "")
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
		if e.Path != rocks.Path(fmt.Sprintf("entry%04d", i)) {
			t.Fatalf("got unexpected entry from list at index %d: expected entry%04d, got: %s", i, i, e.Path)
		}
	}
}

func TestSeek(t *testing.T) {
	s := prepareMgr(t)
	numOfEntries := 100
	for i := 0; i < numOfEntries; i++ {
		err := s.SetEntry(context.Background(), "t1", rocks.Path(fmt.Sprintf("entry%04d", i)), entry("addr1"))
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
	it, _ := s.ListEntries(context.Background(), "t1", "")
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
	s := prepareMgr(t)
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
	err = s.SetEntry(context.Background(), "t1", "entry1", entry("addr3"))
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

func entry(addr string) *rocks.Entry {
	return &rocks.Entry{
		LastModified: time.Now(),
		Address:      addr,
		Metadata:     nil,
		ETag:         "abcdefghijklmnop",
		Size:         1000,
	}
}
