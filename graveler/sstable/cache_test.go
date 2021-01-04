package sstable_test

import (
	"strings"
	"testing"

	pebble_sst "github.com/cockroachdb/pebble/sstable"
	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"
)

// marker is an item that has an unusable SSTable reader but fakes behaviour on Close.
type marker struct {
	t      testing.TB
	closed bool
}

func (m *marker) Close() error {
	if m.closed {
		m.t.Errorf("failed to close %v", m)
	}
	m.closed = true
	return nil
}

func (m *marker) GetSSTable() *pebble_sst.Reader {
	return nil
}

type namespaceID struct {
	namespace string
	id        committed.ID
}

type fakeOpener struct {
	t      testing.TB
	byName map[namespaceID]*marker
}

func NewFakeOpener(t *testing.T, names []namespaceID) *fakeOpener {
	ret := &fakeOpener{
		t:      t,
		byName: make(map[namespaceID]*marker, len(names)),
	}
	for _, name := range names {
		m := &marker{t: t}
		ret.byName[name] = m
	}
	return ret
}

func (fo *fakeOpener) Open(namespace string, id string) (sstable.Item, error) {
	return fo.byName[namespaceID{namespace, sstable.ID(id)}], nil
}

func (fo *fakeOpener) Exists(namespace string, id string) (bool, error) {
	_, ok := fo.byName[namespaceID{namespace, sstable.ID(id)}]
	return ok, nil
}

func TestCacheGet(t *testing.T) {
	nids := []namespaceID{{"foo", "a"}, {"bar", "a"}, {"foo", "b-dontclose"}}
	fo := NewFakeOpener(t, nids)
	c := sstable.NewCacheWithOpener(
		lru.ParamsWithDisposal{Name: t.Name(), Size: 50, Shards: 3},
		fo.Open,
		fo.Exists,
	)

	// TODO(ariels): Add error
	for _, nid := range nids {
		_, deref, err := c.GetOrOpen(nid.namespace, nid.id)
		if err != nil {
			t.Error(err)
		}
		if !strings.HasSuffix(string(nid.id), "dontclose") {
			err = deref()
			if err != nil {
				t.Error(err)
			}
		}
	}

	// Verify the right files got closed
	for _, nid := range nids {
		m := fo.byName[nid]
		if m.closed == strings.HasSuffix(string(nid.id), "dontclose") {
			not := ""
			if !m.closed {
				not = " not"
			}
			t.Errorf("got that %+v was%s closed", c, not)
		}
	}
}
