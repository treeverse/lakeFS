package testutils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

type FakeValueIterator struct {
	Records []*graveler.ValueRecord
	Error   error
	index   int
}

func (f *FakeValueIterator) Next() bool {
	if f.Error != nil {
		return false
	}
	f.index++
	return f.index < len(f.Records)
}

func (f *FakeValueIterator) SeekGE(id graveler.Key) {
	if f.Error != nil {
		return
	}
	for i, ent := range f.Records {
		if bytes.Compare(ent.Key, id) >= 0 {
			f.index = i - 1
			return
		}
	}
}

func (f *FakeValueIterator) Value() *graveler.ValueRecord {
	if f.Error != nil || f.index < 0 || f.index >= len(f.Records) {
		return nil
	}
	return f.Records[f.index]
}

func (f *FakeValueIterator) Err() error {
	return f.Error
}

func (f *FakeValueIterator) Close() {}

func NewFakeValueIterator(records []*graveler.ValueRecord) *FakeValueIterator {
	return &FakeValueIterator{
		Records: records,
		Error:   nil,
		index:   -1,
	}
}

type FakeEntryIterator struct {
	Entries []*catalog.EntryRecord
	Error   error
	index   int
}

func NewFakeEntryIterator(entries []*catalog.EntryRecord) *FakeEntryIterator {
	return &FakeEntryIterator{
		Entries: entries,
		index:   -1,
	}
}

func (f *FakeEntryIterator) Next() bool {
	if f.Error != nil {
		return false
	}
	f.index++
	return f.index < len(f.Entries)
}

func (f *FakeEntryIterator) SeekGE(id catalog.Path) {
	if f.Error != nil {
		return
	}
	for i, ent := range f.Entries {
		if ent.Path >= id {
			f.index = i - 1
			return
		}
	}
}

func (f *FakeEntryIterator) Value() *catalog.EntryRecord {
	if f.Error != nil || f.index < 0 || f.index >= len(f.Entries) {
		return nil
	}
	return f.Entries[f.index]
}

func (f *FakeEntryIterator) Err() error {
	return f.Error
}

func (f *FakeEntryIterator) Close() {}

type FakeFactory struct {
	Walker *FakeWalker
}

func (f FakeFactory) GetWalker(_ context.Context, op store.WalkerOptions) (*store.WalkerWrapper, error) {
	u, _ := url.Parse(op.StorageURI)
	return store.NewWrapper(f.Walker, u), nil
}

func NewFakeWalker(count, max int, uriPrefix, expectedAfter, expectedContinuationToken, expectedFromSourceURIWithPrefix string, err error) *FakeWalker {
	w := &FakeWalker{
		Max:                             max,
		uriPrefix:                       uriPrefix,
		expectedAfter:                   expectedAfter,
		expectedContinuationToken:       expectedContinuationToken,
		expectedFromSourceURIWithPrefix: expectedFromSourceURIWithPrefix,
		err:                             err,
	}
	w.createEntries(count)
	return w
}

type FakeWalker struct {
	Entries                         []store.ObjectStoreEntry
	curr                            int
	Max                             int
	uriPrefix                       string
	expectedAfter                   string
	expectedContinuationToken       string
	expectedFromSourceURIWithPrefix string
	err                             error
}

func (w *FakeWalker) createEntries(count int) {
	ents := make([]store.ObjectStoreEntry, count)
	for i := 0; i < count; i++ {
		relativeKey := randstr.Base64(64)
		fullkey := w.uriPrefix + "/" + relativeKey
		ents[i] = store.ObjectStoreEntry{
			RelativeKey: relativeKey,
			FullKey:     fullkey,
			Address:     w.expectedFromSourceURIWithPrefix + "/" + relativeKey,
			ETag:        "some_etag",
			Mtime:       time.Time{},
			Size:        132,
		}
	}
	sort.Slice(ents[:], func(i, j int) bool {
		return ents[i].RelativeKey < ents[j].RelativeKey
	})
	w.Entries = ents
}

func (w *FakeWalker) Walk(_ context.Context, storageURI *url.URL, op store.WalkOptions, walkFn func(e store.ObjectStoreEntry) error) error {
	if w.expectedAfter != op.After {
		return errors.New(fmt.Sprintf("after; expected %s, got %s", w.expectedAfter, op.After))
	}
	if w.expectedContinuationToken != op.ContinuationToken {
		return errors.New(fmt.Sprintf("continuationToken; expected %s, got %s", w.expectedContinuationToken, op.ContinuationToken))
	}
	if w.expectedFromSourceURIWithPrefix != storageURI.String() {
		return errors.New(fmt.Sprintf("fromSourceURIWithPrefix; expected %s, got %s", w.expectedFromSourceURIWithPrefix, storageURI.String()))
	}
	if w.err != nil {
		return w.err
	}

	for i, e := range w.Entries {
		w.curr = i
		if err := walkFn(e); err != nil {
			if i < w.Max {
				return fmt.Errorf("didn't expect walk err: %w", err)
			}
			if i >= w.Max && !errors.Is(err, catalog.ErrItClosed) {
				return errors.New(fmt.Sprintf("expected error; expected (%s), got (%s)", catalog.ErrItClosed, err))
			}
		}
	}
	return nil
}

const ContinuationTokenOpaque = "FakeContToken"

func (w *FakeWalker) Marker() store.Mark {
	return store.Mark{
		LastKey:           w.Entries[w.curr].FullKey,
		HasMore:           w.curr < len(w.Entries)-1,
		ContinuationToken: ContinuationTokenOpaque,
	}
}
