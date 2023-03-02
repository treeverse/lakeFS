package testutils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ingest/store"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/testutil"
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

type FakeKVEntryIterator struct {
	Entries []*kv.Entry
	Error   error
	index   int
}

func NewFakeKVEntryIterator(entries []*kv.Entry) *FakeKVEntryIterator {
	return &FakeKVEntryIterator{
		Entries: entries,
		index:   -1,
	}
}

func (f *FakeKVEntryIterator) Next() bool {
	if f.Error != nil {
		return false
	}
	f.index++
	return f.index < len(f.Entries)
}

func (f *FakeKVEntryIterator) Entry() *kv.Entry {
	if f.Error != nil || f.index < 0 || f.index >= len(f.Entries) {
		return nil
	}
	return f.Entries[f.index]
}

func (f *FakeKVEntryIterator) Err() error {
	return f.Error
}

func (f *FakeKVEntryIterator) Close() {}

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
	Entries                         []block.ObjectStoreEntry
	curr                            int
	Max                             int
	uriPrefix                       string
	expectedAfter                   string
	expectedContinuationToken       string
	expectedFromSourceURIWithPrefix string
	err                             error
}

const (
	randomKeyLength = 64
	entrySize       = 132
)

func (w *FakeWalker) createEntries(count int) {
	ents := make([]block.ObjectStoreEntry, count)

	// Use same sequence to overcome Graveler ability to create small ranges.
	// Calling test functions rely on Graveler to not break on the first 1000 entries.
	// For example, setting "5" here will cause the test to constantly fail.
	// Fix Bug #3384
	const seed = 6
	//nolint:gosec
	randGen := rand.New(rand.NewSource(seed))
	for i := 0; i < count; i++ {
		relativeKey := testutil.RandomString(randGen, randomKeyLength)
		fullkey := w.uriPrefix + "/" + relativeKey
		ents[i] = block.ObjectStoreEntry{
			RelativeKey: relativeKey,
			FullKey:     fullkey,
			Address:     w.expectedFromSourceURIWithPrefix + "/" + relativeKey,
			ETag:        "some_etag",
			Mtime:       time.Time{},
			Size:        entrySize,
		}
	}
	sort.Slice(ents, func(i, j int) bool {
		return ents[i].RelativeKey < ents[j].RelativeKey
	})
	w.Entries = ents
}

var errUnexpectedValue = errors.New("unexpected value")

func (w *FakeWalker) Walk(_ context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	if w.expectedAfter != op.After {
		return fmt.Errorf("(after) expected %s, got %s: %w", w.expectedAfter, op.After, errUnexpectedValue)
	}
	if w.expectedContinuationToken != op.ContinuationToken {
		return fmt.Errorf("(continuationToken) expected %s, got %s: %w", w.expectedContinuationToken, op.ContinuationToken, errUnexpectedValue)
	}
	if w.expectedFromSourceURIWithPrefix != storageURI.String() {
		return fmt.Errorf("(fromSourceURIWithPrefix) expected %s, got %s: %w", w.expectedFromSourceURIWithPrefix, storageURI.String(), errUnexpectedValue)
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
				return fmt.Errorf("expected error; expected (%s), got: %w", catalog.ErrItClosed, err)
			}
		}
	}
	return nil
}

const ContinuationTokenOpaque = "FakeContToken"

func (w *FakeWalker) Marker() block.Mark {
	token := ""
	if w.curr < len(w.Entries)-1 {
		token = ContinuationTokenOpaque
	}
	return block.Mark{
		LastKey:           w.Entries[w.curr].FullKey,
		HasMore:           w.curr < len(w.Entries)-1,
		ContinuationToken: token,
	}
}
