package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/ingest/store"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type walkEntryIterator struct {
	entries chan EntryWithMarker
	walker  *store.WalkerWrapper

	done   chan bool
	closed *atomic.Bool
	err    *atomic.Error

	curr EntryWithMarker
}

// Mark stands for pagination information when listing objects from the blockstore.
// It is used for server-client communication on the status of range ingestions.
type Mark store.Mark

type EntryWithMarker struct {
	EntryRecord
	Mark
}

// used to determined the reason for the end of the walk
var errItClosed = errors.New("iterator closed")

// buffer size of the buffer between reading entries from the blockstore Walk and passing it on
const bufferSize = 100

func newWalkEntryIterator(ctx context.Context, fromSourceURI, prepend, after, continuationToken string) (*walkEntryIterator, error) {
	if prepend != "" && prepend[len(prepend)-1:] != "/" {
		prepend += "/"
	}

	it := walkEntryIterator{
		entries: make(chan EntryWithMarker, bufferSize),

		done:   make(chan bool),
		closed: atomic.NewBool(false),
		err:    atomic.NewError(nil),
	}
	var err error
	it.walker, err = store.GetWalker(ctx, store.WalkerOptions{StorageURI: fromSourceURI})
	if err != nil {
		return nil, fmt.Errorf("creating object-store walker: %w", err)
	}

	go func() {
		err := it.walker.Walk(ctx, store.WalkOptions{
			After:             after,
			ContinuationToken: continuationToken,
		}, func(e store.ObjectStoreEntry) error {
			if it.closed.Load() {
				return errItClosed
			}

			it.entries <- EntryWithMarker{
				EntryRecord: EntryRecord{
					Path: Path(prepend + e.RelativeKey),
					Entry: &Entry{
						Address:      e.Address,
						LastModified: timestamppb.New(e.Mtime),
						Size:         e.Size,
						ETag:         e.ETag,
						AddressType:  Entry_FULL,
						ContentType:  e.Address,
					},
				},
				Mark: Mark(it.walker.Marker()),
			}
			return nil
		})
		if err == nil {
			it.curr.Mark = Mark{
				ContinuationToken: "",
				HasMore:           false,
			}
		} else if !errors.Is(err, errItClosed) {
			it.err.Store(err)
		}
		close(it.done)
	}()

	return &it, nil
}

func (it walkEntryIterator) Next() bool {
	if it.err.Load() != nil || it.closed.Load() {
		return false
	}

	var ok bool
	it.curr, ok = <-it.entries
	return ok
}

func (it walkEntryIterator) SeekGE(Path) {
	// unsupported
}

func (it walkEntryIterator) Value() *EntryRecord {
	return &it.curr.EntryRecord
}

func (it walkEntryIterator) Err() error {
	return it.err.Load()
}

func (it walkEntryIterator) Close() {
	it.closed.Store(true)

	// non-block read of last entry that might got in
	select {
	case <-it.entries:
		// do nothing
	default:
		// do nothing
	}

	<-it.done
}

func (it walkEntryIterator) Marker() Mark {
	return it.curr.Mark
}
