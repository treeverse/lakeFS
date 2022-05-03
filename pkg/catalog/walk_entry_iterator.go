package catalog

import (
	"context"
	"errors"
	"strings"

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
// It is used for server-client communication on the status of range ingestion.
type Mark store.Mark

type EntryWithMarker struct {
	EntryRecord
	Mark
}

var (
	// ErrItClosed is used to determine the reason for the end of the walk
	ErrItClosed = errors.New("iterator closed")

	errSeekGENotSupported = errors.New("SeekGE not supported for walk entry iterator")
)

// bufferSize - buffer size of the buffer between reading entries from the blockstore Walk and passing it on
const bufferSize = 100

// WalkerFactory provides an abstraction for creating Walker
type WalkerFactory interface {
	GetWalker(ctx context.Context, opts store.WalkerOptions) (*store.WalkerWrapper, error)
}

func NewWalkEntryIterator(ctx context.Context, walker *store.WalkerWrapper, prepend, after, continuationToken string) (*walkEntryIterator, error) {
	if prepend != "" && !strings.HasSuffix(prepend, "/") {
		prepend += "/"
	}

	it := walkEntryIterator{
		entries: make(chan EntryWithMarker, bufferSize),
		walker:  walker,
		done:    make(chan bool),
		closed:  atomic.NewBool(false),
		err:     atomic.NewError(nil),
	}
	go func() {
		defer close(it.done)
		defer close(it.entries)

		err := it.walker.Walk(ctx, store.WalkOptions{
			After:             after,
			ContinuationToken: continuationToken,
		}, func(e store.ObjectStoreEntry) error {
			if it.closed.Load() {
				return ErrItClosed
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
		if !errors.Is(err, ErrItClosed) {
			it.err.Store(err)
		}
	}()

	return &it, nil
}

func (it *walkEntryIterator) Next() bool {
	if it.err.Load() != nil || it.closed.Load() {
		return false
	}

	var ok bool

	select {
	case it.curr, ok = <-it.entries:
	case <-it.done:
		// making sure not to miss entries
		it.curr, ok = <-it.entries
		if !ok {
			// entries were exhausted
			it.curr.Mark = Mark{
				LastKey: it.curr.LastKey,
				HasMore: false,
			}
		}
	}

	return ok
}

func (it *walkEntryIterator) SeekGE(Path) {
	it.err.Store(errSeekGENotSupported)
}

func (it *walkEntryIterator) Value() *EntryRecord {
	rec := it.curr.EntryRecord
	return &rec
}

func (it *walkEntryIterator) Err() error {
	return it.err.Load()
}

func (it *walkEntryIterator) Close() {
	it.closed.Store(true)

	// non-block read of last entry that might got in and is now blocking the main thread
	select {
	case <-it.entries:
		// do nothing
	default:
		// do nothing
	}

	<-it.done
}

func (it *walkEntryIterator) Marker() Mark {
	return it.curr.Mark
}
