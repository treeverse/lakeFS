package graveler

import (
	"bytes"
	"context"
	"errors"

	"github.com/treeverse/lakefs/logging"
)

// PrefixIterator holds a ValueIterator and iterates only over values the their Key starts with the prefix
type PrefixIterator struct {
	prefix        Key
	valueIterator ValueIterator
	current       *ValueRecord
}

func NewPrefixIterator(iterator ValueIterator, prefix Key) *PrefixIterator {
	iterator.SeekGE(prefix)
	return &PrefixIterator{
		prefix:        prefix,
		valueIterator: iterator,
		current:       nil,
	}
}

func (p *PrefixIterator) Next() bool {
	if !p.valueIterator.Next() {
		p.current = nil
		return false
	}
	val := p.valueIterator.Value()
	if !bytes.HasPrefix(val.Key, p.prefix) {
		p.current = nil
		return false
	}
	p.current = val
	return true
}

func (p *PrefixIterator) SeekGE(id Key) {
	if bytes.Compare(id, p.prefix) <= 0 {
		p.valueIterator.SeekGE(p.prefix)
		return
	}
	p.valueIterator.SeekGE(id)
}

func (p *PrefixIterator) Value() *ValueRecord {
	return p.current
}

func (p *PrefixIterator) Err() error {
	return p.valueIterator.Err()
}

func (p *PrefixIterator) Close() {
	p.valueIterator.Close()
}

// ListingIter implements a listing iterator using a ValueIterator
// assumes all values in valueIterator start with prefix
type ListingIter struct {
	valueIterator ValueIterator
	delimiter     Key
	nextDelimiter Key
	prefix        Key
	current       *Listing
	nextFunc      func(l *ListingIter) bool
	err           error
}

// getFollowingValue returns the following value (i.e will increase the last byte by 1)
// in the following cases will return received value : value is nil, value length is 0, last byte is maximum byte
func getFollowingValue(value []byte) []byte {
	if value == nil || len(value) == 0 || value[len(value)-1] == 255 {
		return value
	}
	copiedDelimiter := make([]byte, len(value))
	copy(copiedDelimiter, value)
	return append(copiedDelimiter[:len(copiedDelimiter)-1], copiedDelimiter[len(copiedDelimiter)-1]+1)
}

func NewListingIter(iterator ValueIterator, delimiter, prefix Key) *ListingIter {
	var nextDelimiter Key
	var nextFunc func(l *ListingIter) bool
	if len(delimiter) == 0 {
		nextFunc = nextNoDelimiter
	} else {
		nextFunc = nextWithDelimiter
		nextDelimiter = getFollowingValue(delimiter)
	}

	return &ListingIter{
		valueIterator: iterator,
		delimiter:     delimiter,
		nextDelimiter: nextDelimiter,
		prefix:        prefix,
		nextFunc:      nextFunc,
	}
}

func nextNoDelimiter(l *ListingIter) bool {
	hasNext := l.valueIterator.Next()
	if !hasNext {
		l.current = nil
		return false
	}
	val := l.valueIterator.Value()
	l.current = &Listing{
		CommonPrefix: false,
		Key:          val.Key,
		Value:        val.Value,
	}
	return true
}

func nextWithDelimiter(l *ListingIter) bool {
	var hasNext bool
	if l.current == nil || !l.current.CommonPrefix {
		hasNext = l.valueIterator.Next()
	} else {
		nextKey := append(l.current.Key, l.nextDelimiter...)
		l.valueIterator.SeekGE(nextKey)
		hasNext = l.valueIterator.Next()
	}

	if hasNext {
		nextValue := l.valueIterator.Value()
		if !bytes.HasPrefix(nextValue.Key, l.prefix) {
			l.current = nil
			l.err = ErrUnexpected
			return false
		}
		l.current = l.getListingFromValue(nextValue.Value, nextValue.Key)
	} else {
		l.current = nil
	}
	return hasNext
}

func (l *ListingIter) Next() bool {
	return l.nextFunc(l)
}

func (l *ListingIter) getListingFromValue(value *Value, key Key) *Listing {
	relevantPath := key[len(l.prefix):]
	delimiterIndex := bytes.Index(relevantPath, l.delimiter)
	commonPrefix := delimiterIndex >= 0
	if commonPrefix {
		relevantPath = relevantPath[:delimiterIndex]
		value = nil
	}
	newKey := append(l.prefix, relevantPath...)
	return &Listing{
		Key:          newKey,
		CommonPrefix: commonPrefix,
		Value:        value,
	}
}

func (l *ListingIter) SeekGE(id Key) {
	l.current = nil
	l.valueIterator.SeekGE(id)
}

func (l *ListingIter) Value() *Listing {
	return l.current
}

func (l *ListingIter) Err() error {
	if l.err != nil {
		return l.err
	}
	return l.valueIterator.Err()
}

func (l *ListingIter) Close() {
	l.valueIterator.Close()
}

// CombinedIterator iterates over two listing iterators,
// in case of duplication (in values or in errors) returns value in iterA
type CombinedIterator struct {
	iterA ListingIterator
	iterB ListingIterator
	p     ListingIterator
	err   error
}

func NewCombinedIterator(iterA, iterB ListingIterator) *CombinedIterator {
	return &CombinedIterator{
		iterA: iterA,
		iterB: iterB,
		p:     nil,
		err:   nil,
	}
}

func (c *CombinedIterator) Next() bool {
	// call next with the relevant iterators
	valA := c.iterA.Value()
	valB := c.iterB.Value()

	switch {
	case c.p == nil:
		// first
		c.iterA.Next()
		c.iterB.Next()
	case valA == nil && valB == nil:
		// last
		return false
	case valA == nil:
		c.p = c.iterB
		return c.iterB.Next()
	case valB == nil:
		c.p = c.iterA
		return c.iterA.Next()
	case bytes.Equal(valA.Key, valB.Key):
		c.iterA.Next()
		c.iterB.Next()
	case bytes.Compare(valA.Key, valB.Key) < 0:
		c.iterA.Next()
	default:
		// value of iterA < value of iterB
		c.iterB.Next()
	}

	if c.iterA.Err() != nil || c.iterB.Err() != nil {
		return false
	}
	// get current pointer
	valA = c.iterA.Value()
	valB = c.iterB.Value()
	switch {
	case valA == nil && valB == nil:
		c.p = c.iterA // in order not to be stuck in start state
		return false

	case valA == nil:
		c.p = c.iterB
		return true

	case valB == nil:
		c.p = c.iterA
		return true

	case bytes.Compare(valA.Key, valB.Key) <= 0:
		c.p = c.iterA
		return true
	default:
		c.p = c.iterB
	}
	return true
}

func (c *CombinedIterator) SeekGE(id Key) {
	c.p = nil
	c.iterA.SeekGE(id)
	c.iterB.SeekGE(id)
}

func (c *CombinedIterator) Value() *Listing {
	if c.p == nil {
		return nil
	}
	return c.p.Value()
}

func (c *CombinedIterator) Err() error {
	if c.iterA.Err() != nil {
		return c.iterA.Err()
	}
	return c.iterB.Err()
}

func (c *CombinedIterator) Close() {
	c.iterA.Close()
	c.iterB.Close()
}

type UncommittedDiffIterator struct {
	committedManager CommittedManager
	list             ValueIterator
	sn               StorageNamespace
	treeID           TreeID
	value            *Diff
	err              error
}

func newUncommittedDiffIterator(manager CommittedManager, list ValueIterator, sn StorageNamespace, treeId TreeID) *UncommittedDiffIterator {
	return &UncommittedDiffIterator{
		committedManager: manager,
		list:             list,
		sn:               sn,
		treeID:           treeId,
	}
}

func valueExistsInCommitted(ctx context.Context, committedManager CommittedManager, sn StorageNamespace, treeID TreeID, key Key) (bool, error) {
	_, err := committedManager.Get(ctx, sn, treeID, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func getDiffType(ctx context.Context, committedManager CommittedManager, sn StorageNamespace, treeID TreeID, key Key, tombstone bool) (DiffType, error) {
	existsInCommitted, err := valueExistsInCommitted(ctx, committedManager, sn, treeID, key)
	if err != nil {
		return 0, err
	}
	var diffType DiffType
	diffType = DiffTypeAdded
	if tombstone && existsInCommitted {
		diffType = DiffTypeRemoved
	}
	if tombstone && !existsInCommitted {
		logging.Default().WithFields(logging.Fields{"treeID": treeID, "stagingToken": sn, "key": key}).Warn("tombstone for file that does not exist")
		diffType = DiffTypeRemoved
	}
	if !tombstone && existsInCommitted {
		diffType = DiffTypeChanged
	}
	if !tombstone && !existsInCommitted {
		diffType = DiffTypeAdded
	}
	return diffType, nil
}

func (d *UncommittedDiffIterator) Next() bool {
	if !d.list.Next() {
		d.value = nil
		return false
	}
	val := d.list.Value()
	diffType, err := getDiffType(context.Background(), d.committedManager, d.sn, d.treeID, val.Key, val.Value == nil)
	if err != nil {
		d.value = nil
		d.err = err
		return false
	}
	d.value = &Diff{
		Type:  diffType,
		Key:   val.Key,
		Value: val.Value,
	}
	return true
}

func (d *UncommittedDiffIterator) SeekGE(id Key) {
	d.value = nil
	d.list.SeekGE(id)
}

func (d *UncommittedDiffIterator) Value() *Diff {
	return d.value
}

func (d *UncommittedDiffIterator) Err() error {
	return d.err
}

func (d *UncommittedDiffIterator) Close() {
	d.list.Close()
}
