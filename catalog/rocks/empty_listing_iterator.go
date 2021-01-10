package rocks

type EmptyListingIterator struct{}

func (e *EmptyListingIterator) Next() bool {
	return false
}

func (e *EmptyListingIterator) SeekGE(Path) {}

func (e *EmptyListingIterator) Value() *EntryListing {
	return nil
}

func (e *EmptyListingIterator) Err() error {
	return nil
}

func (e *EmptyListingIterator) Close() {}
