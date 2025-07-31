package graveler

func NewEmptyValueIterator() ValueIterator {
	return &emptyValueIterator{}
}

type emptyValueIterator struct{}

func (e *emptyValueIterator) SeekGE(_ Key) {
}

func (e *emptyValueIterator) Value() *ValueRecord {
	return nil
}

func (e *emptyValueIterator) Err() error {
	return nil
}

func (e *emptyValueIterator) Close() {
}

func (e *emptyValueIterator) Next() bool {
	return false
}
