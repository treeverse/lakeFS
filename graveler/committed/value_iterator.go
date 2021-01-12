package committed

import "github.com/treeverse/lakefs/graveler"

type valueIterator struct {
	it Iterator
}

func (v *valueIterator) Next() bool {
	for v.it.Next() {
		if val, _ := v.it.Value(); val != nil {
			return true
		}
	}
	return false
}

func (v *valueIterator) SeekGE(id graveler.Key) {
	v.it.SeekGE(id)
}

func (v *valueIterator) Value() *graveler.ValueRecord {
	rec, _ := v.it.Value()
	return rec
}

func (v *valueIterator) Err() error {
	return v.it.Err()
}

func (v *valueIterator) Close() {
	v.it.Close()
}

func NewValueIterator(it Iterator) graveler.ValueIterator {
	return &valueIterator{
		it: it,
	}
}
