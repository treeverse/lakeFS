package committed

import (
	"github.com/treeverse/lakefs/graveler"
)

type RangeIterator struct {
	it    ValueIterator
	err   error
	value *graveler.ValueRecord
}

func NewRangeIterator(it ValueIterator) *RangeIterator {
	return &RangeIterator{
		it: it,
	}
}

func (r *RangeIterator) Next() bool {
	if !r.it.Next() {
		r.err = r.it.Err()
		r.value = nil
		return false
	}
	val := r.it.Value()
	// unmarshal value
	var v *graveler.Value
	if val.Value != nil {
		v, r.err = UnmarshalValue(val.Value)
		if r.err != nil {
			r.value = nil
			return false
		}
	}
	r.value = &graveler.ValueRecord{
		Key:   graveler.Key(val.Key),
		Value: v,
	}
	return true
}

func (r *RangeIterator) SeekGE(id graveler.Key) {
	r.it.SeekGE(Key(id))
	r.value = nil
	r.err = r.Err()
}

func (r *RangeIterator) Value() *graveler.ValueRecord {
	if r.err != nil {
		return nil
	}
	return r.value
}

func (r *RangeIterator) Err() error {
	return r.err
}

func (r *RangeIterator) Close() {
	r.it.Close()
}
