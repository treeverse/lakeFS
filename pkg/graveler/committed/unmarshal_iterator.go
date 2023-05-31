package committed

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

// UnmarshalIterator wrap value iterator and unmarshal each value
type UnmarshalIterator struct {
	it    ValueIterator
	value *graveler.ValueRecord
	err   error
}

func NewUnmarshalIterator(it ValueIterator) *UnmarshalIterator {
	return &UnmarshalIterator{
		it: it,
	}
}

func (r *UnmarshalIterator) Next() bool {
	if !r.it.Next() {
		r.err = r.it.Err()
		r.value = nil
		return false
	}
	val := r.it.Value()
	// unmarshal value
	var v *graveler.Value
	if val.Value != nil {
		v, r.err = UnmarshalValue(val.Value, "", "", graveler.Key(""))
		if r.err != nil {
			r.value = nil
			return false
		}
	}
	r.value = &graveler.ValueRecord{
		Key:   graveler.Key(val.Key.Copy()),
		Value: v,
	}
	return true
}

func (r *UnmarshalIterator) SeekGE(id graveler.Key) {
	r.it.SeekGE(Key(id))
	r.value = nil
	r.err = r.Err()
}

func (r *UnmarshalIterator) Value() *graveler.ValueRecord {
	if r.err != nil {
		return nil
	}
	return r.value
}

func (r *UnmarshalIterator) Err() error {
	return r.err
}

func (r *UnmarshalIterator) Close() {
	r.it.Close()
}
