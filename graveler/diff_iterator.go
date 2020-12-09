package graveler

import "bytes"

type ValueDiffIterator struct {
	left        ValueIterator
	right       ValueIterator
	leftNext    bool
	rightNext   bool
	currentVal  *ValueRecord
	currentType DiffType
}

func NewValueDiffIterator(left ValueIterator, right ValueIterator) *ValueDiffIterator {
	it := &ValueDiffIterator{left: left, right: right}
	it.leftNext = it.left.Next()
	it.rightNext = it.right.Next()
	return it
}

func (d *ValueDiffIterator) Next() bool {
	for {
		if d.left.Err() != nil || d.right.Err() != nil {
			return false
		}
		if d.rightNext && d.leftNext {
			leftKey := d.left.Value().Key
			rightKey := d.right.Value().Key
			if bytes.Equal(leftKey, rightKey) {
				leftVal := d.left.Value()
				rightVal := d.right.Value()
				d.rightNext = d.right.Next()
				d.leftNext = d.left.Next()
				if !bytes.Equal(leftVal.Value.Identity, rightVal.Value.Identity) {
					d.currentVal = rightVal
					d.currentType = DiffTypeChanged
					return true
				}
				continue // identical values
			} else if bytes.Compare(leftKey, rightKey) < 0 {
				d.currentVal = d.left.Value()
				d.leftNext = d.left.Next()
				d.currentType = DiffTypeRemoved
				return true
			} else { // leftKey > rightKey
				d.currentVal = d.right.Value()
				d.rightNext = d.right.Next()
				d.currentType = DiffTypeAdded
				return true
			}
		} else if d.leftNext {
			d.currentVal = d.left.Value()
			d.leftNext = d.left.Next()
			d.currentType = DiffTypeRemoved
			return true
		} else if d.rightNext {
			d.currentVal = d.right.Value()
			d.rightNext = d.right.Next()
			d.currentType = DiffTypeAdded
			return true
		}
		return false
	}
}

func (d *ValueDiffIterator) SeekGE(id Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.currentType = 0
}

func (d *ValueDiffIterator) Value() *Diff {
	return &Diff{
		Type:  d.currentType,
		Key:   d.currentVal.Key,
		Value: d.currentVal.Value,
	}
}

func (d *ValueDiffIterator) Err() error {
	if d.left.Err() != nil {
		return d.left.Err()
	}
	return d.right.Err()
}

func (d *ValueDiffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
