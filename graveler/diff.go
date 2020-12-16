package graveler

import (
	"bytes"
	"fmt"
)

type diffIterator struct {
	left        ValueIterator
	right       ValueIterator
	leftNext    bool
	rightNext   bool
	currentVal  *Diff
	currentType DiffType
	err         error
}

func NewDiffIterator(left ValueIterator, right ValueIterator) DiffIterator {
	it := &diffIterator{left: left, right: right}
	it.leftNext = it.left.Next()
	it.rightNext = it.right.Next()
	return it
}

func (d *diffIterator) compareKeys() int {
	if d.leftNext && !d.rightNext {
		return -1
	}
	if !d.leftNext && d.rightNext {
		return 1
	}
	return bytes.Compare(d.left.Value().Key, d.right.Value().Key)
}

func (d *diffIterator) Next() bool {
	for d.leftNext || d.rightNext {
		if d.left.Err() != nil {
			d.err = fmt.Errorf("failed in left tree: %w", d.left.Err())
			return false
		}
		if d.right.Err() != nil {
			d.err = fmt.Errorf("failed in right tree: %w", d.right.Err())
			return false
		}
		comp := d.compareKeys()
		switch comp {
		case 0:
			leftVal := d.left.Value()
			rightVal := d.right.Value()
			d.rightNext = d.right.Next()
			d.leftNext = d.left.Next()
			if !bytes.Equal(leftVal.Value.Identity, rightVal.Value.Identity) {
				d.currentVal = &Diff{
					Type:  DiffTypeChanged,
					Key:   rightVal.Key,
					Value: rightVal.Value,
				}
				return true
			}
		case -1:
			d.currentVal = &Diff{
				Type:  DiffTypeRemoved,
				Key:   d.left.Value().Key,
				Value: d.left.Value().Value,
			}
			d.leftNext = d.left.Next()
			return true
		default: // leftKey > rightKey
			d.currentVal = &Diff{
				Type:  DiffTypeAdded,
				Key:   d.right.Value().Key,
				Value: d.right.Value().Value,
			}
			d.rightNext = d.right.Next()
			return true
		}
	}
	d.currentVal = nil
	return false
}

func (d *diffIterator) SeekGE(id Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.leftNext = d.left.Next()
	d.rightNext = d.right.Next()
	d.err = nil
}

func (d *diffIterator) Value() *Diff {
	return d.currentVal
}

func (d *diffIterator) Err() error {
	return d.err
}

func (d *diffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
