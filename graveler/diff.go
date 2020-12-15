package graveler

import "bytes"

type diffIterator struct {
	left        ValueIterator
	right       ValueIterator
	leftNext    bool
	rightNext   bool
	currentVal  *ValueRecord
	oldIdentity []byte
	currentType DiffType
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
		if d.left.Err() != nil || d.right.Err() != nil {
			return false
		}
		comp := d.compareKeys()
		switch comp {
		case 0:
			leftVal := d.left.Value()
			d.currentVal = d.right.Value()
			d.rightNext = d.right.Next()
			d.leftNext = d.left.Next()
			if !bytes.Equal(leftVal.Value.Identity, d.currentVal.Value.Identity) {
				d.oldIdentity = leftVal.Identity
				d.currentType = DiffTypeChanged
				return true
			}
		case -1:
			d.currentVal = d.left.Value()
			d.oldIdentity = d.currentVal.Identity
			d.leftNext = d.left.Next()
			d.currentType = DiffTypeRemoved
			return true
		default: // leftKey > rightKey
			d.currentVal = d.right.Value()
			d.oldIdentity = nil
			d.rightNext = d.right.Next()
			d.currentType = DiffTypeAdded
			return true
		}
	}
	return false
}

func (d *diffIterator) SeekGE(id Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.currentType = 0
}

func (d *diffIterator) Value() *Diff {
	return &Diff{
		Type:        d.currentType,
		Key:         d.currentVal.Key,
		Value:       d.currentVal.Value,
		OldIdentity: d.oldIdentity,
	}
}

func (d *diffIterator) Err() error {
	if d.left.Err() != nil {
		return d.left.Err()
	}
	return d.right.Err()
}

func (d *diffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
