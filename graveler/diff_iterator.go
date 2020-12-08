package graveler

import "bytes"

type ValueDiffIterator struct {
	left        ValueIterator
	right       ValueIterator
	leftNext    bool
	rightNext   bool
	currentVal  *ValueRecord
	currentType DiffType
	err         error
}

func (d *ValueDiffIterator) Next() bool {
	if d.rightNext {
		d.rightNext = d.right.Next()
	}
	if d.leftNext {
		d.leftNext = d.left.Next()
	}
	for {
		if d.left.Err() != nil || d.right.Err() != nil {
			return false
		}
		if d.rightNext && d.leftNext {
			leftKey := d.left.Value().Key
			rightKey := d.right.Value().Key
			if bytes.Equal(leftKey, rightKey) {
				if !bytes.Equal(d.left.Value().Value.Identity, d.right.Value().Value.Identity) {
					d.currentVal = d.left.Value()
					d.currentType = DiffTypeChanged
					return true
				}
				d.rightNext = d.right.Next()
				d.leftNext = d.left.Next()
				continue // identical values
			} else if bytes.Compare(leftKey, rightKey) < 0 {
				d.currentVal = d.left.Value()
				d.currentType = DiffTypeRemoved
				return true
			} else { // leftKey > rightKey
				d.currentVal = d.right.Value()
				d.currentType = DiffTypeAdded
				return true
			}
		} else if d.leftNext {
			d.currentVal = d.left.Value()
			d.currentType = DiffTypeRemoved
			return true
		} else if d.rightNext {
			d.currentVal = d.right.Value()
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
