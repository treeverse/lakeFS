package graveler

import "bytes"

type SimpleDiffIterator struct {
	left        ValueIterator
	right       ValueIterator
	leftNext    bool
	rightNext   bool
	currentVal  *ValueRecord
	currentType DiffType
}

func (d *SimpleDiffIterator) Next() bool {
	if d.rightNext {
		d.rightNext = d.right.Next()
	}
	if d.leftNext {
		d.leftNext = d.left.Next()
	}
	return d.computeDiffValues()
}

func (d *SimpleDiffIterator) computeDiffValues() bool {
	for {
		if d.rightNext && d.leftNext {
			leftKey := d.left.Value().Key
			rightKey := d.right.Value().Key
			if bytes.Equal(leftKey, rightKey) {
				d.rightNext = d.right.Next()
				d.leftNext = d.left.Next()
				if !bytes.Equal(d.left.Value().Value.Identity, d.right.Value().Value.Identity) {
					d.currentVal = d.left.Value()
					d.currentType = DiffTypeChanged
					return true
				}
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
		} else if d.leftNext {
			d.currentVal = d.right.Value()
			d.currentType = DiffTypeAdded
			return true
		}
		return false
	}
}

func (d *SimpleDiffIterator) SeekGE(id Key) bool {
	d.leftNext = d.left.SeekGE(id)
	d.rightNext = d.right.SeekGE(id)
	d.currentVal = nil
	d.currentType = 0
	return d.computeDiffValues()
}

func (d *SimpleDiffIterator) Value() *Diff {
	return &Diff{
		Type:  d.currentType,
		Key:   d.currentVal.Key,
		Value: d.currentVal.Value,
	}
}

func (d *SimpleDiffIterator) Err() error {
	panic("implement me")
}

func (d *SimpleDiffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
