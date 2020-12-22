package tree

import (
	"bytes"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type diffIterator struct {
	left       Iterator
	right      Iterator
	leftNext   bool
	rightNext  bool
	currentVal *graveler.Diff
	err        error
}

const (
	LeftLower = iota
	RightLower
	SameKey
	SamePart
)

func NewDiffIterator(left Iterator, right Iterator) graveler.DiffIterator {
	it := &diffIterator{left: left, right: right}
	it.leftNext = it.left.Next()
	it.rightNext = it.right.Next()
	return it
}

func getKey(it Iterator) graveler.Key {
	val, part := it.Value()
	if val == nil {
		return part.MinKey
	}
	return val.Key
}

func (d *diffIterator) compareKeys() int {
	return bytes.Compare(getKey(d.left), getKey(d.right))
}

func (d *diffIterator) peek() int {
	if d.rightNext && !d.leftNext {
		return RightLower
	}
	if !d.rightNext && d.leftNext {
		return LeftLower
	}
	comp := d.compareKeys()
	leftVal, leftPart := d.left.Value()
	rightVal, rightPart := d.right.Value()
	switch comp {
	case 0:
		if leftPart.Name == rightPart.Name {
			return SamePart
		}
		if leftVal == nil {
			return LeftLower
		}
		if rightVal == nil {
			return RightLower
		}
		return SameKey
	case -1:
		return LeftLower
	default:
		return RightLower
	}
}
func getAndStep(it Iterator) (*graveler.ValueRecord, bool) {
	val, _ := it.Value()

	return val, it.Next()
}

func (d *diffIterator) Next() bool {
	if d.err != nil {
		return false
	}
	for d.leftNext || d.rightNext {
		if d.left.Err() != nil {
			d.err = fmt.Errorf("failed in left tree: %w", d.left.Err())
			return false
		}
		if d.right.Err() != nil {
			d.err = fmt.Errorf("failed in right tree: %w", d.right.Err())
			return false
		}
		peek := d.peek()
		switch peek {
		case SamePart:
			d.leftNext = d.left.NextPart()
			d.rightNext = d.right.NextPart()
			continue
		case SameKey:
			leftVal, _ := d.left.Value()
			rightVal, _ := d.right.Value()
			//if leftVal == nil {
			//	d.leftNext = d.left.Next()
			//	continue
			//}
			//if rightVal == nil {
			//	d.rightNext = d.right.Next()
			//	continue
			//}
			d.leftNext = d.left.Next()
			d.rightNext = d.right.Next()
			if bytes.Equal(leftVal.Identity, rightVal.Identity) {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeChanged,
				Key:   rightVal.Key,
				Value: rightVal.Value,
			}
			return true
		case LeftLower:
			leftVal, _ := d.left.Value()
			d.leftNext = d.left.Next()
			if leftVal == nil {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeRemoved,
				Key:   leftVal.Key,
				Value: leftVal.Value,
			}
			return true
		case RightLower:
			rightVal, _ := d.right.Value()
			d.rightNext = d.right.Next()
			if rightVal == nil {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeAdded,
				Key:   rightVal.Key,
				Value: rightVal.Value,
			}
			return true
		}

	}
	d.currentVal = nil
	return false
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.leftNext = d.left.Next()
	d.rightNext = d.right.Next()
	d.err = nil
}

func (d *diffIterator) Value() *graveler.Diff {
	return d.currentVal
}

func (d *diffIterator) Err() error {
	return d.err
}

func (d *diffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
