package committed

import (
	"github.com/treeverse/lakefs/graveler"
)

// rangeToValue returns a Value representing a Range in MetaRange
func rangeToValue(rng Range) (Value, error) {
	data, err := MarshalRange(rng)
	if err != nil {
		return nil, err
	}
	rangeValue := &graveler.Value{
		Identity: []byte(rng.ID),
		Data:     data,
	}
	return MarshalValue(rangeValue)
}

// valueToRange returns the Range that value encodes in its Data as a MetaRange
func valueToRange(value Value) (*Range, error) {
	v, err := UnmarshalValue([]byte(value))
	if err != nil {
		return nil, err
	}
	rng, err := UnmarshalRange(v.Data)
	if err != nil {
		return nil, err
	}
	return &rng, nil
}
