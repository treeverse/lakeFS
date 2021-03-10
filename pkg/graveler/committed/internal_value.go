package committed

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

// rangeToValue returns the Value representing rng in a MetaRange
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
