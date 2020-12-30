package committed

import "google.golang.org/protobuf/proto"

// Range represents a Range of sorted Keys
type Range struct {
	ID            ID
	MinKey        Key
	MaxKey        Key
	EstimatedSize uint64 // EstimatedSize estimated Range size in bytes
}

func MarshalRange(r Range) ([]byte, error) {
	return proto.Marshal(&RangeData{
		MinKey:        r.MinKey,
		MaxKey:        r.MaxKey,
		EstimatedSize: r.EstimatedSize,
	})
}

func UnmarshalRange(b []byte) (Range, error) {
	var p RangeData
	err := proto.Unmarshal(b, &p)
	if err != nil {
		return Range{}, err
	}
	return Range{
		MinKey:        p.MinKey,
		MaxKey:        p.MaxKey,
		EstimatedSize: p.EstimatedSize,
	}, nil
}
