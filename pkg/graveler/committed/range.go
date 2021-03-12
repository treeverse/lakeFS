package committed

import "google.golang.org/protobuf/proto"

// Range represents a range of sorted Keys

type Range struct {
	ID            ID
	MinKey        Key
	MaxKey        Key
	EstimatedSize uint64 // EstimatedSize estimated Range size in bytes
	Count         int64
}

func (r Range) Copy() *Range {
	return &Range{
		ID:            r.ID,
		MinKey:        r.MinKey.Copy(),
		MaxKey:        r.MaxKey.Copy(),
		EstimatedSize: r.EstimatedSize,
		Count:         r.Count,
	}
}

func (r Range) IsTombstone() bool {
	return r.EstimatedSize == 0
}

func (r *Range) SetTombstone() {
	r.EstimatedSize = 0
}

func MarshalRange(r Range) ([]byte, error) {
	return proto.Marshal(&RangeData{
		MinKey:        r.MinKey,
		MaxKey:        r.MaxKey,
		EstimatedSize: r.EstimatedSize,
		Count:         r.Count,
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
		Count:         p.Count,
	}, nil
}
