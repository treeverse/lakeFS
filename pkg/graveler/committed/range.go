package committed

import (
	"bytes"

	"google.golang.org/protobuf/proto"
)

// Range represents a range of sorted Keys
type Range struct {
	ID            ID
	MinKey        Key
	MaxKey        Key
	EstimatedSize uint64 // EstimatedSize estimated Range size in bytes
	Count         int64
	Tombstone     bool
}

func (r Range) Copy() *Range {
	return &Range{
		ID:            r.ID,
		MinKey:        r.MinKey.Copy(),
		MaxKey:        r.MaxKey.Copy(),
		EstimatedSize: r.EstimatedSize,
		Count:         r.Count,
		Tombstone:     r.Tombstone,
	}
}

func (r Range) IsBefore(o *Range) bool {
	return bytes.Compare(r.MaxKey, o.MinKey) < 0
}

func (r Range) SameBounds(o *Range) bool {
	return bytes.Equal(r.MinKey, o.MinKey) && bytes.Equal(r.MaxKey, o.MaxKey)
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
