package committed

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// ErrBadValueBytes is an error that is probably returned when unmarshalling bytes that are
// supposed to encode a Value.
var ErrBadValueBytes = errors.New("bad bytes format for graveler.Value")

// ErrTooLong is an error that is returned when trying to marshal too long a key or value.
// This should never normally happen in graveler files generated by graveler.
var ErrTooLong = errors.New("too long")

// MaxValueComponentBytes is the longest size allowed for the data length of a graveler value
// (or its identity, but that is controlled by code here, so less likely).  It (only) protects
// the process from unbounded serialization.  "640 KB should be enough for anyone" - even at a
// few 10s of KiBs you may be better served with some other format or implementation.
const MaxValueComponentBytes = 640 << 16

/*
 * Value is serialized in a trivial fixed-order format:
 *
 *    | len(Identity) | Identity | len(Value) | Value |
 *
 * where each length is serialized as a varint and additional bytes after Value are silently
 * ignored.
 */

func varintBytes(i int) []byte {
	e := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(e, int64(i))
	return e[:l]
}

func putBytes(buf *[]byte, b []byte) {
	*buf = append(*buf, varintBytes(len(b))...)
	*buf = append(*buf, b...)
}

// MarshalValue returns bytes that uniquely unmarshal into a Value equal to v.
func MarshalValue(v *graveler.Value) ([]byte, error) {
	if len(v.Identity) > MaxValueComponentBytes || len(v.Data) > MaxValueComponentBytes {
		return nil, ErrTooLong
	}
	ret := make([]byte, 0, len(v.Identity)+len(v.Data)+2*binary.MaxVarintLen32)
	putBytes(&ret, v.Identity)
	putBytes(&ret, v.Data)
	return ret, nil
}

// MustMarshalValue a MarshalValue that will panic on error
func MustMarshalValue(v *graveler.Value) []byte {
	val, err := MarshalValue(v)
	if err != nil {
		panic(err)
	}
	return val
}

// splitBytes splits a given byte slice into two: the first part defined by the interpreted length (provided in the
// slice), and the second part is the remainder of bytes from the slice
func splitBytes(b []byte) ([]byte, []byte, error) {
	l, o := binary.Varint(b)
	if o < 0 {
		return nil, nil, fmt.Errorf("read length: %w", ErrBadValueBytes)
	}
	remainedBuf := b[o:]
	if len(remainedBuf) < int(l) {
		return nil, nil, fmt.Errorf("not enough bytes to read %d bytes: %w", l, ErrBadValueBytes)
	}
	if l < 0 {
		return nil, nil, fmt.Errorf("impossible negative length %d: %w", l, ErrBadValueBytes)
	}
	value := remainedBuf[:l]
	rest := remainedBuf[l:]
	return value, rest, nil
}

func UnmarshalValue(b []byte) (*graveler.Value, error) {
	ret := &graveler.Value{}
	var err error
	data := b
	if ret.Identity, data, err = splitBytes(data); err != nil {
		return nil, fmt.Errorf("identity field: %w", err)
	}
	if ret.Data, _, err = splitBytes(data); err != nil {
		return nil, fmt.Errorf("data field: %w", err)
	}
	return ret, nil
}
