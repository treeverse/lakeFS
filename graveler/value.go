package graveler

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

/*
 * Value is serialized in a trivial fixed-order format:
 *
 *    | len(Identity) | Identity | len(Value) | Value |
 *
 * where each length is serialized as a varint and additional bytes after Value are silently
 * ignored.
 */

func writeVarint(w io.Writer, i int) error {
	e := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(e, int64(i))
	_, err := w.Write(e[:l])
	return err
}

func writeBytes(tag string, w io.Writer, b []byte) error {
	if err := writeVarint(w, len(b)); err != nil {
		return fmt.Errorf("write %s length: %w", tag, err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("write %s: %w", tag, err)
	}
	return nil
}

// MarshalValue returns bytes that uniquely unmarshal into a Value equal to v.
func MarshalValue(v *Value) ([]byte, error) {
	var w = &bytes.Buffer{}

	if err := writeBytes("identity", w, v.Identity); err != nil {
		return nil, err
	}
	if err := writeBytes("data", w, v.Data); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// ErrBadValueBytes is an error that is probably returned when unmarshalling bytes that are
// supposed to encode a Value.
var ErrBadValueBytes = errors.New("bad bytes format for graveler.Value")

func getBytes(tag string, b *[]byte) ([]byte, error) {
	l, o := binary.Varint(*b)
	if o <= 0 {
		return nil, fmt.Errorf("read %s length: %w", tag, ErrBadValueBytes)
	}
	*b = (*b)[o:]
	if len(*b) < int(l) {
		return nil, fmt.Errorf("not enough bytes to read %d bytes for %s: %w", l, tag, ErrBadValueBytes)
	}
	if l < 0 {
		return nil, fmt.Errorf("impossible negative length %d for %s: %w", l, tag, ErrBadValueBytes)
	}
	ret := make([]byte, l)
	copy(ret, (*b)[:l])
	*b = (*b)[l:]
	return ret, nil
}

func UnmarshalValue(b []byte) (*Value, error) {
	ret := &Value{}
	var err error
	if ret.Identity, err = getBytes("identifier", &b); err != nil {
		return nil, err
	}
	if ret.Data, err = getBytes("data", &b); err != nil {
		return nil, err
	}
	return ret, nil
}

// UnmarshalIdentity returns *only* the Identity field encoded by b.  It does not even examine
// any bytes beyond the prefix of b holding Identity.
func UnmarshalIdentity(b []byte) ([]byte, error) {
	return getBytes("identifier", &b)
}
