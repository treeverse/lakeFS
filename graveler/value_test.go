package graveler_test

import (
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
)

func TestGravelerValueMarshal(t *testing.T) {
	cases := []struct {
		name string
		v    graveler.Value
		b    []byte
	}{
		{name: "empty", v: graveler.Value{}, b: []byte{0, 0}},
		{name: "identity", v: graveler.Value{Identity: []byte("foo")}, b: []byte{6, 102, 111, 111, 0}},
		{name: "data", v: graveler.Value{Data: []byte("the quick brown fox")}, b: []byte{0, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "identityAndData", v: graveler.Value{Identity: []byte("foo"), Data: []byte("the quick brown fox")}, b: []byte{6, 102, 111, 111, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := graveler.MarshalValue(&c.v)
			if err != nil {
				t.Error(err)
			}
			if diffs := deep.Equal(b, c.b); diffs != nil {
				t.Error("bad bytes: ", diffs)
			}
		})
	}
}

func TestGravelerValueUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		v    *graveler.Value
		b    []byte
		err  error
	}{
		{name: "empty", v: &graveler.Value{}, b: []byte{0, 0}},
		{name: "identity", v: &graveler.Value{Identity: []byte("foo")}, b: []byte{6, 102, 111, 111, 0}},
		{name: "data", v: &graveler.Value{Data: []byte("the quick brown fox")}, b: []byte{0, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "identityAndData", v: &graveler.Value{Identity: []byte("foo"), Data: []byte("the quick brown fox")}, b: []byte{6, 102, 111, 111, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "failIdentityNegativeLength", b: []byte{3, 102, 111, 111, 0}, err: graveler.ErrBadValueBytes},
		{name: "failIdentityTooShort", b: []byte{4, 102, 111, 111, 0}, err: graveler.ErrBadValueBytes},
		{name: "failIdentityTooLong", b: []byte{16, 102, 111, 111, 0}, err: graveler.ErrBadValueBytes},
		{name: "failIdentityDataNegativeLength", b: []byte{6, 102, 111, 111, 17, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}, err: graveler.ErrBadValueBytes},
		{name: "identityAndDataAndLeftovers", v: &graveler.Value{Identity: []byte("foo"), Data: []byte("the quick brown fox")}, b: []byte{6, 102, 111, 111, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{name: "failIdentityDataTooLong", b: []byte{6, 102, 111, 111, 250, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}, err: graveler.ErrBadValueBytes},
	}

	for _, c := range cases {
		if c.v != nil && c.v.Identity == nil {
			c.v.Identity = make([]byte, 0)
		}
		if c.v != nil && c.v.Data == nil {
			c.v.Data = make([]byte, 0)
		}
		t.Run(c.name, func(t *testing.T) {
			v, err := graveler.UnmarshalValue(c.b)
			if !errors.Is(err, c.err) {
				t.Errorf("got error %s != %s", err, c.err)
			}
			if diffs := deep.Equal(v, c.v); diffs != nil {
				t.Errorf("bad value %+v: %s", v, diffs)
			}
		})
	}
}

func TestGravelerValueIdentityUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		id   []byte
		b    []byte
		err  error
	}{
		{name: "empty", b: []byte{0, 0}},
		{name: "identity", id: []byte("foo"), b: []byte{6, 102, 111, 111, 0}},
		{name: "data", b: []byte{0, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "identityAndData", id: []byte("foo"), b: []byte{6, 102, 111, 111, 38, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "failIdentityNegativeLength", b: []byte{3, 102, 111, 111, 0}, err: graveler.ErrBadValueBytes},
		{name: "okIdentityTooShort", id: []byte("fo"), b: []byte{4, 102, 111, 111, 0}},
		{name: "failIdentityTooLong", b: []byte{16, 102, 111, 111, 0}, err: graveler.ErrBadValueBytes},
		{name: "okIdentityDataNegativeLength", id: []byte("foo"), b: []byte{6, 102, 111, 111, 17, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
		{name: "okIdentityDataTooLong", id: []byte("foo"), b: []byte{6, 102, 111, 111, 250, 116, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120}},
	}

	for _, c := range cases {
		if c.id == nil {
			c.id = make([]byte, 0)
		}
		t.Run(c.name, func(t *testing.T) {
			id, err := graveler.UnmarshalIdentity(c.b)
			if !errors.Is(err, c.err) {
				t.Errorf("got error %s != %s", err, c.err)
			}
			if err == nil {
				if diffs := deep.Equal(id, c.id); diffs != nil {
					t.Errorf("bad value %+v: %s", id, diffs)
				}
			}
		})
	}
}
