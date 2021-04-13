package config_test

import (
	"reflect"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	tagName        = "test"
	squashTagValue = "squash"
)

func TestStructKeys_Simple(t *testing.T) {
	type s struct {
		A int
		B string
		C *float64
		D []rune
	}

	keys := config.GetStructKeys(reflect.TypeOf(s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"A", "B", "C", "D"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}

	keys = config.GetStructKeys(reflect.TypeOf(&s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"A", "B", "C", "D"}); diffs != nil {
		t.Error("wrong keys for pointer to struct: ", diffs)
	}

	ps := &s{}
	keys = config.GetStructKeys(reflect.TypeOf(&ps), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"A", "B", "C", "D"}); diffs != nil {
		t.Error("wrong keys for pointer to pointer to struct: ", diffs)
	}
}

func TestStructKeys_Nested(t *testing.T) {
	type s struct {
		A struct {
			X string
			Y int
		}
		B ***struct {
			Z float32
			W float64
		}
	}

	keys := config.GetStructKeys(reflect.TypeOf(s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"A.X", "A.Y", "B.Z", "B.W"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestStructKeys_SimpleTagged(t *testing.T) {
	type s struct {
		A int `test:"Aaa"`
		B int `toast:"bee"`
		c int `test:"ccc" toast:"sea"`
	}

	keys := config.GetStructKeys(reflect.TypeOf(s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"Aaa", "B", "ccc"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestStructKeys_NestedTagged(t *testing.T) {
	type s struct {
		A struct {
			X  int `test:"eks"`
			BE string
		} `test:"aaa"`
		B **struct {
			Gamma int32
			Delta uint8 `test:"dee"`
		}
	}

	keys := config.GetStructKeys(reflect.TypeOf(s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"aaa.eks", "aaa.BE", "B.Gamma", "B.dee"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestStructKeys_Squash(t *testing.T) {
	type I struct {
		A int
		B int
	}
	type J struct {
		C uint
		D uint
	}
	type s struct {
		I `test:",squash"`
		J `test:"ignore,squash"`
	}

	keys := config.GetStructKeys(reflect.TypeOf(s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"A", "B", "C", "D"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}
