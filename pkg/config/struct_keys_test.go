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
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}

	keys = config.GetStructKeys(reflect.TypeOf(&s{}), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for pointer to struct: ", diffs)
	}

	ps := &s{}
	keys = config.GetStructKeys(reflect.TypeOf(&ps), tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
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
	if diffs := deep.Equal(keys, []string{"a.x", "a.y", "b.z", "b.w"}); diffs != nil {
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
	if diffs := deep.Equal(keys, []string{"Aaa", "b", "ccc"}); diffs != nil {
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
	if diffs := deep.Equal(keys, []string{"aaa.eks", "aaa.be", "b.gamma", "b.dee"}); diffs != nil {
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
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestValidateMissingRequired_SimpleRequired(t *testing.T) {
	type s struct {
		A  int `validate:"required"`
		AA int
		B  string   `validate:"x,required,y"`
		BB string   `validate:"unrequired"`
		C  *float64 `validate:"0,required"`
		CC *float64 `validate:"unrequired,1,2,3"`
		D  []rune   `validate:"required,2"`
		DD []rune
	}

	keys := config.ValidateMissingRequiredKeys(s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong missing keys for struct: ", diffs)
	}
	keys = config.ValidateMissingRequiredKeys(&s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong missing keys for pointer to struct: ", diffs)
	}
	ps := &s{}
	keys = config.ValidateMissingRequiredKeys(&ps, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong missing keys for pointer to pointer to struct: ", diffs)
	}
}

func TestValidateMissingRequired_SimpleNotMissing(t *testing.T) {
	type s struct {
		A  int `validate:"required"`
		AA int
		B  string   `validate:"x,required,y"`
		BB string   `validate:"unrequired"`
		C  *float64 `validate:"0,required"`
		CC *float64 `validate:"unrequired,1,2,3"`
		D  []rune   `validate:"required,2"`
		DD []rune
	}

	c := 12.34
	cases := []struct {
		Value           s
		MissingRequired []string
	}{
		{s{A: 2, C: &c}, []string{"b", "d"}},
		{s{AA: 22, B: "foo", D: []rune{'a', 'b', 'c'}}, []string{"a", "c"}},
	}

	for _, c := range cases {
		keys := config.ValidateMissingRequiredKeys(c.Value, tagName, squashTagValue)
		if diffs := deep.Equal(keys, c.MissingRequired); diffs != nil {
			t.Errorf("wrong missing keys for %+v: %s", c.Value, diffs)
		}
	}
}

func TestValidateMissingRequired_Nested(t *testing.T) {
	type B struct {
		Z float32 `validate:"required"`
		W float64
	}
	type s struct {
		A struct {
			X string
			Y int `validate:"required"`
		}
		B ***B `validate:"required"`
	}

	keys := config.ValidateMissingRequiredKeys(s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a.y", "b"}); diffs != nil {
		t.Error("wrong missing keys for empty struct: ", diffs)
	}

	b := B{}
	pb := &b
	ppb := &pb
	keys = config.ValidateMissingRequiredKeys(&s{B: &ppb}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a.y", "b.z"}); diffs != nil {
		t.Error("wrong missing keys for struct with empty optional: ", diffs)
	}
}

func TestValidateMissingRequired_NestedNotMissing(t *testing.T) {
	type A struct {
		X string
		Y int `validate:"required"`
	}
	type B struct {
		Z float32 `validate:"required"`
		W float64
	}
	type s struct {
		A A
		B ***B `validate:"required"`
	}

	ptr3 := func(b B) ***B { pb := &b; ppb := &pb; return &ppb }
	ptr2 := func(b *B) ***B { pb := &b; return &pb }
	ptr1 := func(b **B) ***B { return &b }

	cases := []struct {
		Name            string
		Value           s
		MissingRequired []string
	}{
		{"s{}", s{}, []string{"a.y", "b"}},
		{"s{A: A{Y: 7}, B: &&&B{}}", s{A: A{Y: 7}, B: ptr3(B{})}, []string{"b.z"}},
		{"s{B: &&&B{Z: 1.23}}", s{B: ptr3(B{Z: 1.23})}, []string{"a.y"}},
		// Required field B is present but leads nowhere: all good(!).
		{"s{A: A{Y: 7}, B: &nil", s{A: A{Y: 7}, B: ptr1(nil)}, nil},
		// Required field B is present but leads nowhere: all good(!).
		{"s{A: A{Y: 7}, B: &&nil}", s{A: A{Y: 7}, B: ptr2(nil)}, nil},
	}

	for _, c := range cases {
		keys := config.ValidateMissingRequiredKeys(c.Value, tagName, squashTagValue)
		if diffs := deep.Equal(keys, c.MissingRequired); diffs != nil {
			t.Errorf("%s: wrong keys for %+v: %s", c.Name, c.Value, diffs)
		}
	}
}

func TestValidateMissingRequired_SimpleTagged(t *testing.T) {
	type s struct {
		A int `test:"Aaa" validate:"required"`
		B int `toast:"bee" validate:"required"`
		c int `test:"ccc" toast:"sea" validate:"required"`
	}

	keys := config.ValidateMissingRequiredKeys(s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"Aaa", "b", "ccc"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestValidateMissingRequired_NestedTagged(t *testing.T) {
	type B struct {
		Gamma int32 `validate:"required"`
		Delta uint8 `test:"dee" validate:"required"`
	}
	type s struct {
		A struct {
			X  int    `test:"eks" validate:"required"`
			BE string `validate:"required"`
		} `test:"aaa"`
		B *B
	}

	keys := config.ValidateMissingRequiredKeys(s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"aaa.eks", "aaa.be"}); diffs != nil {
		t.Error("wrong missing keys for missing optional: ", diffs)
	}

	b := B{}
	keys = config.ValidateMissingRequiredKeys(s{B: &b}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"aaa.eks", "aaa.be", "b.gamma", "b.dee"}); diffs != nil {
		t.Error("wrong missing keys for present optional missing fields: ", diffs)
	}
}

func TestValidateMissingRequired_Squash(t *testing.T) {
	type I struct {
		A int `validate:"required"`
		B int `validate:"required"`
	}
	type J struct {
		C uint `validate:"required"`
		D uint `validate:"required"`
	}
	type s struct {
		I `test:",squash"`
		J `test:"ignore,squash"`
	}

	keys := config.ValidateMissingRequiredKeys(s{}, tagName, squashTagValue)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong missing keys for struct: ", diffs)
	}
}
