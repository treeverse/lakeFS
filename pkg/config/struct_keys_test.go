package config_test

import (
	"reflect"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	tagName         = "test"
	squashTagValue  = "squash"
	requiredTagName = "required"
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

func TestGetMissingRequired_SimpleRequired(t *testing.T) {
	type s struct {
		A  int `required:"yes"`
		AA int
		B  string `required:"no (is actually yes)"`
		BB string
		C  *float64 `required:"yup"`
		CC *float64
		D  []rune `required:""`
		DD []rune
	}

	keys := config.GetMissingRequiredKeys(s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
	keys = config.GetMissingRequiredKeys(&s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for pointer to struct: ", diffs)
	}
	ps := &s{}
	keys = config.GetMissingRequiredKeys(&ps, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for pointer to pointer to struct: ", diffs)
	}
}

func TestGetMissingRequired_SimpleNotMissing(t *testing.T) {
	type s struct {
		A  int `required:"yes"`
		AA int
		B  string `required:"no (is actually yes)"`
		BB string
		C  *float64 `required:"yup"`
		CC *float64
		D  []rune `required:""`
		DD []rune
	}

	c := float64(12.34)
	cases := []struct {
		Value           s
		MissingRequired []string
	}{
		{s{A: 2, C: &c}, []string{"b", "d"}},
		{s{AA: 22, B: "foo", D: []rune{'a', 'b', 'c'}}, []string{"a", "c"}},
	}

	for _, c := range cases {
		keys := config.GetMissingRequiredKeys(c.Value, tagName, squashTagValue, requiredTagName)
		if diffs := deep.Equal(keys, c.MissingRequired); diffs != nil {
			t.Errorf("wrong keys for %+v: %s", c.Value, diffs)
		}
	}
}

func TestGetMissingRequired_Nested(t *testing.T) {
	type s struct {
		A struct {
			X string
			Y int `required:"yes"`
		}
		B ***struct {
			Z float32 `required:"yes"`
			W float64
		}
	}

	keys := config.GetMissingRequiredKeys(s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"a.y", "b.z"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestGetMissingRequired_NestedNotMissing(t *testing.T) {
	type A struct {
		X string
		Y int `required:"yes"`
	}
	type B struct {
		Z float32 `required:"yes"`
		W float64
	}
	type s struct {
		A A
		B ***B
	}

	ptr3 := func(b B) ***B { pb := &b; ppb := &pb; return &ppb }
	ptr2 := func(b *B) ***B { pb := &b; return &pb }
	ptr1 := func(b **B) ***B { return &b }

	cases := []struct {
		Value           s
		MissingRequired []string
	}{
		{s{A: A{Y: 7}, B: ptr3(B{})}, []string{"b.z"}},
		{s{B: ptr3(B{Z: 1.23})}, []string{"a.y"}},
		{s{A: A{Y: 7}, B: ptr1(nil)}, []string{"b.z"}},
		{s{A: A{Y: 7}, B: ptr2(nil)}, []string{"b.z"}},
	}

	for _, c := range cases {
		keys := config.GetMissingRequiredKeys(c.Value, tagName, squashTagValue, requiredTagName)
		if diffs := deep.Equal(keys, c.MissingRequired); diffs != nil {
			t.Errorf("wrong keys for %+v: %s", c.Value, diffs)
		}
	}
}

func TestGetMissingRequired_SimpleTagged(t *testing.T) {
	type s struct {
		A int `test:"Aaa" required:"yes"`
		B int `toast:"bee" required:"yes"`
		c int `test:"ccc" toast:"sea" required:"yes"`
	}

	keys := config.GetMissingRequiredKeys(s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"aaa", "b", "ccc"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestGetMissingRequired_NestedTagged(t *testing.T) {
	type s struct {
		A struct {
			X  int    `test:"eks" required:"yes"`
			BE string `required:"yes"`
		} `test:"aaa"`
		B **struct {
			Gamma int32 `required:"yes"`
			Delta uint8 `test:"dee" required:"yes"`
		}
	}

	keys := config.GetMissingRequiredKeys(s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"aaa.eks", "aaa.be", "b.gamma", "b.dee"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}

func TestGetMissingRequired_Squash(t *testing.T) {
	type I struct {
		A int `required:"yes"`
		B int `required:"yes"`
	}
	type J struct {
		C uint `required:"yes"`
		D uint `required:"yes"`
	}
	type s struct {
		I `test:",squash"`
		J `test:"ignore,squash"`
	}

	keys := config.GetMissingRequiredKeys(s{}, tagName, squashTagValue, requiredTagName)
	if diffs := deep.Equal(keys, []string{"a", "b", "c", "d"}); diffs != nil {
		t.Error("wrong keys for struct: ", diffs)
	}
}
