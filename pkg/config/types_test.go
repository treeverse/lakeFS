package config_test

import (
	"github.com/go-test/deep"
	"github.com/mitchellh/mapstructure"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/testutil"

	"errors"
	"strings"
	"testing"
)

type StringsStruct struct {
	S config.Strings
	I int
}

func TestStrings(t *testing.T) {
	cases := []struct {
		Name     string
		Source   map[string]interface{}
		Expected StringsStruct
	}{
		{
			Name: "single string",
			Source: map[string]interface{}{
				"s": "value",
			},
			Expected: StringsStruct{
				S: config.Strings{"value"},
			},
		}, {
			Name: "comma-separated string",
			Source: map[string]interface{}{
				"s": "the,quick,brown",
			},
			Expected: StringsStruct{
				S: config.Strings{"the", "quick", "brown"},
			},
		}, {
			Name: "multiple strings",
			Source: map[string]interface{}{
				"s": []string{"the", "quick", "brown"},
			},
			Expected: StringsStruct{
				S: config.Strings{"the", "quick", "brown"},
			},
		}, {
			Name: "other values",
			Source: map[string]interface{}{
				"s": []string{"yes"},
				"i": 17,
			},
			Expected: StringsStruct{
				S: config.Strings{"yes"},
				I: 17,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var s StringsStruct

			dc := mapstructure.DecoderConfig{
				DecodeHook: config.DecodeStrings,
				Result:     &s,
			}
			decoder, err := mapstructure.NewDecoder(&dc)
			testutil.MustDo(t, "new decoder", err)
			err = decoder.Decode(c.Source)
			testutil.MustDo(t, "decode", err)
			if diffs := deep.Equal(s, c.Expected); diffs != nil {
				t.Error(diffs)
			}
		})
	}
}

type OnlyStringStruct struct {
	S config.OnlyString
}

// errorsMatch returns true if errors.Is(err, target), or if the error message of err
// contains the error message of target as a substring.  It is needed until
// mapstructure wraps errors returned from string functions (see PR
// mitchellh/mapstructure#282).
func errorsMatch(err, target error) bool {
	return errors.Is(err, target) ||
		target != nil && err != nil && strings.Contains(err.Error(), target.Error())
}

func TestOnlyString(t *testing.T) {
	cases := []struct {
		Name     string
		Source   map[string]interface{}
		Expected *OnlyStringStruct
		Err      error
	}{
		{
			Name:     "String",
			Source:   map[string]interface{}{"S": "string"},
			Expected: &OnlyStringStruct{S: "string"},
		}, {
			Name:   "Number",
			Source: map[string]interface{}{"S": 17},
			Err:    config.ErrMustBeString,
		}, {
			Name:   "Struct",
			Source: map[string]interface{}{"S": OnlyStringStruct{S: "string nested in struct"}},
			Err:    config.ErrMustBeString,
		},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var o OnlyStringStruct

			dc := mapstructure.DecoderConfig{
				DecodeHook: config.DecodeOnlyString,
				Result:     &o,
			}
			decoder, err := mapstructure.NewDecoder(&dc)
			testutil.MustDo(t, "new decoder", err)
			err = decoder.Decode(c.Source)
			if c.Err != nil && !errorsMatch(err, c.Err) {
				t.Errorf("Got value %+v, error %v when expecting error %v", o, err, c.Err)
			}
			if c.Err == nil && err != nil {
				t.Errorf("Got error %v when expecting to succeed", err)
			}
			if c.Expected != nil {
				if diffs := deep.Equal(o, *c.Expected); diffs != nil {
					t.Errorf("Got unexpected value: %s", diffs)
				}
			}
		})
	}
}
