package config_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-openapi/swag"

	"github.com/go-test/deep"
	"github.com/mitchellh/mapstructure"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/testutil"
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

func TestDecodeStringToMap(t *testing.T) {
	cases := []struct {
		Name        string
		Source      string
		Expected    map[string]string
		ExpectedErr error
	}{
		{
			Name:     "empty string",
			Source:   "",
			Expected: map[string]string{},
		}, {
			Name:     "single pair",
			Source:   "key=value",
			Expected: map[string]string{"key": "value"},
		}, {
			Name:     "multiple pairs",
			Source:   "key1=value1,key2=value2",
			Expected: map[string]string{"key1": "value1", "key2": "value2"},
		}, {
			Name:     "pair with spaces",
			Source:   "key = value , key2 = value2",
			Expected: map[string]string{"key": "value", "key2": "value2"},
		}, {
			Name:        "invalid pair",
			Source:      "key1=value1,key2",
			ExpectedErr: config.ErrInvalidKeyValuePair,
		},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			result := make(map[string]string)
			dc := mapstructure.DecoderConfig{
				DecodeHook: config.DecodeStringToMap(),
				Result:     &result,
			}
			decoder, err := mapstructure.NewDecoder(&dc)
			testutil.MustDo(t, "new decoder", err)
			err = decoder.Decode(c.Source)
			if c.ExpectedErr == nil {
				testutil.MustDo(t, "decode", err)
				if diffs := deep.Equal(result, c.Expected); diffs != nil {
					t.Error(diffs)
				}
			} else {
				if !errorsMatch(err, c.ExpectedErr) {
					t.Errorf("Got error \"%v\", expected error \"%v\"", err, c.ExpectedErr)
				}
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

func TestStringToSliceWithBracketHookFunc(t *testing.T) {
	cases := []struct {
		Name       string
		Source     string
		Expected   []string
		ErrMessage *string
	}{
		{
			Name:     "Empty string",
			Source:   "",
			Expected: []string{},
		},
		{
			Name:     "Valid array",
			Source:   `["one", "two", "three"]`,
			Expected: []string{"\"one\"", "\"two\"", "\"three\""},
		},
		{
			Name:       "Invalid array (json)",
			Source:     `{"key": "value"}`,
			ErrMessage: swag.String("source data must be an array or slice"),
		},
		{
			Name:       "Invalid array (string)",
			Source:     "not a json array",
			ErrMessage: swag.String("source data must be an array or slice"),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var s []string

			dc := mapstructure.DecoderConfig{
				DecodeHook: config.StringToSliceWithBracketHookFunc(),
				Result:     &s,
			}
			decoder, err := mapstructure.NewDecoder(&dc)
			testutil.MustDo(t, "new decoder", err)
			err = decoder.Decode(c.Source)
			if c.ErrMessage != nil && err == nil {
				t.Errorf("Got value %+v, error %v when expecting error %v", s, err, c.ErrMessage)
			} else if err != nil && !strings.Contains(err.Error(), *c.ErrMessage) {
				t.Errorf("Got error %v when expecting to succeed", err)
			} else if diffs := deep.Equal(s, c.Expected); diffs != nil {
				t.Errorf("Got unexpected value: %s", diffs)
			}
		})
	}
}

func TestStringToStructHookFunc(t *testing.T) {
	type TestStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	cases := []struct {
		Name     string
		Source   string
		Result   interface{}
		Expected interface{}
	}{
		{
			Name:     "Empty string",
			Source:   "",
			Result:   &TestStruct{},
			Expected: &TestStruct{},
		},
		{
			Name:   "Valid JSON object",
			Source: `{"name": "test", "value": 42}`,
			Result: &TestStruct{},
			Expected: &TestStruct{
				Name:  "test",
				Value: 42,
			},
		},
		{
			Name:     "Invalid JSON (array)",
			Source:   `["one", "two", "three"]`,
			Expected: `["one", "two", "three"]`,
		},
		{
			Name:     "Invalid JSON (string)",
			Source:   "just a string",
			Expected: "just a string",
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			dc := mapstructure.DecoderConfig{
				DecodeHook: config.StringToStructHookFunc(),
				Result:     &c.Result,
			}
			decoder, err := mapstructure.NewDecoder(&dc)
			testutil.MustDo(t, "new decoder", err)
			err = decoder.Decode(c.Source)
			if err != nil {
				t.Errorf("Got error %v when expecting to succeed", err)
			} else if diffs := deep.Equal(c.Result, c.Expected); diffs != nil {
				t.Errorf("Got unexpected value: %s", diffs)
			}
		})
	}
}
