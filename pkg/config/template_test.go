package config_test

import (
	"github.com/go-test/deep"
	"github.com/mitchellh/mapstructure"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/testutil"

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
