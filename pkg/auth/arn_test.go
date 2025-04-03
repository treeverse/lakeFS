package auth_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
)

func TestParseARN(t *testing.T) {
	cases := []struct {
		Input string
		Arn   auth.Arn
		Error bool
	}{
		{Input: "", Error: true},
		{Input: "arn:lakefs:repo", Error: true},
		{Input: "arn:lakefs:repos:a:b:myrepo", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "repos",
			Region:     "a",
			AccountID:  "b",
			ResourceID: "myrepo"}},
		{Input: "arn:lakefs:repos:a::myrepo", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "repos",
			Region:     "a",
			AccountID:  "",
			ResourceID: "myrepo"}},
		{Input: "arn:lakefs:repos::b:myrepo", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "repos",
			Region:     "",
			AccountID:  "b",
			ResourceID: "myrepo"}},
		{Input: "arn:lakefs:repos:::myrepo", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "repos",
			Region:     "",
			AccountID:  "",
			ResourceID: "myrepo"}},
		{Input: "arn:lakefs:fs:::myrepo/branch/file:with:colon", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "fs",
			Region:     "",
			AccountID:  "",
			ResourceID: "myrepo/branch/file:with:colon"}},
	}

	for _, c := range cases {
		got, err := auth.ParseARN(c.Input)
		if err != nil && !c.Error {
			t.Fatalf("got unexpected error parsing arn: \"%s\": \"%s\"", c.Input, err)
		} else if err != nil {
			continue
		} else if c.Error {
			t.Fatalf("expected error parsing arn: \"%s\"", c.Input)
		}
		if got.AccountID != c.Arn.AccountID {
			t.Fatalf("got unexpected account ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.AccountID, c.Arn.AccountID)
		}
		if got.Region != c.Arn.Region {
			t.Fatalf("got unexpected region parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Region, c.Arn.Region)
		}
		if got.Partition != c.Arn.Partition {
			t.Fatalf("got unexpected partition parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Partition, c.Arn.Partition)
		}
		if got.Service != c.Arn.Service {
			t.Fatalf("got unexpected service parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Service, c.Arn.Service)
		}
		if got.ResourceID != c.Arn.ResourceID {
			t.Fatalf("got unexpected resource ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.ResourceID, c.Arn.ResourceID)
		}
	}
}

func TestParseResources(t *testing.T) {
	cases := []struct {
		inputResource   string
		outputResources []string
		expectedError   error
	}{
		{
			inputResource:   "[\"arn:lakefs:repos::b:myrepo\",\"arn:lakefs:repos::b:hisrepo\"]",
			outputResources: []string{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:hisrepo"},
			expectedError:   nil,
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myrepo\",\"arn:lakefs:repos::b:hisrepo\",\"arn:lakefs:repos::b:ourrepo\"]",
			outputResources: []string{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:hisrepo", "arn:lakefs:repos::b:ourrepo"},
			expectedError:   nil,
		},
		{
			inputResource:   "       arn:lakefs:repos::b:myrepo  ",
			outputResources: []string{"       arn:lakefs:repos::b:myrepo  "},
			expectedError:   nil,
		},
		{

			inputResource:   "[    \"arn:lakefs:repos::b:myrepo  \"]",
			outputResources: []string{"arn:lakefs:repos::b:myrepo  "},
			expectedError:   nil,
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myre,po\"]",
			outputResources: []string{"arn:lakefs:repos::b:myre,po"},
			expectedError:   nil,
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myre,po\",\"arn:lakefs:repos::b,:myrepo\"]",
			outputResources: []string{"arn:lakefs:repos::b:myre,po", "arn:lakefs:repos::b,:myrepo"},
			expectedError:   nil,
		},
		{
			inputResource:   "*",
			outputResources: []string{"*"},
			expectedError:   nil,
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myre,po,\"arn:lakefs:repos::b,:myrepo\"]",
			outputResources: []string{},
			expectedError:   fmt.Errorf("unmarshal resource"),
		},
		{
			inputResource:   "",
			outputResources: nil,
			expectedError:   auth.ErrInvalidArn,
		},
		{
			inputResource:   sliceToJsonStrHelper(t, "arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:hisrepo"),
			outputResources: []string{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:hisrepo"},
			expectedError:   nil,
		},
		{
			inputResource:   sliceToJsonStrHelper(t, ""),
			outputResources: []string{""},
			expectedError:   auth.ErrInvalidArn,
		},
		{
			inputResource:   sliceToJsonStrHelper(t, "arn:lakefs:repos::b:m\"yrepo  ", "arn:lakefs:repos::b:hisrepo"),
			outputResources: []string{"arn:lakefs:repos::b:m\"yrepo  ", "arn:lakefs:repos::b:hisrepo"},
			expectedError:   nil,
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myrepo\", arn:lakefs:repos::b:hisrepo\"]",
			outputResources: []string{},
			expectedError:   fmt.Errorf("unmarshal resource"),
		},
		{
			inputResource:   "[\"arn:lakefs:repos::b:myrepo\" \"arn:lakefs:repos::b:hisrepo\"]",
			outputResources: []string{},
			expectedError:   fmt.Errorf("unmarshal resource"),
		},
	}

	for _, c := range cases {
		got, err := auth.ParsePolicyResourceAsList(c.inputResource)
		if err != nil && !strings.Contains(err.Error(), c.expectedError.Error()) {
			t.Fatalf("expected %v error, to contain %v error", err, c.expectedError)
		}
		if len(got) != len(c.outputResources) {
			t.Fatalf("expected %d resources, got %d for input: %s", len(c.outputResources), len(got), c.inputResource)
		}
		if len(got) == 0 {
			continue

		}
		for i, expected := range c.outputResources {
			if got[i] != expected {
				t.Fatalf("expected resource %s at index %d, got %s for input: %s", expected, i, got[i], c.inputResource)
			}
		}
	}

}

func sliceToJsonStrHelper(t *testing.T, s ...string) string {
	t.Helper()
	jsStr, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("failed to marshal slice '%v' to json string: %v", s, err)
	}
	return string(jsStr)
}

func TestArnMatch(t *testing.T) {
	cases := []struct {
		InputSource      string
		InputDestination string
		Match            bool
	}{
		{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:*", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:my*", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:my*po", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:our*", "arn:lakefs:repos::b:myrepo", false},
		{"arn:lakefs:repos::b:my*own", "arn:lakefs:repos::b:myrepo", false},
		{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:*", false},
		{"arn:lakefs:repo:::*", "arn:lakefs:repo:::*", true},
		{"arn:lakefs:repo", "arn:lakefs:repo", false},
	}

	for _, c := range cases {
		got := auth.ArnMatch(c.InputSource, c.InputDestination)
		if got != c.Match {
			t.Fatalf("expected match %v, got %v on source = %s, destination = %s", c.Match, got, c.InputSource, c.InputDestination)
		}
	}
}
