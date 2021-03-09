package auth_test

import (
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
		{Input: "arn:lakefs:repos:a:b/myrepo", Arn: auth.Arn{
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
		{Input: "arn:lakefs:repos::/myrepo", Arn: auth.Arn{
			Partition:  "lakefs",
			Service:    "repos",
			Region:     "",
			AccountID:  "",
			ResourceID: "myrepo"}},
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

func TestArnMatch(t *testing.T) {
	cases := []struct {
		InputSource      string
		InputDestination string
		Match            bool
	}{
		{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:*", "arn:lakefs:repos::b:myrepo", true},
		{"arn:lakefs:repos::b:myrepo", "arn:lakefs:repos::b:*", false},
		{"arn:lakefs:repos::b/myrepo", "arn:lakefs:repos::b/*", false},
		{"arn:lakefs:repos::b/*", "arn:lakefs:repos::b/myrepo", true},
		{"arn:lakefs:repo", "arn:lakefs:repo", false},
	}

	for _, c := range cases {
		got := auth.ArnMatch(c.InputSource, c.InputDestination)
		if got != c.Match {
			t.Fatalf("expected match %v, got %v on source = %s, destination = %s", c.Match, got, c.InputSource, c.InputDestination)
		}
	}
}
