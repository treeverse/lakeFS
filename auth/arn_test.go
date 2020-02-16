package auth_test

import (
	"testing"

	"github.com/treeverse/lakefs/auth"
)

func TestParseARN(t *testing.T) {
	cases := []struct {
		Input string
		Arn   auth.Arn
		Error bool
	}{
		{Input: "", Error: true},
		{Input: "arn:treeverse:repo", Error: true},
		{Input: "arn:treeverse:repos:a:b:myrepo", Arn: auth.Arn{
			Partition:  "treeverse",
			Service:    "repos",
			Region:     "a",
			AccountId:  "b",
			ResourceId: "myrepo"}},
		{Input: "arn:treeverse:repos:a:b/myrepo", Arn: auth.Arn{
			Partition:  "treeverse",
			Service:    "repos",
			Region:     "a",
			AccountId:  "b",
			ResourceId: "myrepo"}},
		{Input: "arn:treeverse:repos:a::myrepo", Arn: auth.Arn{
			Partition:  "treeverse",
			Service:    "repos",
			Region:     "a",
			AccountId:  "",
			ResourceId: "myrepo"}},
		{Input: "arn:treeverse:repos::b:myrepo", Arn: auth.Arn{
			Partition:  "treeverse",
			Service:    "repos",
			Region:     "",
			AccountId:  "b",
			ResourceId: "myrepo"}},
		{Input: "arn:treeverse:repos::/myrepo", Arn: auth.Arn{
			Partition:  "treeverse",
			Service:    "repos",
			Region:     "",
			AccountId:  "",
			ResourceId: "myrepo"}},
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
		if got.AccountId != c.Arn.AccountId {
			t.Fatalf("got unexpected account ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.AccountId, c.Arn.AccountId)
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
		if got.ResourceId != c.Arn.ResourceId {
			t.Fatalf("got unexpected resource ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.ResourceId, c.Arn.ResourceId)
		}
	}
}

func TestArnMatch(t *testing.T) {
	cases := []struct {
		InputSource      string
		InputDestination string
		Match            bool
	}{
		{"arn:treeverse:repos::b:myrepo", "arn:treeverse:repos::b:myrepo", true},
		{"arn:treeverse:repos::b:*", "arn:treeverse:repos::b:myrepo", true},
		{"arn:treeverse:repos::b:myrepo", "arn:treeverse:repos::b:*", false},
		{"arn:treeverse:repos::b/myrepo", "arn:treeverse:repos::b/*", false},
		{"arn:treeverse:repos::b/*", "arn:treeverse:repos::b/myrepo", true},
		{"arn:treeverse:repo", "arn:treeverse:repo", false},
	}

	for _, c := range cases {
		got := auth.ArnMatch(c.InputSource, c.InputDestination)
		if got != c.Match {
			t.Fatalf("expected match %v, got %v on source = %s, destination = %s", c.Match, got, c.InputSource, c.InputDestination)
		}
	}
}
