package auth_test

import (
	"testing"
	"versio-index/auth"
)

func TestParseARN(t *testing.T) {
	cases := []struct {
		Input string
		Arn   auth.Arn
		Error bool
	}{
		{Input: "", Error: true},
	}

	for _, c := range cases {
		got, err := auth.ParseARN(c.Input)
		if err != nil && !c.Error {
			t.Fatalf("got unexpected error parsing arn: %s: %s", c.Input, err)
		} else if err != nil {
			continue
		}
		if got.AccountId != c.Arn.AccountId {
			t.Fatalf("got unexpected account ID parsing arn: %s: %s", c.Input, got.AccountId)
		}
		if got.Region != c.Arn.Region {
			t.Fatalf("got unexpected region parsing arn: %s: %s", c.Input, got.Region)
		}
		if got.Partition != c.Arn.Partition {
			t.Fatalf("got unexpected partition parsing arn: %s: %s", c.Input, got.Partition)
		}
		if got.Service != c.Arn.Service {
			t.Fatalf("got unexpected service parsing arn: %s: %s", c.Input, got.Service)
		}
		if got.ResourceId != c.Arn.ResourceId {
			t.Fatalf("got unexpected region parsing arn: %s: %s", c.Input, got.ResourceId)
		}
	}
}

func TestArnMatch(t *testing.T) {

}
