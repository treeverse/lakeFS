package local_test

import (
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
)

func TestParseGithubUrl(t *testing.T) {
	cases := []string{
		"git@github.com:treeverse/lakeFS.git",
	}

	for i, cas := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			parsed, err := local.ParseGitUrl(cas)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Name != "lakeFS" {
				t.Errorf("case: %s - wrong repo: %s", cas, parsed.Name)
			}
			if parsed.Org != "treeverse" {
				t.Errorf("case: %s - wrong org: %s", cas, parsed.Org)
			}
			if parsed.Domain != "github.com" {
				t.Errorf("case: %s - wrong domain: %s", cas, parsed.Domain)
			}
		})
	}
}
