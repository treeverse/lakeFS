package local_test

import (
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"testing"
)

func TestParseGitUrl(t *testing.T) {
	cases := []struct {
		Url             string
		Err             error
		ExpectedOwner   string
		ExpectedProject string
		ExpectedServer  string
	}{
		{
			Url:             "git@github.com:treeverse/lakeFS.git",
			Err:             nil,
			ExpectedProject: "lakeFS",
			ExpectedOwner:   "treeverse",
			ExpectedServer:  "github.com",
		},
		{
			Url:             "ssh://git@github.com/treeverse/lakeFS.git",
			Err:             nil,
			ExpectedProject: "lakeFS",
			ExpectedOwner:   "treeverse",
			ExpectedServer:  "github.com",
		},
		{
			Url:             "https://github.com/treeverse/lakeFS.git",
			Err:             nil,
			ExpectedProject: "lakeFS",
			ExpectedOwner:   "treeverse",
			ExpectedServer:  "github.com",
		},
		{
			Url:             "git://git@192.168.1.20:MyGroup/MyProject.git",
			Err:             nil,
			ExpectedProject: "MyProject",
			ExpectedOwner:   "MyGroup",
			ExpectedServer:  "192.168.1.20",
		},
		{
			Url:             "git://git@192.168.1.20:22:MyGroup/MyProject.git",
			Err:             nil,
			ExpectedProject: "MyProject",
			ExpectedOwner:   "MyGroup",
			ExpectedServer:  "192.168.1.20:22",
		},
	}

	for _, c := range cases {
		t.Run(c.Url, func(t *testing.T) {
			parsed, err := local.ParseGitUrl(c.Url)
			if err != c.Err {
				t.Errorf("expected error: %v got %v", c.Err, err)
			}
			if err != nil {
				return
			}
			if parsed.Server != c.ExpectedServer {
				t.Errorf("expected server: '%s', got '%s'", c.ExpectedServer, parsed.Server)
			}
			if parsed.Owner != c.ExpectedOwner {
				t.Errorf("expected owner: '%s', got '%s'", c.ExpectedOwner, parsed.Owner)
			}
			if parsed.Project != c.ExpectedProject {
				t.Errorf("expected project: '%s', got '%s'", c.ExpectedProject, parsed.Project)
			}
		})
	}
}
