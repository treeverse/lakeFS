package api_test

import (
	"bytes"
	"embed"
	"io/fs"
	"testing"

	"github.com/treeverse/lakefs/pkg/api"
	apiparams "github.com/treeverse/lakefs/pkg/api/params"
)

//go:embed testdata/*.html
var testdataFS embed.FS

func TestNewInjectIndexFS(t *testing.T) {
	const (
		name   = "testdata/first.html"
		marker = "<!-- code snippets -->"
	)
	snippets := []apiparams.CodeSnippet{
		{
			ID:   "code1",
			Code: "<script>console.log('code1')</script>",
		},
		{
			ID:   "code2",
			Code: "<script>console.log('code2')</script>",
		},
	}
	fsys, err := api.NewInjectIndexFS(testdataFS, name, marker, snippets)
	if err != nil {
		t.Fatal("Failed to create new inject", err)
	}

	t.Run("inject", func(t *testing.T) {
		data, err := fs.ReadFile(fsys, name)
		if err != nil {
			t.Fatalf("Failed to read '%s' file content", name)
		}
		if err != nil {
			t.Fatalf("Failed to read '%s' file: %s", name, err)
		}
		for _, item := range snippets {
			idx := bytes.Index(data, []byte(item.Code))
			if idx == -1 {
				t.Errorf("Snippet '%s' code, was not found in data", item.ID)
			}
		}
	})

	t.Run("nothing", func(t *testing.T) {
		const filename = "testdata/second.html"
		data, err := fs.ReadFile(fsys, filename)
		if err != nil {
			t.Fatalf("Failed to read '%s' file content", filename)
		}
		if err != nil {
			t.Fatalf("Failed to read '%s' file: %s", filename, err)
		}
		for _, item := range snippets {
			idx := bytes.Index(data, []byte(item.Code))
			if idx != -1 {
				t.Errorf("Snippet '%s' code, was found in data unexpected", item.ID)
			}
		}
	})
}
