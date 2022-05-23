package api_test

import (
	"bytes"
	"embed"
	_ "embed"
	"io/fs"
	"testing"

	"github.com/treeverse/lakefs/pkg/api"
)

//go:embed testdata/*.html
var testdataFS embed.FS

func TestNewInjectIndexFS(t *testing.T) {
	const (
		name   = "testdata/first.html"
		marker = "<!-- code snippets -->"
	)
	snippets := map[string]string{
		"code1": "<script>console.log('code1')</script>",
		"code2": "<script>console.log('code2')</script>",
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
		for tag, code := range snippets {
			idx := bytes.Index(data, []byte(code))
			if idx == -1 {
				t.Errorf("Snippet '%s' code, was not found in data", tag)
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
		for tag, code := range snippets {
			idx := bytes.Index(data, []byte(code))
			if idx != -1 {
				t.Errorf("Snippet '%s' code, was found in data unexpected", tag)
			}
		}
	})
}
