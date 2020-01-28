package mime_test

import (
	"os"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/mime"
)

func TestResolver_Resolve(t *testing.T) {
	file, err := os.Open("testdata/mime-db.json")
	if err != nil {
		t.Fatalf("unexpected error opening fixture db: %s", err)
	}
	resolver, err := mime.BuildResolver(file)
	if err != nil {
		t.Fatalf("unexpected error opening fixture db: %s", err)
	}
	cases := []struct {
		Filename string
		Expected string
	}{
		{"file.foo.xml", "application/xml"},
		{"mymovie.mkv", "video/x-matroska"},
		{"a/b.mpg", "video/mpeg"},
		{"fofofofofo.fofofo", "application/octet-stream"},
		{"/something/something-else", "application/octet-stream"},
		{"xml", "application/octet-stream"},
	}
	for _, cas := range cases {
		got := resolver.Resolve(cas.Filename)
		if !strings.EqualFold(got, cas.Expected) {
			t.Fatalf("expected %s got %s for filename %s", cas.Expected, got, cas.Filename)
		}
	}
}
