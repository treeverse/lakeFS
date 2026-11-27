package cmdutils

import (
	"strings"
	"testing"

	"github.com/jedib0t/go-pretty/v6/text"
)

func TestColors(t *testing.T) {
	text.EnableColors()
	defer text.DisableColors()
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{name: "plain", template: `abc`, want: "abc"},
		{name: "red", template: `{{"abc" | red}}def`, want: "\x1b[91mabc\x1b[0mdef"},
		{name: "yellow", template: `{{"abc" | yellow}}def`, want: "\x1b[93mabc\x1b[0mdef"},
		{name: "green", template: `{{"abc" | green}}def`, want: "\x1b[92mabc\x1b[0mdef"},
		{name: "blue", template: `{{"abc" | blue}}def`, want: "\x1b[94mabc\x1b[0mdef"},
		{name: "bold", template: `{{"abc" | bold}}def`, want: "\x1b[1mabc\x1b[0mdef"},
		{name: "underline", template: `{{"abc" | underline}}def`, want: "\x1b[4mabc\x1b[0mdef"},
		{name: "boldgreen", template: `{{"abc" | bold | green}}def`, want: "\x1b[1;92mabc\x1b[0mdef"},
		{name: "greenbold", template: `{{"abc" | green | bold}}def`, want: "\x1b[92;1mabc\x1b[0mdef"},
		{name: "redunderline", template: `{{"abc" | red | underline}}def`, want: "\x1b[91;4mabc\x1b[0mdef"},
		{name: "red-number", template: `{{2 | red}}`, want: "\x1b[91m2\x1b[0m"},
		{name: "red-pointer", template: `{{. | red}}`, want: "\x1b[91mxyzzy\x1b[0m"},
	}

	text := "xyzzy"
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := strings.Builder{}
			WriteTo(tc.template, &text, &w)
			got := w.String()
			if got != tc.want {
				t.Errorf("%s got %q want %q", tc.template, got, tc.want)
			}
			// Show off the nice colors!
			t.Log(got)
		})
	}
}

func TestTableNotInteractive(t *testing.T) {
	DisableColors()
	t.Cleanup(func() {
		text.EnableColors()
		interactive = true
	})

	tab := &Table{
		Headers: []any{"id", "name"},
		Rows: [][]any{
			{"1", "first"},
			{"2", "second"},
		},
	}
	w := strings.Builder{}
	WriteTo("{{.Table | table}}", struct{ Table *Table }{Table: tab}, &w)

	const want = "1\tfirst\n2\tsecond\n"
	if got := w.String(); got != want {
		t.Errorf("table got %q want %q", got, want)
	}
	if Interactive() {
		t.Error("Interactive() = true after DisableColors()")
	}
}

func TestPaginationNotInteractive(t *testing.T) {
	DisableColors()
	t.Cleanup(func() {
		text.EnableColors()
		interactive = true
	})

	pag := &Pagination{Amount: 100, HasNext: true, After: "cursor"}
	w := strings.Builder{}
	WriteTo("{{.Pagination | paginate }}", struct{ Pagination *Pagination }{Pagination: pag}, &w)

	if got := w.String(); got != "" {
		t.Errorf("paginate got %q want empty", got)
	}
}
