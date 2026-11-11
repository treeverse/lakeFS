package cmdutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

// interactive reports whether output is rendered for a terminal. When it is
// false, output is machine friendly: no colors, no pagination hint and tables
// are written as tab-separated rows.
var interactive = true

// DisableColors disables ANSI colors and terminal-only formatting.
func DisableColors() {
	text.DisableColors()
	interactive = false
}

// Interactive reports whether output is rendered for a terminal.
func Interactive() bool {
	return interactive
}

// Table is the table rendered by the "table" template function.
type Table struct {
	Headers []any
	Rows    [][]any
}

// Pagination is the pagination hint rendered by the "paginate" template function.
type Pagination struct {
	Amount  int
	HasNext bool
	After   string
}

// ColoredText is text with an array of colors applied to it.  Without it go-pretty cannot
// render combinations, e.g. bold-green.
type ColoredText struct {
	Colors text.Colors
	Text   any // Usually just a string, but anything goes
}

func deref(a any) string {
	r := reflect.ValueOf(a)
	for {
		next := reflect.Indirect(r)
		if next == r {
			break
		}
		r = next
	}
	i := r.Interface()
	return fmt.Sprint(i)
}

// String converts c to a string with ANSI control codes.
func (c ColoredText) String() string {
	t := deref(c.Text)
	return c.Colors.Sprint(t)
}

// Add returns c with color added to it.
func (c ColoredText) Add(color text.Color) ColoredText {
	return ColoredText{
		Colors: append(c.Colors, color),
		Text:   c.Text,
	}
}

// Colored returns text as ColoredText with no Color if it is not already ColoredText.
func Colored(val any) ColoredText {
	if c, ok := val.(ColoredText); ok {
		return c
	}
	if t, ok := val.(fmt.Stringer); ok {
		val = t.String()
	}
	return ColoredText{Colors: nil, Text: val}
}

// WriteTo renders tpl with data into w.
func WriteTo(tpl string, data any, w io.Writer) {
	templ := template.New("output")
	templ.Funcs(template.FuncMap{
		"red": func(arg any) ColoredText {
			return Colored(arg).Add(text.FgHiRed)
		},
		"yellow": func(arg any) ColoredText {
			return Colored(arg).Add(text.FgHiYellow)
		},
		"green": func(arg any) ColoredText {
			return Colored(arg).Add(text.FgHiGreen)
		},
		"blue": func(arg any) ColoredText {
			return Colored(arg).Add(text.FgHiBlue)
		},
		"bold": func(arg any) ColoredText {
			return Colored(arg).Add(text.Bold)
		},
		"underline": func(arg any) ColoredText {
			return Colored(arg).Add(text.Underline)
		},
		"date": func(ts int64) string {
			return time.Unix(ts, 0).String()
		},
		"ljust": func(length int, s string) string {
			return text.AlignLeft.Apply(s, length)
		},
		"json": func(v any) string {
			encoded, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				panic(fmt.Sprintf("failed to encode JSON: %s", err.Error()))
			}
			return string(encoded)
		},
		"paginate": func(pag *Pagination) string {
			if pag != nil && pag.Amount > 0 && pag.HasNext && interactive {
				params := text.FgHiYellow.Sprintf("--amount %d --after \"%s\"", pag.Amount, pag.After)
				return fmt.Sprintf("for more results run with %s\n", params)
			}
			return ""
		},
		"lower": strings.ToLower,
		"human_bytes": func(b int64) string {
			var unit int64 = 1000
			if b < unit {
				return fmt.Sprintf("%d B", b)
			}
			div, exp := unit, 0
			for n := b / unit; n >= unit; n /= unit {
				div *= unit
				exp++
			}
			return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
		},
		"join": func(sep string, args []string) string {
			return strings.Join(args, sep)
		},
		"table": func(tab *Table) string {
			if interactive {
				buf := new(bytes.Buffer)
				t := table.NewWriter()
				t.SetOutputMirror(buf)
				t.AppendHeader(tab.Headers)
				for _, row := range tab.Rows {
					t.AppendRow(row)
				}
				t.Render()
				return buf.String()
			}
			var b strings.Builder
			for _, row := range tab.Rows {
				for ic, cell := range row {
					fmt.Fprint(&b, cell)
					if ic < len(row)-1 {
						b.WriteString("\t")
					}
				}
				b.WriteString("\n")
			}
			return b.String()
		},
	})
	t := template.Must(templ.Parse(tpl))
	err := t.Execute(w, data)
	if err != nil {
		panic(err)
	}
}

// Write renders tpl with data to the standard output.
func Write(tpl string, data any) {
	WriteTo(tpl, data, os.Stdout)
}
