package cmdutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
)

// isTerminal is true while output may use terminal-only features, such as
// pagination hints and aligned tables.  DisableColors turns it off.
var isTerminal = true

// Templates shared by the lakeFS command line tools.  Write them with WriteTo
// or Write, or use the helpers below.
const (
	DeathMessage           = "{{.Error|red}}\nError executing command.\n"
	DeathMessageWithFields = "{{.Message|red}}\n{{.Status}}\n"
	WarnMessage            = "{{.Prefix|yellow}} {{.Message}}\n\n"
	InfoMessage            = "{{.Prefix|blue}} {{.Message}}\n"
	ErrorMessage           = "{{.Prefix|red}} {{.Message}}\n"

	resourceListTemplate = `{{.Table | table -}}
{{.Pagination | paginate }}
`
)

// DisableColors disables colored output and the features that assume an
// interactive terminal for all writers in this package.
func DisableColors() {
	text.DisableColors()
	isTerminal = false
}

// IsTerminal reports whether output is still meant for an interactive terminal.
// It is false once DisableColors has been called.
func IsTerminal() bool {
	return isTerminal
}

// Table is a set of rows to render with the "table" template function.
type Table struct {
	Headers []any
	Rows    [][]any
}

// Pagination describes how to fetch the next page of a listing.
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
func Colored(text any) ColoredText {
	if c, ok := text.(ColoredText); ok {
		return c
	}
	if t, ok := text.(fmt.Stringer); ok {
		text = t.String()
	}
	return ColoredText{Colors: nil, Text: text}
}

// WriteTo renders tpl with data and writes the result to w.
func WriteTo(w io.Writer, tpl string, data any) {
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
			if pag != nil && pag.Amount > 0 && pag.HasNext && isTerminal {
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
			if isTerminal {
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

// Write renders tpl with data and writes the result to stdout.
func Write(tpl string, data any) {
	WriteTo(os.Stdout, tpl, data)
}

// Warning writes message to stderr as a warning.
func Warning(message string) {
	WriteTo(os.Stderr, WarnMessage, struct{ Prefix, Message string }{Prefix: "Warning:", Message: message})
}

// InfoFmt writes a formatted message to stderr as an informational message.
func InfoFmt(format string, args ...any) {
	WriteTo(os.Stderr, InfoMessage, struct{ Prefix, Message string }{Prefix: "Info:", Message: fmt.Sprintf(format, args...)})
}

// NonFatalErrorFmt writes a formatted message to stderr as an error and
// returns, so that the command can carry on.
func NonFatalErrorFmt(format string, args ...any) {
	WriteTo(os.Stderr, ErrorMessage, struct{ Prefix, Message string }{Prefix: "Error:", Message: fmt.Sprintf(format, args...)})
}

// Die writes errMsg to stderr as an error and exits with code.
func Die(errMsg string, code int) {
	WriteTo(os.Stderr, DeathMessage, struct{ Error string }{Error: errMsg})
	os.Exit(code)
}

// DieFmt is Die with fmt.Sprintf-style formatting.
func DieFmt(msg string, args ...any) {
	Die(fmt.Sprintf(msg, args...), 1)
}

// APIError is implemented by errors that carry an API error payload.
type APIError interface {
	GetPayload() *apigen.Error
}

// DieErr writes err to stderr as an error and exits with code 1.
func DieErr(err error) {
	type ErrData struct {
		Error string
	}
	var (
		apiError         APIError
		userVisibleError helpers.UserVisibleAPIError
	)
	apiError, _ = err.(APIError)
	switch {
	case errors.As(err, &userVisibleError):
		WriteTo(os.Stderr, DeathMessageWithFields, userVisibleError.APIFields)
	case apiError != nil:
		WriteTo(os.Stderr, DeathMessage, ErrData{Error: apiError.GetPayload().Message})
	default:
		WriteTo(os.Stderr, DeathMessage, ErrData{Error: err.Error()})
	}
	os.Exit(1)
}

// PrintTable writes rows and headers to stdout, followed by a hint on how to
// fetch the next page when paginator has more results.
func PrintTable(rows [][]any, headers []any, paginator *apigen.Pagination, amount int) {
	ctx := struct {
		Table      *Table
		Pagination *Pagination
	}{
		Table: &Table{
			Headers: headers,
			Rows:    rows,
		},
	}
	if paginator.HasMore {
		ctx.Pagination = &Pagination{
			Amount:  amount,
			HasNext: true,
			After:   paginator.NextOffset,
		}
	}

	Write(resourceListTemplate, ctx)
}
