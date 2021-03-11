package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/treeverse/lakefs/pkg/api/gen/models"
	"golang.org/x/term"
)

var isTerminal = true
var noColorRequested = false

const (
	LakectlInteractive        = "LAKECTL_INTERACTIVE"
	LakectlInteractiveDisable = "no"
	DeathMessage              = "Error executing command: {{.Error|red}}\n"
)

const resourceListTemplate = `{{.Table | table -}}
{{.Pagination | paginate }}
`

//nolint:gochecknoinits
func init() {
	// disable colors if we're not attached to interactive TTY
	if !term.IsTerminal(int(os.Stdout.Fd())) || os.Getenv(LakectlInteractive) == LakectlInteractiveDisable || noColorRequested {
		DisableColors()
	}
}

func DisableColors() {
	text.DisableColors()
	isTerminal = false
}

type Table struct {
	Headers []interface{}
	Rows    [][]interface{}
}

type Pagination struct {
	Amount  int
	HasNext bool
	After   string
}

func WriteTo(tpl string, data interface{}, w io.Writer) {
	templ := template.New("output")
	templ.Funcs(template.FuncMap{
		"red": func(arg interface{}) string {
			return text.FgHiRed.Sprint(arg)
		},
		"yellow": func(arg interface{}) string {
			return text.FgHiYellow.Sprint(arg)
		},
		"green": func(arg interface{}) string {
			return text.FgHiGreen.Sprint(arg)
		},
		"bold": func(arg interface{}) string {
			return text.Bold.Sprint(arg)
		},
		"date": func(ts int64) string {
			return time.Unix(ts, 0).String()
		},
		"ljust": func(length int, s string) string {
			return text.AlignLeft.Apply(s, length)
		},
		"json": func(v interface{}) string {
			encoded, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				panic(fmt.Sprintf("failed to encode JSON: %s", err.Error()))
			}
			return string(encoded)
		},
		"paginate": func(pag *Pagination) string {
			if pag != nil && pag.HasNext && isTerminal {
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
					b.WriteString(fmt.Sprintf("%s", cell))
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

func Write(tpl string, data interface{}) {
	WriteTo(tpl, data, os.Stdout)
}

func Die(err string, code int) {
	WriteTo(DeathMessage, struct{ Error string }{err}, os.Stderr)
	os.Exit(code)
}

func DieFmt(msg string, args ...interface{}) {
	Die(fmt.Sprintf(msg, args...), 1)
}

type APIError interface {
	GetPayload() *models.Error
}

func DieErr(err error) {
	errData := struct{ Error string }{}
	apiError, isAPIError := err.(APIError)
	if isAPIError {
		errData.Error = apiError.GetPayload().Message
	}
	if errData.Error == "" {
		errData.Error = err.Error()
	}
	WriteTo(DeathMessage, errData, os.Stderr)
	os.Exit(1)
}

func Fmt(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
}

func PrintTable(rows [][]interface{}, headers []interface{}, paginator *models.Pagination, amount int) {
	ctx := struct {
		Table      *Table
		Pagination *Pagination
	}{
		Table: &Table{
			Headers: headers,
			Rows:    rows,
		},
	}
	if paginator != nil && swag.BoolValue(paginator.HasMore) {
		ctx.Pagination = &Pagination{
			Amount:  amount,
			HasNext: true,
			After:   paginator.NextOffset,
		}
	}

	Write(resourceListTemplate, ctx)
}
