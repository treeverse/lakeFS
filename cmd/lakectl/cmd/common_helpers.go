package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/term"
)

var (
	isTerminal       = true
	noColorRequested = false
	verboseMode      = false
)

// ErrInvalidValueInList is an error returned when a parameter of type list contains an empty string
var ErrInvalidValueInList = errors.New("empty string in list")

var accessKeyRegexp = regexp.MustCompile(`^AKIA[I|J][A-Z0-9]{14}Q$`)

const (
	PathDelimiter = "/"
)

const (
	LakectlInteractive     = "LAKECTL_INTERACTIVE"
	DeathMessage           = "{{.Error|red}}\nError executing command.\n"
	DeathMessageWithFields = "{{.Message|red}}\n{{.Status}}\n"
)

const (
	internalPageSize           = 1000 // when retrieving all records, use this page size under the hood
	defaultAmountArgumentValue = 100  // when no amount is specified, use this value for the argument
)

const resourceListTemplate = `{{.Table | table -}}
{{.Pagination | paginate }}
`

//nolint:gochecknoinits
func init() {
	// disable colors if we're not attached to interactive TTY.
	// when environment variable is set we use it to control interactive mode
	// otherwise we will try to detect based on the standard output
	interactiveVal := os.Getenv(LakectlInteractive)
	if interactiveVal != "" {
		if interactive, err := strconv.ParseBool(interactiveVal); err == nil && !interactive {
			DisableColors()
		}
	} else if !term.IsTerminal(int(os.Stdout.Fd())) {
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
					callValue := fmt.Sprintf("%s", cell)
					b.WriteString(callValue)
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

func WriteIfVerbose(tpl string, data interface{}) {
	if verboseMode {
		WriteTo(tpl, data, os.Stdout)
	}
}

func Die(err string, code int) {
	WriteTo(DeathMessage, struct{ Error string }{err}, os.Stderr)
	os.Exit(code)
}

func DieFmt(msg string, args ...interface{}) {
	Die(fmt.Sprintf(msg, args...), 1)
}

type APIError interface {
	GetPayload() *api.Error
}

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
		WriteTo(DeathMessageWithFields, userVisibleError.APIFields, os.Stderr)
	case apiError != nil:
		WriteTo(DeathMessage, ErrData{Error: apiError.GetPayload().Message}, os.Stderr)
	default:
		WriteTo(DeathMessage, ErrData{Error: err.Error()}, os.Stderr)
	}
	os.Exit(1)
}

type StatusCoder interface {
	StatusCode() int
}

func RetrieveError(response interface{}, err error) error {
	if err != nil {
		return err
	}
	return helpers.ResponseAsError(response)
}

func dieOnResponseError(response interface{}, err error) {
	retrievedErr := RetrieveError(response, err)
	if retrievedErr != nil {
		DieErr(retrievedErr)
	}
}

func DieOnErrorOrUnexpectedStatusCode(response interface{}, err error, expectedStatusCode int) {
	dieOnResponseError(response, err)
	var statusCode int
	if httpResponse, ok := response.(*http.Response); ok {
		statusCode = httpResponse.StatusCode
	} else {
		r := reflect.Indirect(reflect.ValueOf(response))
		f := r.FieldByName("HTTPResponse")
		httpResponse, _ := f.Interface().(*http.Response)
		if httpResponse != nil {
			statusCode = httpResponse.StatusCode
		}
	}

	if statusCode == 0 {
		Die("could not get status code from response", 1)
	}
	if statusCode != expectedStatusCode {
		DieFmt("got unexpected status code: %d, expected: %d", statusCode, expectedStatusCode)
	}
}

func DieOnHTTPError(httpResponse *http.Response) {
	err := helpers.HTTPResponseAsError(httpResponse)
	if err != nil {
		DieErr(err)
	}
}

func Fmt(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
}

func PrintTable(rows [][]interface{}, headers []interface{}, paginator *api.Pagination, amount int) {
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

func MustParseRepoURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("Invalid '%s': %s", name, err)
	}
	if !u.IsRepository() {
		DieFmt("Invalid '%s': %s", name, uri.ErrInvalidRepoURI)
	}
	return u
}

func MustParseRefURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("Invalid '%s': %s", name, err)
	}
	if !u.IsRef() {
		DieFmt("Invalid %s: %s", name, uri.ErrInvalidRefURI)
	}
	return u
}

func MustParseBranchURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("Invalid '%s': %s", name, err)
	}
	if !u.IsBranch() {
		DieFmt("Invalid %s: %s", name, uri.ErrInvalidBranchURI)
	}
	return u
}

func MustParsePathURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("Invalid '%s': %s", name, err)
	}
	if !u.IsFullyQualified() {
		DieFmt("Invalid '%s': %s", name, uri.ErrInvalidPathURI)
	}
	return u
}

func IsValidAccessKeyID(accessKeyID string) bool {
	return accessKeyRegexp.MatchString(accessKeyID)
}

func IsValidSecretAccessKey(secretAccessKey string) bool {
	return IsBase64(secretAccessKey) && len(secretAccessKey) == 40
}

func IsBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}
