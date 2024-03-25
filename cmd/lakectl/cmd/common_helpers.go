package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/manifoldco/promptui"
	"github.com/spf13/pflag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/term"
)

var isTerminal = true

const (
	PathDelimiter = "/"
)

const (
	LakectlInteractive     = "LAKECTL_INTERACTIVE"
	DeathMessage           = "{{.Error|red}}\nError executing command.\n"
	DeathMessageWithFields = "{{.Message|red}}\n{{.Status}}\n"
	WarnMessage            = "{{.Warning|yellow}}\n\n"
)

const (
	internalPageSize           = 1000 // when retrieving all records, use this page size under the hood
	defaultAmountArgumentValue = 100  // when no amount is specified, use this value for the argument

	defaultPollInterval = 3 * time.Second // default interval while pulling tasks status
	minimumPollInterval = time.Second     // minimum interval while pulling tasks status
	defaultPollTimeout  = time.Hour       // default expiry for pull status with no update
)

const resourceListTemplate = `{{.Table | table -}}
{{.Pagination | paginate }}
`

var ErrTaskNotCompleted = errors.New("task not completed")

//nolint:gochecknoinits
func init() {
	// disable colors if we're not attached to interactive TTY.
	// when an environment variable is set, we use it to control interactive mode
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
		"blue": func(arg interface{}) string {
			return text.FgHiBlue.Sprint(arg)
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
					b.WriteString(fmt.Sprint(cell))
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

func Warning(message string) {
	WriteTo(WarnMessage, struct{ Warning string }{Warning: "Warning: " + message}, os.Stderr)
}

func Die(errMsg string, code int) {
	WriteTo(DeathMessage, struct{ Error string }{Error: errMsg}, os.Stderr)
	os.Exit(code)
}

func DieFmt(msg string, args ...interface{}) {
	errMsg := fmt.Sprintf(msg, args...)
	Die(errMsg, 1)
}

type APIError interface {
	GetPayload() *apigen.Error
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

func PrintTable(rows [][]interface{}, headers []interface{}, paginator *apigen.Pagination, amount int) {
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
		DieFmt("%s %s", name, err)
	}
	if err = u.ValidateRepository(); err != nil {
		DieFmt("%s %s", name, err)
	}
	return u
}

func MustParseRefURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("%s %s", name, err)
	}
	if err = u.ValidateRef(); err != nil {
		DieFmt("%s %s", name, err)
	}
	return u
}

func MustParseBranchURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("%s %s", name, err)
	}
	if err = u.ValidateBranch(); err != nil {
		DieFmt("%s %s", name, err)
	}
	return u
}

func MustParsePathURI(name, s string) *uri.URI {
	u, err := uri.ParseWithBaseURI(s, baseURI)
	if err != nil {
		DieFmt("%s %s", name, err)
	}
	if err = u.ValidateFullyQualified(); err != nil {
		DieFmt("%s %s", name, err)
	}
	return u
}

const (
	AutoConfirmFlagName     = "yes"
	AutoConfigFlagShortName = "y"
	AutoConfirmFlagHelp     = "Automatically say yes to all confirmations"

	StdinFileName = "-"
)

func AssignAutoConfirmFlag(flags *pflag.FlagSet) {
	flags.BoolP(AutoConfirmFlagName, AutoConfigFlagShortName, false, AutoConfirmFlagHelp)
}

func Confirm(flags *pflag.FlagSet, question string) (bool, error) {
	yes, err := flags.GetBool(AutoConfirmFlagName)
	if err == nil && yes {
		// got auto confirm flag
		return true, nil
	}
	prm := promptui.Prompt{
		Label:     question,
		IsConfirm: true,
	}
	_, err = prm.Run()
	if err != nil {
		return false, err
	}
	return true, nil
}

// nopCloser wraps a ReadSeekCloser to ignore calls to Close().  It is io.NopCloser (or
// ioutils.NopCloser) for Seeks.
type nopCloser struct {
	io.ReadSeekCloser
}

func (nc *nopCloser) Close() error {
	return nil
}

// deleteOnClose wraps a File to be a ReadSeekCloser that deletes itself when closed.
type deleteOnClose struct {
	*os.File
}

func (d *deleteOnClose) Close() error {
	if err := os.Remove(d.Name()); err != nil {
		_ = d.File.Close() // "Only" file descriptor leak if close fails (but data might stay).
		return fmt.Errorf("delete on close: %w", err)
	}
	return d.File.Close()
}

// OpenByPath returns a reader from the given path.
// If the path is "-", it consumes Stdin and
// opens a readable copy that is either deleted (POSIX) or will delete itself on close
// (non-POSIX, notably WINs).
func OpenByPath(path string) (io.ReadSeekCloser, error) {
	if path != StdinFileName {
		return os.Open(path)
	}

	// check if stdin is seekable
	_, err := os.Stdin.Seek(0, io.SeekCurrent)
	if err == nil {
		return &nopCloser{ReadSeekCloser: os.Stdin}, nil
	}

	temp, err := os.CreateTemp("", "lakectl-stdin")
	if err != nil {
		return nil, fmt.Errorf("create temporary file to buffer stdin: %w", err)
	}
	if _, err = io.Copy(temp, os.Stdin); err != nil {
		return nil, fmt.Errorf("copy stdin to temporary file: %w", err)
	}
	if _, err = temp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind temporary copied file: %w", err)
	}
	// Try to delete the file.  This will fail on Windows, we shall try to
	// delete on close anyway.
	if os.Remove(temp.Name()) != nil {
		return &deleteOnClose{File: temp}, nil
	}
	return temp, nil
}

// Must return the call value or die with error if err is not nil
func Must[T any](v T, err error) T {
	if err != nil {
		DieErr(err)
	}
	return v
}
