package templater

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"text/template"

	"github.com/treeverse/lakefs/pkg/auth"
	auth_model "github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	// Prefix inside templates FS to access actual contents.
	embeddedContentPrefix = "content"
)

var (
	ErrNotFound             = errors.New("template not found")
	ErrPathTraversalBlocked = errors.New("path traversal blocked")
)

type AuthService interface {
	auth.Authorizer
	auth.CredentialsCreator
}

type Phase string

const (
	PhasePrepare Phase = "prepare"
	PhaseExpand  Phase = "expand"
)

type ControlledParams struct {
	Ctx   context.Context
	Phase Phase
	Auth  AuthService
	// Headers are the HTTP response headers.  They may only be modified
	// during PhasePrepare.
	Header http.Header
	// User is the user expanding the template.
	User *auth_model.User
	// Store is a place for funcs to keep stuff around, mostly between
	// phases.  Each func should use its name as its index.
	Store map[string]interface{}
}

type UncontrolledData struct {
	// UserName is the name of the executing user.
	Username string
	// Query is the (parsed) querystring of the HTTP access.
	Query map[string]string
}

// Params parametrizes a single template expansion.
type Params struct {
	// Controlled is the data visible to functions to control expansion.
	// It is _not_ directly visible to templates for expansion.
	Controlled *ControlledParams
	// Data is directly visible to templates for expansion, with no
	// authorization required.
	Data *UncontrolledData
}

// Expander is a template that may be expanded as requested by users.
type Expander interface {
	// Expand serves the template into w using the parameters specified
	// in params.  If during expansion a template function fails,
	// returns an error without writing anything to w.  (However if
	// expansion fails for other reasons, Expand may write to w!)
	Expand(w io.Writer, params *Params) error
}

type expander struct {
	template *template.Template
	cfg      *config.Config
	auth     AuthService
}

// MakeExpander creates an expander for the text of tmpl.
func MakeExpander(name, tmpl string, cfg *config.Config, auth AuthService) (Expander, error) {
	t := template.New(name).Funcs(templateFuncs).Option("missingkey=error")
	t, err := t.Parse(tmpl)
	if err != nil {
		return nil, err
	}

	return &expander{
		template: t,
		cfg:      cfg,
		auth:     auth,
	}, nil
}

func (e *expander) Expand(w io.Writer, params *Params) error {
	// Expand with no output: verify that no template functions will
	// fail.
	params.Controlled.Phase = PhasePrepare
	if err := e.expandTo(io.Discard, params); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	params.Controlled.Phase = PhaseExpand
	if err := e.expandTo(w, params); err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	return nil
}

func (e *expander) expandTo(w io.Writer, params *Params) error {
	clone, err := e.template.Clone()
	if err != nil {
		return err
	}
	wrappedFuncs := WrapFuncMapWithData(templateFuncs, params.Controlled)
	clone.Funcs(wrappedFuncs)
	return clone.Execute(w, params.Data)
}

// ExpanderMap reads and caches Expanders from a fs.FS.  Currently, it
// provides no uncaching as it is only used with a prebuilt FS.
type ExpanderMap struct {
	fs   fs.FS
	cfg  *config.Config
	auth AuthService

	expanders map[string]Expander
}

func NewExpanderMap(fs fs.FS, cfg *config.Config, auth AuthService) *ExpanderMap {
	return &ExpanderMap{
		fs:        fs,
		cfg:       cfg,
		auth:      auth,
		expanders: make(map[string]Expander, 0),
	}
}

func (em *ExpanderMap) Get(_ context.Context, _, name string) (Expander, error) {
	if e, ok := em.expanders[name]; ok {
		// Fast-path through the cache
		if e == nil {
			// Negative cache
			return nil, ErrNotFound
		}
		return e, nil
	}

	// Compute path
	p := path.Join(embeddedContentPrefix, name)
	if !strings.HasPrefix(p, embeddedContentPrefix+"/") {
		// Path traversal, fail
		return nil, fmt.Errorf("%s: %w", name, ErrPathTraversalBlocked)
	}

	tmpl, err := fs.ReadFile(em.fs, p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	e, err := MakeExpander(name, string(tmpl), em.cfg, em.auth)
	if err != nil {
		// Store negative cache result
		e = nil
	}
	em.expanders[name] = e
	return e, err
}
