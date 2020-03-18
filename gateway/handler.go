package gateway

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/treeverse/lakefs/gateway/operations"

	"github.com/treeverse/lakefs/index"
)

const DebugPprofPrefix = "/debug/pprof/"

type Handler struct {
	BareDomain string
	ctx        *ServerContext

	NotFoundHandler    http.Handler
	ServerErrorHandler http.Handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pprof endpoints
	h.servePprof(w, r)

	// handle path based
	h.servePathBased(w, r)

	// handle virtual host
	h.serveVirtualHost(w, r)
}

func (h *Handler) servePprof(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, DebugPprofPrefix) {
		return
	}
	endpoint := strings.TrimPrefix(r.URL.Path, DebugPprofPrefix)
	switch endpoint {
	case "":
		pprof.Index(w, r)
	case "cmdline":
		pprof.Cmdline(w, r)
	case "profile":
		pprof.Profile(w, r)
	case "symbol":
		pprof.Symbol(w, r)
	case "trace":
		pprof.Trace(w, r)
	case "block", "goroutine", "heap", "threadcreate":
		pprof.Handler(endpoint).ServeHTTP(w, r)
	default:
		h.NotFoundHandler.ServeHTTP(w, r)
	}
}

func (h *Handler) pathBasedHandler(method, repositoryId, ref, path string) http.Handler {
	var handler operations.PathOperationHandler
	switch method {
	case http.MethodDelete:
		handler = &operations.DeleteObject{}
	case http.MethodPost:
		handler = &operations.PostObject{}
	case http.MethodGet:
		handler = &operations.GetObject{}
	case http.MethodHead:
		handler = &operations.HeadObject{}
	case http.MethodPut:
		handler = &operations.PutObject{}
	default:
		return h.NotFoundHandler
	}

	return PathOperationHandler(h.ctx, repositoryId, ref, path, handler)
}

func (h *Handler) repositoryBasedHandler(method, repositoryId string) http.Handler {
	var handler operations.RepoOperationHandler
	switch method {
	case http.MethodDelete, http.MethodPut:
		return unsupportedOperationHandler()
	case http.MethodHead:
		handler = &operations.HeadBucket{}
	case http.MethodPost:
		handler = &operations.DeleteObjects{}
	case http.MethodGet:
		handler = &operations.ListObjects{}
	default:
		return h.NotFoundHandler
	}

	return RepoOperationHandler(h.ctx, repositoryId, handler)
}

func getHost(r *http.Request) (string, error) {
	if strings.Contains(r.Host, ":") {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			return "", err
		}
		return host, nil
	}
	return r.Host, nil
}

func (h *Handler) servePathBased(w http.ResponseWriter, r *http.Request) {
	host := r.Host

	if !strings.EqualFold(host, h.BareDomain) {
		return // maybe it's a virtual host, but def not a path based request because the host is wrong
	}

	if parts, ok := SplitFirst(r.URL.Path, 3); ok {
		repositoryId := parts[0]
		ref := parts[1]
		key := parts[2]
		if err := index.ValidateAll(
			index.ValidateRepoId(repositoryId),
			index.ValidateRef(ref),
			index.ValidatePath(key),
		); err != nil {
			h.NotFoundHandler.ServeHTTP(w, r)
			return // invalid path
		}

		h.pathBasedHandler(r.Method, repositoryId, ref, key).ServeHTTP(w, r)
		return
	}

	// Paths for repository and ref only (none exist)
	if _, ok := SplitFirst(r.URL.Path, 2); ok {
		h.NotFoundHandler.ServeHTTP(w, r)
		return
	}

	if parts, ok := SplitFirst(r.URL.Path, 1); ok {
		// Paths for bare repository
		repositoryId := parts[0]
		if err := index.ValidateAll(
			index.ValidateRepoId(repositoryId),
		); err != nil {
			h.NotFoundHandler.ServeHTTP(w, r)
			return
		}

		h.repositoryBasedHandler(r.Method, repositoryId).ServeHTTP(w, r)
		return
	}

	// no repository given
	switch r.Method {
	case http.MethodGet:
		OperationHandler(h.ctx, &operations.ListBuckets{}).ServeHTTP(w, r)
		return
	}

	h.NotFoundHandler.ServeHTTP(w, r)
}

func (h *Handler) serveVirtualHost(w http.ResponseWriter, r *http.Request) {
	// is it a virtual host?
	host := r.Host

	if !strings.HasSuffix(host, h.BareDomain) {
		h.NotFoundHandler.ServeHTTP(w, r)
		return // not a virtual host
	}

	// remove bare domain suffix
	repositoryId := strings.TrimSuffix(host, fmt.Sprintf(".%s", h.BareDomain))

	if err := index.ValidateRepoId(repositoryId); err != nil {
		h.NotFoundHandler.ServeHTTP(w, r)
		return
	}

	// Paths that have both a repository, a refId and a path
	if parts, ok := SplitFirst(r.URL.Path, 2); ok {
		// validate ref, key
		ref := parts[0]
		key := parts[1]
		if err := index.ValidateAll(index.ValidateRef(ref), index.ValidatePath(key)); err != nil {
			h.NotFoundHandler.ServeHTTP(w, r)
			return
		}
		h.pathBasedHandler(r.Method, repositoryId, ref, key).ServeHTTP(w, r)
		return
	}

	// Paths that only have a repository and a refId (always 404)
	if _, ok := SplitFirst(r.URL.Path, 1); ok {
		h.NotFoundHandler.ServeHTTP(w, r)
		return
	}

	h.repositoryBasedHandler(r.Method, repositoryId).ServeHTTP(w, r)
}

func SplitFirst(pth string, parts int) ([]string, bool) {
	const sep = "/"
	result := make([]string, parts)
	if strings.HasPrefix(pth, sep) {
		pth = pth[1:]
	}
	splitted := strings.Split(pth, sep)
	if len(splitted) < parts {
		return result, false
	}

	if len(splitted) == 1 && len(splitted[0]) == 0 {
		return result, false
	}

	for i := 0; i < parts-1; i++ {
		result[i] = splitted[i]
	}
	result[parts-1] = strings.Join(splitted[parts-1:], sep)
	return result, true
}
