package gateway

import (
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/httputil"
)

type Handler struct {
	BareDomain         string
	sc                 *ServerContext
	operationID        string
	NotFoundHandler    http.Handler
	ServerErrorHandler http.Handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pprof endpoints
	handler := h.servePathBased(r)
	if handler == nil {
		handler = h.serveVirtualHost(r)
	}
	if handler == nil {
		handler = h.NotFoundHandler
	}
	start := time.Now()
	mrw := httputil.NewMetricResponseWriter(w)
	handler.ServeHTTP(mrw, r)
	requestHistograms.WithLabelValues(h.operationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
}

func (h *Handler) servePathBased(r *http.Request) http.Handler {
	host := httputil.HostOnly(r.Host)
	if !strings.EqualFold(host, httputil.HostOnly(h.BareDomain)) {
		return nil // maybe it's a virtual host, but def not a path based request because the host is wrong
	}

	if parts, ok := SplitFirst(r.URL.Path, 3); ok {
		repository := parts[0]
		ref := parts[1]
		key := parts[2]
		if err := catalog.Validate(catalog.ValidateFields{
			{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
			{Name: "reference", IsValid: catalog.ValidateReference(ref)},
			{Name: "path", IsValid: catalog.ValidatePath(key)},
		}); err != nil {
			return h.NotFoundHandler
		}

		return h.pathBasedHandler(r.Method, repository, ref, key)
	}

	// Paths for repository and ref only (none exist)
	if _, ok := SplitFirst(r.URL.Path, 2); ok {
		return h.NotFoundHandler
	}

	if parts, ok := SplitFirst(r.URL.Path, 1); ok {
		// Paths for bare repository
		repository := parts[0]
		if !catalog.IsValidRepositoryName(repository) {
			return h.NotFoundHandler
		}

		return h.repositoryBasedHandler(r.Method, repository)
	}
	// no repository given
	if r.Method == http.MethodGet {
		h.operationID = "list_buckets"
		return OperationHandler(h.sc, &operations.ListBuckets{})
	}
	h.operationID = "not_found_operation"
	return h.NotFoundHandler
}

func (h *Handler) serveVirtualHost(r *http.Request) http.Handler {
	// is it a virtual host?
	host := httputil.HostOnly(r.Host)
	ourHost := httputil.HostOnly(h.BareDomain)
	if !strings.HasSuffix(host, ourHost) {
		return nil
	}

	// remove bare domain suffix
	repository := strings.TrimSuffix(host, "."+ourHost)
	if !catalog.IsValidRepositoryName(repository) {
		return h.NotFoundHandler
	}

	// Paths that have both a repository, a refId and a path
	if parts, ok := SplitFirst(r.URL.Path, 2); ok {
		// validate ref, key
		ref := parts[0]
		key := parts[1]
		if err := catalog.Validate(catalog.ValidateFields{
			{Name: "reference", IsValid: catalog.ValidateReference(ref)},
			{Name: "path", IsValid: catalog.ValidatePath(key)},
		}); err != nil {
			return h.NotFoundHandler
		}
		return h.pathBasedHandler(r.Method, repository, ref, key)
	}

	// Paths that only have a repository and a refId (always 404)
	if _, ok := SplitFirst(r.URL.Path, 1); ok {
		return h.NotFoundHandler
	}

	return h.repositoryBasedHandler(r.Method, repository)
}

func (h *Handler) pathBasedHandler(method, repository, ref, path string) http.Handler {
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
		h.operationID = "not_found_operation"
		return h.NotFoundHandler
	}
	h.operationID = reflect.TypeOf(handler).Elem().Name()
	return PathOperationHandler(h.sc, repository, ref, path, handler)
}

func (h *Handler) repositoryBasedHandler(method, repository string) http.Handler {
	var handler operations.RepoOperationHandler
	switch method {
	case http.MethodDelete, http.MethodPut:
		h.operationID = "unsupported_operation"
		return unsupportedOperationHandler()
	case http.MethodHead:
		handler = &operations.HeadBucket{}
	case http.MethodPost:
		handler = &operations.DeleteObjects{}
	case http.MethodGet:
		handler = &operations.ListObjects{}
	default:
		h.operationID = "not_found_operation"
		return h.NotFoundHandler
	}
	h.operationID = reflect.TypeOf(handler).Elem().Name()

	return RepoOperationHandler(h.sc, repository, handler)
}

func SplitFirst(pth string, parts int) ([]string, bool) {
	pth = strings.TrimPrefix(pth, path.Separator)
	pathParts := strings.SplitN(pth, path.Separator, parts)
	if len(pathParts) < parts {
		return []string{}, false
	}
	if len(pathParts) == 1 && len(pathParts[0]) == 0 {
		return []string{}, false
	}
	return pathParts, true
}
