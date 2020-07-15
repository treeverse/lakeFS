package gateway

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
)

type Handler struct {
	BareDomain string
	ctx        *ServerContext

	NotFoundHandler    http.Handler
	ServerErrorHandler http.Handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pprof endpoints
	var handler http.Handler
	if handler == nil {
		handler = h.servePathBased(r)
	}
	if handler == nil {
		handler = h.serveVirtualHost(r)
	}
	if handler == nil {
		handler = h.NotFoundHandler
	}

	handler.ServeHTTP(w, r)
}

func (h *Handler) servePathBased(r *http.Request) http.Handler {
	host := httputil.HostOnly(r.Host)

	if !strings.EqualFold(host, httputil.HostOnly(h.BareDomain)) {
		return nil // maybe it's a virtual host, but def not a path based request because the host is wrong
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
			return h.NotFoundHandler
		}

		return h.pathBasedHandler(r.Method, repositoryId, ref, key)
	}

	// Paths for repository and ref only (none exist)
	if _, ok := SplitFirst(r.URL.Path, 2); ok {
		return h.NotFoundHandler
	}

	if parts, ok := SplitFirst(r.URL.Path, 1); ok {
		// Paths for bare repository
		repositoryId := parts[0]
		if err := index.ValidateAll(
			index.ValidateRepoId(repositoryId),
		); err != nil {
			return h.NotFoundHandler
		}

		return h.repositoryBasedHandler(r.Method, repositoryId)
	}

	// no repository given
	switch r.Method {
	case http.MethodGet:
		return OperationHandler(h.ctx, &operations.ListBuckets{})
	}

	return h.NotFoundHandler
}

func (h *Handler) serveVirtualHost(r *http.Request) http.Handler {
	// is it a virtual host?
	host := httputil.HostOnly(r.Host)

	if !strings.HasSuffix(host, httputil.HostOnly(h.BareDomain)) {
		return nil
	}

	// remove bare domain suffix
	repositoryId := strings.TrimSuffix(host, fmt.Sprintf(".%s", httputil.HostOnly(h.BareDomain)))

	if err := index.ValidateRepoId(repositoryId); err != nil {
		return h.NotFoundHandler
	}

	// Paths that have both a repository, a refId and a path
	if parts, ok := SplitFirst(r.URL.Path, 2); ok {
		// validate ref, key
		ref := parts[0]
		key := parts[1]
		if err := index.ValidateAll(index.ValidateRef(ref), index.ValidatePath(key)); err != nil {
			return h.NotFoundHandler
		}
		return h.pathBasedHandler(r.Method, repositoryId, ref, key)
	}

	// Paths that only have a repository and a refId (always 404)
	if _, ok := SplitFirst(r.URL.Path, 1); ok {
		return h.NotFoundHandler
	}

	return h.repositoryBasedHandler(r.Method, repositoryId)
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
