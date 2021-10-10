package api

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	gomime "github.com/cubewise-code/go-mime"
	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/operations"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/webui"
)

// Taken from https://github.com/mytrile/nocache
var noCacheHeaders = map[string]string{
	"Expires":         time.Unix(0, 0).Format(time.RFC1123),
	"Cache-Control":   "no-cache, private, max-age=0",
	"Pragma":          "no-cache",
	"X-Accel-Expires": "0",
}

var etagHeaders = []string{
	"ETag",
	"If-Modified-Since",
	"If-Match",
	"If-None-Match",
	"If-Range",
	"If-Unmodified-Since",
}

func NewUIHandler(authService auth.Service, gatewayDomains []string) http.Handler {
	mux := http.NewServeMux()
	staticFiles, _ := fs.NewWithNamespace(webui.Webui)
	fileServer := http.FileServer(staticFiles)
	noCacheFileServer := NoCacheHandler(fileServer)
	mux.Handle("/", NewHandlerWithDefault(staticFiles, noCacheFileServer, "/", gatewayDomains))
	return mux
}

// NoCacheHandler based on github.com/zenazn/goji's NoCache
func NoCacheHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delete any ETag headers that may have been set
		for _, v := range etagHeaders {
			if r.Header.Get(v) != "" {
				r.Header.Del(v)
			}
		}

		// Set our NoCache headers
		for k, v := range noCacheHeaders {
			w.Header().Set(k, v)
		}

		handler.ServeHTTP(w, r)
	})
}

func NewHandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath string, gatewayDomains []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGatewayRequest(r) {
			// s3 signed request reaching the ui handler, return an error response instead of the default path
			o := operations.Operation{}
			err := errors.Codes[errors.ERRLakeFSWrongEndpoint]
			err.Description = fmt.Sprintf("%s (%v)", err.Description, gatewayDomains)
			o.EncodeError(w, r, err)
			return
		}
		urlPath := r.URL.Path
		if !strings.HasPrefix(urlPath, "/") {
			urlPath = "/" + urlPath
			r.URL.Path = urlPath
		}
		_, err := root.Open(path.Clean(urlPath))
		if err != nil && os.IsNotExist(err) {
			r.URL.Path = defaultPath
		}
		// consistent content-type
		contentType := gomime.TypeByExtension(filepath.Ext(r.URL.Path))
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		handler.ServeHTTP(w, r)
	})
}

func isGatewayRequest(r *http.Request) bool {
	// v4 and v2 header key are equal
	vals := r.Header.Values(sig.V4authHeaderName)
	for _, v := range vals {
		if strings.HasPrefix(v, sig.V4authHeaderPrefix) {
			return true
		}
		if sig.V2AuthHeaderRegexp.MatchString(v) {
			return true
		}
	}

	return false
}
