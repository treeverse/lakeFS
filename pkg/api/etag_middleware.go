package api

import (
	// MD5 required for ETag computation.
	//nolint:gosec
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// EtagMiddleware returns a new Etag middleware handler.
// It designs to work on embedded FS, where the content doesn't change.
// It calculates the Etag for each file on startup and serves it on each request.
func EtagMiddleware(root fs.FS, next http.Handler) http.Handler {
	etags, err := scanFSEtags(root)
	if err != nil {
		panic(err)
	}
	return &etagHandler{
		root:  root,
		next:  next,
		etags: etags,
	}
}

type etagHandler struct {
	root  fs.FS
	next  http.Handler
	etags map[string]string
}

func (e *etagHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
	}
	upath = path.Clean(upath)
	if strings.HasSuffix(upath, "/") {
		upath += "index.html"
	}
	etag, ok := e.etags[upath]
	if ok {
		w.Header().Set("Etag", "\""+etag+"\"")
	}
	e.next.ServeHTTP(w, r)
}

func scanFSEtags(fSys fs.FS) (map[string]string, error) {
	etags := make(map[string]string)
	err := fs.WalkDir(fSys, ".", func(fpath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		f, err := fSys.Open(fpath)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		// MD5 required for ETag computation.
		h := md5.New() //nolint:gosec
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		hashValue := hex.EncodeToString(h.Sum(nil))
		etags["/"+fpath] = hashValue
		return nil
	})
	if err != nil {
		return nil, err
	}
	return etags, nil
}
