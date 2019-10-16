package gateway

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	"versio-index/gateway/serde"
	"versio-index/index"
	"versio-index/index/errors"
	"versio-index/index/model"

	"golang.org/x/xerrors"

	"github.com/gorilla/mux"
)

const (
	RepoPrefixPattern = "{repo:[a-z0-9]+}"
)

type Server struct {
	meta   index.Index
	server *http.Server
	router mux.Router
}

func NewServer(meta index.Index, listenAddr, bareDomain string) *Server {
	r := mux.NewRouter()
	s := &Server{
		meta: meta,
		server: &http.Server{
			Handler: r,
			Addr:    listenAddr,
		},
	}

	repoSubDomain := fmt.Sprintf("%s.%s", RepoPrefixPattern, bareDomain)

	nakedRouter := r.Host(bareDomain).Subrouter()
	repoRouter := r.Host(repoSubDomain).Subrouter()

	// non-bucket-specific endpoints
	nakedRouter.Path("/").Methods(http.MethodGet).HandlerFunc(s.ListBuckets)
	// weird Boto stuff :(
	nakedRouter.Path(fmt.Sprintf("/%s", RepoPrefixPattern)).Methods(http.MethodPut).HandlerFunc(s.CreateBucket)

	// bucket-specific actions that don't relate to a specific key
	repoRouter.Path("/").Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	repoRouter.Path("/").Methods(http.MethodGet).HandlerFunc(s.ListObjects)
	repoRouter.Path("/").Methods(http.MethodDelete).HandlerFunc(s.DeleteBucket)
	repoRouter.Path("/").Methods(http.MethodHead).HandlerFunc(s.HeadBucket)
	repoRouter.Path("/").Methods(http.MethodPost).HandlerFunc(s.DeleteObjects)

	// bucket-specific actions that relate to a key
	repoRouter.Path("/{branch}/{key}").Methods(http.MethodDelete).HandlerFunc(s.DeleteObject)
	repoRouter.Path("/{branch}/{key}").Methods(http.MethodGet).HandlerFunc(s.GetObject)
	repoRouter.Path("/{branch}/{key}").Methods(http.MethodHead).HandlerFunc(s.GetObject)
	repoRouter.Path("/{branch}/{key}").Methods(http.MethodPut).HandlerFunc(s.PutObject)

	return s
}

func (s *Server) Listen() error {
	return s.server.ListenAndServe()
}

func getRepo(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["repo"]
}

func getKey(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["key"]
}

func getBranch(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["branch"]
}

func (s *Server) DeleteBucket(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) CreateBucket(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	repoId := getRepo(req)
	err := s.meta.CreateRepo(scope.Client.GetId(), repoId, index.DefaultBranch)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	res.WriteHeader(http.StatusCreated)
}

func (s *Server) HeadBucket(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	repoId := getRepo(req)
	_, err := s.meta.GetRepo(scope.Client.GetId(), repoId)
	if xerrors.Is(err, errors.ErrNotFound) {
		res.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) ListBuckets(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	repos, err := s.meta.ListRepos(scope.Client.GetId())
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	// assemble response
	buckets := make([]serde.Bucket, len(repos))
	for i, repo := range repos {
		buckets[i] = serde.Bucket{
			CreationDate: serde.Timestamp(repo.GetCreationDate()),
			Name:         repo.GetRepoId(),
		}
	}

	err = xml.NewEncoder(res).Encode(serde.ListBucketsOutput{
		Buckets: serde.Buckets{Bucket: buckets},
		Owner: serde.Owner{
			DisplayName: scope.Client.GetName(),
			ID:          scope.Client.GetId(),
		},
	})
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

}

func (s *Server) ListObjects(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) DeleteObject(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) DeleteObjects(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) GetObject(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) HeadObject(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) PutObject(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	repoId := getRepo(req)
	branch := getBranch(req)
	key := getKey(req)
	if strings.EqualFold(req.Header.Get("Expect"), "100-Continue") {
		res.WriteHeader(http.StatusContinue) // this is to ensure we don't block uploads
	}
	// handle the upload itself
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	// write to adapter
	blocks := make([]string, 0)

	// write metadata
	err = s.meta.WriteObject(scope.Client.GetId(), repoId, branch, key, &model.Object{
		Blob: &model.Blob{
			Blocks: blocks,
		},
		Metadata:  nil,
		Timestamp: time.Now().Unix(),
		Size:      int64(len(data)),
	})

	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	res.WriteHeader(http.StatusCreated)
}

//CreateMultipartUpload
//
//CompleteMultipartUpload
//
//AbortMultipartUpload
//
//ListMultipartUploads
//
//UploadPart
