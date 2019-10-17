package gateway

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
	"versio-index/block"
	"versio-index/gateway/serde"
	"versio-index/ident"
	"versio-index/index"
	"versio-index/index/errors"
	"versio-index/index/model"

	"golang.org/x/xerrors"

	"github.com/gorilla/mux"
)

type Server struct {
	meta index.Index
	sink block.Adapter

	server *http.Server
	router mux.Router
}

func NewServer(meta index.Index, sink block.Adapter, listenAddr, bareDomain string) *Server {
	r := mux.NewRouter()
	s := &Server{
		meta: meta,
		sink: sink,
		server: &http.Server{
			Handler: r,
			Addr:    listenAddr,
		},
	}

	repoSubDomain := fmt.Sprintf("{repo:[a-z0-9]+}.%s", bareDomain)

	nakedRouter := r.Host(bareDomain).Subrouter()
	repoRouter := r.Host(repoSubDomain).Subrouter()

	// non-bucket-specific endpoints
	nakedRouter.Path("/").Methods(http.MethodGet).HandlerFunc(s.ListBuckets)
	// weird Boto stuff :(
	nakedRouter.Path("/{repo:[a-z0-9]+}").Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	nakedRouter.
		Path("/{repo:[a-z0-9]+}").
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(s.ListObjects)
	nakedRouter.Path("/{repo:[a-z0-9]+}").Methods(http.MethodDelete).HandlerFunc(s.DeleteBucket)
	nakedRouter.Path("/{repo:[a-z0-9]+}").Methods(http.MethodHead).HandlerFunc(s.HeadBucket)
	nakedRouter.Path("/{repo:[a-z0-9]+}").Methods(http.MethodPost).HandlerFunc(s.DeleteObjects)
	nakedRouter.Path("/{repo:[a-z0-9]+}").Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	nakedRouter.Path("/{repo:[a-z0-9]+}/{branch}/{key:.*}").Methods(http.MethodDelete).HandlerFunc(s.DeleteObject)
	nakedRouter.Path("/{repo:[a-z0-9]+}/{branch}/{key:.*}").Methods(http.MethodGet).HandlerFunc(s.GetObject)
	nakedRouter.Path("/{repo:[a-z0-9]+}/{branch}/{key:.*}").Methods(http.MethodHead).HandlerFunc(s.HeadObject)
	nakedRouter.Path("/{repo:[a-z0-9]+}/{branch}/{key:.*}").Methods(http.MethodPut).HandlerFunc(s.PutObject)

	// bucket-specific actions that don't relate to a specific key
	repoRouter.Path("/").Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	repoRouter.
		Path("/").
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(s.ListObjects)
	repoRouter.Path("/").Methods(http.MethodDelete).HandlerFunc(s.DeleteBucket)
	repoRouter.Path("/").Methods(http.MethodHead).HandlerFunc(s.HeadBucket)
	repoRouter.Path("/").Methods(http.MethodPost).HandlerFunc(s.DeleteObjects)

	// bucket-specific actions that relate to a key
	repoRouter.Path("/{branch}/{key:.*}").Methods(http.MethodDelete).HandlerFunc(s.DeleteObject)
	repoRouter.Path("/{branch}/{key:.*}").Methods(http.MethodGet).HandlerFunc(s.GetObject)
	repoRouter.Path("/{branch}/{key:.*}").Methods(http.MethodHead).HandlerFunc(s.HeadObject)
	repoRouter.Path("/{branch}/{key:.*}").Methods(http.MethodPut).HandlerFunc(s.PutObject)

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
	//scope := getScope(req)
	//repoId := getRepo(req)
	// get branch and path

	//s.meta.ListObjects(scope.Client.GetId(), repoId, "master", "/")
}

func (s *Server) DeleteObject(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) DeleteObjects(res http.ResponseWriter, req *http.Request) {

}

func (s *Server) GetObject(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	repoId := getRepo(req)
	branch := getBranch(req)
	key := getKey(req)

	obj, err := s.meta.ReadObject(scope.Client.GetId(), repoId, branch, key)
	if xerrors.Is(err, errors.ErrNotFound) {
		res.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	blocks := obj.GetBlob().GetBlocks()
	buf := bytes.NewBuffer(nil)
	for _, block := range blocks {
		data, err := s.sink.Get(block)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			return
		}
		buf.Write(data)
	}

	res.Header().Set("Last-Modified", serde.Timestamp(obj.GetTimestamp()))
	res.Header().Set("Etag", ident.Hash(obj))
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	_, err = io.Copy(res, buf)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) HeadObject(res http.ResponseWriter, req *http.Request) {
	scope := getScope(req)
	clientId := scope.Client.GetId()
	repoId := getRepo(req)
	branch := getBranch(req)
	key := getKey(req)

	obj, err := s.meta.ReadObject(clientId, repoId, branch, key)
	if xerrors.Is(err, errors.ErrNotFound) {
		res.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	res.Header().Set("Content-Length", fmt.Sprintf("%d", obj.GetSize()))
	res.Header().Set("Last-Modified", serde.Timestamp(obj.GetTimestamp()))
	res.Header().Set("Etag", ident.Hash(obj))
}

func (s *Server) PutObject(res http.ResponseWriter, req *http.Request) {
	fmt.Printf("GOT REQUEST\n")
	scope := getScope(req)
	repoId := getRepo(req)
	branch := getBranch(req)
	key := getKey(req)

	// handle the upload itself
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	// write to adapter
	blocks := make([]string, 0)
	blockAddr := ident.Bytes(data)
	err = s.sink.Put(data, blockAddr)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	blocks = append(blocks, blockAddr)

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
