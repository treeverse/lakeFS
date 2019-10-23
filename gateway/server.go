package gateway

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
	"versio-index/auth"
	authmodel "versio-index/auth/model"
	"versio-index/block"
	"versio-index/db"
	"versio-index/gateway/serde"
	"versio-index/ident"
	"versio-index/index"
	"versio-index/index/model"

	"golang.org/x/xerrors"

	"github.com/gorilla/mux"
)

const (
	RepoMatch   = "{repo:[a-z0-9]+}"
	KeyMatch    = "{key:.*}"
	BranchMatch = "{branch:[a-z0-9\\-]+}"
)

type Server struct {
	meta        index.Index
	blockStore  block.Adapter
	authService auth.Service

	server *http.Server
	router mux.Router
}

type Operation struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter

	Client      *authmodel.Client
	User        *authmodel.User
	Application *authmodel.Application
}

func (o *Operation) RequestId() string {
	var reqId string
	ctx := o.Request.Context()
	resp := ctx.Value("request_id")
	if resp == nil {
		// assign a request ID for this request
		reqId = auth.HexStringGenerator(8)
		o.Request = o.Request.WithContext(context.WithValue(ctx, "request_id", reqId))
	}
	return reqId
}

func (o *Operation) EncodeResponse(entity interface{}, statusCode int) {
	encoder := xml.NewEncoder(o.ResponseWriter)
	err := encoder.Encode(entity)
	if err != nil {
		o.ResponseWriter.WriteHeader(http.StatusInternalServerError)
		return
	} else if statusCode != http.StatusOK {
		o.ResponseWriter.WriteHeader(statusCode)
	}
}

func (o *Operation) EncodeError(code, message, region string, statusCode int) {
	o.EncodeResponse(&serde.Error{
		Code:      code,
		Message:   message,
		Region:    region,
		RequestId: o.RequestId(),
		HostId:    auth.Base64StringGenerator(56), // this is just for compatibility for now.
	}, statusCode)
}

type RepoOperation struct {
	Operation
	Repo string
}

type BranchOperation struct {
	RepoOperation
	Branch string
}

type PathOperation struct {
	BranchOperation
	Path string
}

/*

RepoOperation

	RegisterRepoPathOperation(
		pathBasedRepo.Methods(http.MethodDelete),
		authmodel.READ_BUCKET,
		"versio:repos::{repo}",
		func(op, req) {
			op.Repo
			op.Path
			op.User
		})



*/

type OperationHandler func(operation *Operation)

func (s *Server) RegisterOperation(route *mux.Route, intent authmodel.Permission_Intent, arn string, fn OperationHandler) {
	route.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// authenticate
		accessKey := req.
			s.authService.GetAPICredentials()
		// authorize
		// structure operation
		// run callback
	})
}

func NewServer(meta index.Index, blockStore block.Adapter, authService auth.Service, listenAddr, bareDomain string) *Server {
	r := mux.NewRouter()
	s := &Server{
		meta:        meta,
		blockStore:  blockStore,
		authService: authService,
		server: &http.Server{
			Handler: r,
			Addr:    listenAddr,
		},
	}

	// path based routing

	// non-bucket-specific endpoints
	serviceEndpoint := r.Host(bareDomain).Subrouter()
	serviceEndpoint.Path("/").Methods(http.MethodGet).HandlerFunc(s.ListBuckets)
	// weird Boto stuff :(
	pathBasedRepo := serviceEndpoint.Path(fmt.Sprintf("/%s", RepoMatch)).Subrouter()
	pathBasedRepo.Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	pathBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(s.ListObjects)
	pathBasedRepo.Methods(http.MethodDelete).HandlerFunc(s.DeleteBucket)
	pathBasedRepo.Methods(http.MethodHead).HandlerFunc(s.HeadBucket)
	pathBasedRepo.Methods(http.MethodPost).HandlerFunc(s.DeleteObjects)
	pathBasedRepo.Methods(http.MethodPut).HandlerFunc(s.CreateBucket)

	pathBasedRepoWithKey := pathBasedRepo.Path(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	pathBasedRepoWithKey.Methods(http.MethodDelete).HandlerFunc(s.DeleteObject)
	pathBasedRepoWithKey.Methods(http.MethodGet).HandlerFunc(s.GetObject)
	pathBasedRepoWithKey.Methods(http.MethodHead).HandlerFunc(s.HeadObject)
	pathBasedRepoWithKey.Methods(http.MethodPut).HandlerFunc(s.PutObject)

	// sub-domain based routing

	// bucket-specific actions that don't relate to a specific key
	subDomainBasedRepo := r.Host(fmt.Sprintf("%s.%s", RepoMatch, bareDomain)).Subrouter()
	subDomainBasedRepo.Methods(http.MethodPut).HandlerFunc(s.CreateBucket)
	subDomainBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(s.ListObjects)
	subDomainBasedRepo.Methods(http.MethodDelete).HandlerFunc(s.DeleteBucket)
	subDomainBasedRepo.Methods(http.MethodHead).HandlerFunc(s.HeadBucket)
	subDomainBasedRepo.Methods(http.MethodPost).HandlerFunc(s.DeleteObjects)

	// bucket-specific actions that relate to a key
	subDomainBasedRepoWithKey := subDomainBasedRepo.Path(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	subDomainBasedRepoWithKey.Methods(http.MethodDelete).HandlerFunc(s.DeleteObject)
	subDomainBasedRepoWithKey.Methods(http.MethodGet).HandlerFunc(s.GetObject)
	subDomainBasedRepoWithKey.Methods(http.MethodHead).HandlerFunc(s.HeadObject)
	subDomainBasedRepoWithKey.Methods(http.MethodPut).HandlerFunc(s.PutObject)

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
	if xerrors.Is(err, db.ErrNotFound) {
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
	if xerrors.Is(err, db.ErrNotFound) {
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
		data, err := s.blockStore.Get(block)
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
	if xerrors.Is(err, db.ErrNotFound) {
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
	err = s.blockStore.Put(data, blockAddr)
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
