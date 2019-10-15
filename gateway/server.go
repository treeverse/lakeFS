package gateway

import (
	"net/http"
	"versio-index/index"

	"github.com/gorilla/mux"
)

type Server struct {
	meta   index.Index
	server http.Server
	router mux.Router
}
