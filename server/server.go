package server

import "versio-index/config"

// Server is the component holding the DB instance and exposing an external API for it
type Server struct {
}

// New returns a new Server instance configured according to the supplied config
func New(conf *config.IndexerConfiguration) (*Server, error) {
	return &Server{}, nil
}
