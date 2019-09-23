package server

import (
	"context"
	"fmt"
	"net"
	"versio-index/config"
	"versio-index/model"

	"google.golang.org/grpc"
)

// Server is the component holding the DB instance and exposing an external API for it
type Server struct {
}

func (s *Server) Read(ctx context.Context, req *model.ReadRequest) (*model.ReadResponse, error) {
	return &model.ReadResponse{
		Blob: &model.Blob{},
	}, nil
}
func (s *Server) Write(ctx context.Context, req *model.WriteRequest) (*model.WriteResponse, error) {
	return &model.WriteResponse{}, nil
}
func (s *Server) Delete(ctx context.Context, req *model.DeleteRequest) (*model.DeleteResponse, error) {
	return &model.DeleteResponse{}, nil
}
func (s *Server) List(ctx context.Context, req *model.ListRequest) (*model.ListResponse, error) {
	return &model.ListResponse{}, nil

}

// New returns a new Server instance configured according to the supplied config
func New(conf *config.IndexerConfiguration) (*Server, error) {
	s := &Server{}
	soc, err := net.Listen("tcp", fmt.Sprintf(":%d", 8080))
	if err != nil {
		return nil, err
	}
	serv := grpc.NewServer()
	model.RegisterIndexServer(serv, s)
	serv.Serve(soc)
	return s, nil
}
