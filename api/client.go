package api

import (
	"github.com/treeverse/lakefs/api/service"
	"google.golang.org/grpc"
)

func NewClient(serverAddr, accessKeyId, accessKeySecret string) (service.APIServerClient, error) {
	conn, err := grpc.Dial(
		serverAddr,
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(AccessKeyCredentials(
			accessKeyId,
			accessKeySecret,
		)),
	)
	if err != nil {
		return nil, err
	}
	clt := service.NewAPIServerClient(conn)
	return clt, nil
}
