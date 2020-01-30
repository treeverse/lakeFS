package main

import (
	"context"
	"fmt"
	"os"

	"github.com/treeverse/lakefs/api/service"
	"google.golang.org/grpc"
)

type creds struct {
	accessKey    string
	accessSecret string
}

func (c *creds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"access_key": c.accessKey, "access_secret": c.accessSecret}, nil
}

func (c *creds) RequireTransportSecurity() bool {
	return false
}

func getCreds() *creds {
	return &creds{
		accessKey:    "AKIAJKLO4PDKEBQUDHYQ",
		accessSecret: "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",
	}
}

func getClient() (service.APIServerClient, error) {
	conn, err := grpc.Dial(
		"localhost:8001",
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(getCreds()),
	)
	if err != nil {
		return nil, err
	}

	clt := service.NewAPIServerClient(conn)
	return clt, nil
}

func getIdent() *service.RequestIdentity {
	return &service.RequestIdentity{
		AccessKeyId:     "AKIAJKLO4PDKEBQUDHYQ",
		AccessSecretKey: "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",
	}
}

func createRepo(name string) {
	c, err := getClient()
	if err != nil {
		panic(err)
	}
	_, err = c.CreateRepo(context.Background(), &service.CreateRepoRequest{
		Ident:  getIdent(),
		RepoId: name,
	})
	if err != nil {
		panic(err)
	}
}

func listRepos() {
	c, err := getClient()
	if err != nil {
		panic(err)
	}
	resp, err := c.ListRepos(context.Background(), &service.ListReposRequest{
		Ident: getIdent(),
	})
	if err != nil {
		panic(err)
	}
	for _, repo := range resp.GetRepos() {
		fmt.Printf("%s\n", repo)
	}
}

func main() {
	switch os.Args[1] {
	case "list":
		listRepos()
	case "create":
		createRepo(os.Args[2])
	}
}
