package api

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/treeverse/lakefs/auth"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/permissions"
	"golang.org/x/xerrors"

	"google.golang.org/grpc"

	"net"

	"github.com/treeverse/lakefs/api/service"
	"github.com/treeverse/lakefs/index"
)

var (
	ErrAuthenticationError = xerrors.New("authentication failed")
	ErrAuthorizationError  = xerrors.New("you are not permitted to perform this action")
)

type Server struct {
	meta        index.Index
	authService auth.Service
}

type IdentifiedRequest interface {
	GetIdent() *service.RequestIdentity
}

func (s *Server) authenticate(req IdentifiedRequest) (*authmodel.User, error) {
	ident := req.GetIdent()
	credentials, err := s.authService.GetAPICredentials(ident.GetAccessKeyId())
	if err != nil {
		return nil, ErrAuthenticationError
	}
	if strings.EqualFold(credentials.GetAccessSecretKey(), ident.GetAccessSecretKey()) {
		user, err := s.authService.GetUser(credentials.GetEntityId())
		if err != nil {
			return nil, ErrAuthenticationError
		}
		return user, nil
	}
	return nil, ErrAuthenticationError
}

func NewServer(meta index.Index, authService auth.Service) *Server {
	return &Server{meta, authService}
}

func (s *Server) auth(request IdentifiedRequest, perm permissions.Permission, arn string) (*authmodel.User, error) {
	user, err := s.authenticate(request)
	if err != nil {
		return nil, err
	}
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserID: user.GetId(), Permission: perm, SubjectARN: arn})
	if err != nil {
		return nil, err
	}

	if authResp.Error != nil {
		return nil, authResp.Error
	}

	if !authResp.Allowed {
		return nil, ErrAuthorizationError
	}
	return user, nil
}

func repoArn(repoId string) string {
	return fmt.Sprintf("arn:treeverse:repos:::%s", repoId)
}

func (s *Server) CreateRepo(ctx context.Context, req *service.CreateRepoRequest) (*service.CreateRepoResponse, error) {
	_, err := s.auth(req, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return nil, err
	}

	// create
	err = s.meta.CreateRepo(req.GetRepoId(), index.DefaultBranch)
	if err != nil {
		return nil, err // TODO: is this really what we want to do??
	}
	return &service.CreateRepoResponse{}, nil
}

func (s *Server) DeleteRepo(ctx context.Context, req *service.DeleteRepoRequest) (*service.DeleteRepoResponse, error) {
	_, err := s.auth(req, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return nil, err
	}
	// delete
	err = s.meta.DeleteRepo(req.GetRepoId())
	if err != nil {
		return nil, err // TODO: is this really what we want to do??
	}
	return &service.DeleteRepoResponse{}, nil
}

func (s *Server) ListRepos(ctx context.Context, req *service.ListReposRequest) (*service.ListReposResponse, error) {
	_, err := s.auth(req, permissions.ReadRepo, repoArn("*"))
	if err != nil {
		return nil, err
	}

	repos, err := s.meta.ListRepos()
	if err != nil {
		return nil, err // TODO: is this really what we want to do?
	}

	names := make([]string, len(repos))
	for i, repo := range repos {
		names[i] = repo.GetRepoId()
	}

	return &service.ListReposResponse{
		Repos: names,
	}, nil
}

func (s *Server) GetRepo(ctx context.Context, req *service.GetRepoRequest) (*service.GetRepoResponse, error) {
	panic("implement me")
}

func (s *Server) CreateBranch(ctx context.Context, req *service.CreateBranchRequest) (*service.CreateBranchResponse, error) {
	panic("implement me")
}

func (s *Server) DeleteBranch(ctx context.Context, req *service.DeleteBranchRequest) (*service.DeleteBranchResponse, error) {
	panic("implement me")
}

func (s *Server) ListBranches(ctx context.Context, req *service.ListBranchesRequest) (*service.ListBranchesResponse, error) {
	panic("implement me")
}

func (s *Server) GetBranch(ctx context.Context, req *service.GetBranchRequest) (*service.GetBranchResponse, error) {
	panic("implement me")
}

func (s *Server) ListEntries(ctx context.Context, req *service.ListEntriesRequest) (*service.ListEntriesResponse, error) {
	panic("implement me")
}

func (s *Server) GetEntry(ctx context.Context, req *service.GetEntryRequest) (*service.GetEntryResponse, error) {
	panic("implement me")
}

func (s *Server) PutEntry(ctx context.Context, req *service.PutEntryRequest) (*service.PutEntryResponse, error) {
	panic("implement me")
}

func (s *Server) DeleteEntry(ctx context.Context, req *service.DeleteEntryRequest) (*service.DeleteEntryResponse, error) {
	panic("implement me")
}

func (s *Server) Download(ctx context.Context, req *service.DownloadRequest) (*service.DownloadResponse, error) {
	panic("implement me")
}

func (s *Server) CreateUser(ctx context.Context, req *service.CreateUserRequest) (*service.CreateUserResponse, error) {
	panic("implement me")
}

func (s *Server) CreateKeyPair(ctx context.Context, req *service.CreateKeyPairRequest) (*service.CreateKeyPairResponse, error) {
	panic("implement me")
}

func (s *Server) AssignRoleToUser(ctx context.Context, req *service.AssignRoleToUserRequest) (*service.AssignRoleToUserResponse, error) {
	panic("implement me")
}

func (s *Server) ListUsers(ctx context.Context, req *service.ListUsersRequest) (*service.ListUsersResponse, error) {
	panic("implement me")
}

func (s *Server) DeleteUser(ctx context.Context, req *service.DeleteUserRequest) (*service.DeleteUserResponse, error) {
	panic("implement me")
}

func (s *Server) CreateGroup(ctx context.Context, req *service.CreateGroupRequest) (*service.CreateGroupResponse, error) {
	panic("implement me")
}

func (s *Server) AssignRoleToGroup(ctx context.Context, req *service.AssignRoleToGroupRequest) (*service.AssignRoleToGroupResponse, error) {
	panic("implement me")
}

func (s *Server) ListGroups(ctx context.Context, req *service.ListGroupsRequest) (*service.ListGroupsResponse, error) {
	panic("implement me")
}

func (s *Server) AssignUserToGroup(ctx context.Context, req *service.AssignUserToGroupRequest) (*service.AssignUserToGroupResponse, error) {
	panic("implement me")
}

func (s *Server) DeleteGroup(ctx context.Context, req *service.DeleteGroupRequest) (*service.DeleteGroupResponse, error) {
	panic("implement me")
}

func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// authenticate first
		md, hasMd := metadata.FromIncomingContext(ctx)
		x := md.Get("access_key")
		fmt.Printf("GOT METADATA: %v (%+v) %s", hasMd, md, x)

		resp, err = handler(ctx, req)
		return
	}))
	service.RegisterAPIServerServer(grpcServer, s)
	if err := grpcServer.Serve(lis); err != nil {
		return err
	}
	return nil
}
