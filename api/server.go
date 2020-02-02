package api

import (
	"context"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"

	"google.golang.org/grpc/status"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/permissions"

	"google.golang.org/grpc"

	"net"

	"github.com/treeverse/lakefs/api/service"
	"github.com/treeverse/lakefs/index"
)

type Server struct {
	meta        index.Index
	authService auth.Service
}

func NewServer(meta index.Index, authService auth.Service) *Server {
	return &Server{meta, authService}
}

var empty = &service.Empty{}

func (s *Server) auth(ctx context.Context, perm permissions.Permission, arn string) error {
	user := getUser(ctx)
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserID: user.GetId(), Permission: perm, SubjectARN: arn})
	if err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	if authResp.Error != nil {
		return status.Error(codes.PermissionDenied, authResp.Error.Error())
	}

	if !authResp.Allowed {
		return status.Error(codes.PermissionDenied, ErrAuthorizationError.Error())
	}
	return nil
}

func repoArn(repoId string) string {
	return fmt.Sprintf("arn:treeverse:repos:::%s", repoId)
}

func (s *Server) CreateRepo(ctx context.Context, req *service.CreateRepoRequest) (*service.CreateRepoResponse, error) {
	if err := s.auth(ctx, permissions.ManageRepos, repoArn("*")); err != nil {
		return nil, err
	}

	// create
	err := s.meta.CreateRepo(req.GetRepoId(), index.DefaultBranch)
	if err != nil {
		return nil, err // TODO: is this really what we want to do??
	}
	return &service.CreateRepoResponse{}, nil
}

func (s *Server) DeleteRepo(ctx context.Context, req *service.DeleteRepoRequest) (*service.DeleteRepoResponse, error) {
	if err := s.auth(ctx, permissions.ManageRepos, repoArn("*")); err != nil {
		return nil, err
	}
	// delete
	err := s.meta.DeleteRepo(req.GetRepoId())
	if err != nil {
		return nil, err // TODO: is this really what we want to do??
	}
	return &service.DeleteRepoResponse{}, nil
}

func (s *Server) ListRepos(ctx context.Context, req *service.ListReposRequest) (*service.ListReposResponse, error) {
	if err := s.auth(ctx, permissions.ReadRepo, repoArn("*")); err != nil {
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

func (s *Server) CreateBranch(ctx context.Context, req *service.CreateBranchRequest) (*service.Empty, error) {
	err := s.meta.CreateBranch(req.GetRepoId(), req.GetBranchName(), req.GetCommitId())
	return empty, err
}

func (s *Server) DeleteBranch(ctx context.Context, req *service.DeleteBranchRequest) (*service.Empty, error) {
	panic("implement me")
}

func (s *Server) ListBranches(ctx context.Context, req *service.ListBranchesRequest) (*service.ListBranchesResponse, error) {
	branches, err := s.meta.ListBranches(req.GetRepoId(), -1)
	if err != nil {
		return nil, err
	}
	resp := make([]*service.Branch, len(branches))
	for i, branch := range branches {
		resp[i] = &service.Branch{
			Name:     branch.GetName(),
			CommitId: branch.GetCommit(),
		}
	}
	return &service.ListBranchesResponse{
		Branches: resp,
	}, nil
}

func (s *Server) GetBranch(ctx context.Context, req *service.GetBranchRequest) (*service.GetBranchResponse, error) {
	branch, err := s.meta.GetBranch(req.GetRepoId(), req.GetBranchName())
	if err != nil {
		return nil, err
	}
	return &service.GetBranchResponse{
		Branch: &service.Branch{
			Name:     branch.GetName(),
			CommitId: branch.GetCommit(),
		},
	}, nil
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

func (s *Server) Commit(context.Context, *service.CommitRequest) (*service.CommitResponse, error) {
	panic("implement me")
}

func (s *Server) Diff(context.Context, *service.DiffRequest) (*service.DiffResponse, error) {
	panic("implement me")
}

func (s *Server) Checkout(context.Context, *service.CheckoutRequest) (*service.CheckoutResponse, error) {
	panic("implement me")
}

func (s *Server) Reset(context.Context, *service.ResetRequest) (*service.ResetResponse, error) {
	panic("implement me")
}

func (s *Server) Merge(context.Context, *service.MergeRequest) (*service.MergeResponse, error) {
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
	accessKeyAuthenticator := KeyAuthenticator(s.authService)
	grpcServer := grpc.NewServer(

		// pass in a list of unary server interceptors, i.e. the grpc version of middleware
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			LoggingUnaryServerInterceptor(log.WithField("service", "api")),
			AuthenticationUnaryServerInterceptor(accessKeyAuthenticator),
		)),

		// another set of middleware for streaming calls
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			LoggingStreamServerInterceptor(log.WithField("service", "api")),
			AuthenticationStreamServerInterceptor(accessKeyAuthenticator),
		)),
	)
	service.RegisterAPIServerServer(grpcServer, s)
	if err := grpcServer.Serve(lis); err != nil {
		return err
	}
	return nil
}
