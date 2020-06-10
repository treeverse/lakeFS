package api

import (
	"context"
	"io"
	"net/url"
	"path"

	"github.com/treeverse/lakefs/api/gen/client/auth"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/index/errors"
)

type AuthClient interface {
	GetCurrentUser(ctx context.Context) (*models.User, error)
	GetUser(ctx context.Context, userId string) (*models.User, error)
	ListUsers(ctx context.Context, after string, amount int) ([]*models.User, *models.Pagination, error)
	DeleteUser(ctx context.Context, userId string) error
	CreateUser(ctx context.Context, userId string) (*models.User, error)
	GetGroup(ctx context.Context, groupId string) (*models.Group, error)
	ListGroups(ctx context.Context, after string, amount int) ([]*models.Group, *models.Pagination, error)
	CreateGroup(ctx context.Context, groupId string) (*models.Group, error)
	DeleteGroup(ctx context.Context, groupId string) error
	ListPolicies(ctx context.Context, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	CreatePolicy(ctx context.Context, creation *models.PolicyCreation) (*models.Policy, error)
	GetPolicy(ctx context.Context, policyId string) (*models.Policy, error)
	DeletePolicy(ctx context.Context, policyId string) error
	ListGroupMembers(ctx context.Context, groupId string, after string, amount int) ([]*models.User, *models.Pagination, error)
	AddGroupMembership(ctx context.Context, groupId, userId string) error
	DeleteGroupMembership(ctx context.Context, groupId, userId string) error
	ListUserCredentials(ctx context.Context, userId string, after string, amount int) ([]*models.Credentials, *models.Pagination, error)
	CreateCredentials(ctx context.Context, userId string) (*models.CredentialsWithSecret, error)
	DeleteCredentials(ctx context.Context, userId, accessKeyId string) error
	GetCredentials(ctx context.Context, userId, accessKeyId string) (*models.Credentials, error)
	ListUserGroups(ctx context.Context, userId string, after string, amount int) ([]*models.Group, *models.Pagination, error)
	ListUserPolicies(ctx context.Context, userId string, effective bool, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	AttachPolicyToUser(ctx context.Context, userId, policyId string) error
	DetachPolicyFromUser(ctx context.Context, userId, policyId string) error
	ListGroupPolicies(ctx context.Context, groupId string, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	AttachPolicyToGroup(ctx context.Context, groupId, policyId string) error
	DetachPolicyFromGroup(ctx context.Context, groupId, policyId string) error
}

type RepositoryClient interface {
	ListRepositories(ctx context.Context, after string, amount int) ([]*models.Repository, *models.Pagination, error)
	GetRepository(ctx context.Context, repoId string) (*models.Repository, error)
	CreateRepository(ctx context.Context, repository *models.RepositoryCreation) error
	DeleteRepository(ctx context.Context, repoId string) error

	ListBranches(ctx context.Context, repoId string, from string, amount int) ([]*models.Ref, *models.Pagination, error)
	GetBranch(ctx context.Context, repoId, branchId string) (*models.Ref, error)
	CreateBranch(ctx context.Context, repoId string, branch *models.BranchCreation) (*models.Ref, error)
	DeleteBranch(ctx context.Context, repoId, branchId string) error
	RevertBranch(ctx context.Context, repoId, branchId string, revertProps *models.RevertCreation) error

	Commit(ctx context.Context, repoId, branchId, message string, metadata map[string]string) (*models.Commit, error)
	GetCommit(ctx context.Context, repoId, commitId string) (*models.Commit, error)
	GetCommitLog(ctx context.Context, repoId, branchId, after string, amount int) ([]*models.Commit, *models.Pagination, error)

	StatObject(ctx context.Context, repoId, ref, path string, readUncommitted bool) (*models.ObjectStats, error)
	ListObjects(ctx context.Context, repoId, ref, tree, from string, amount int, readUncommitted bool) ([]*models.ObjectStats, *models.Pagination, error)
	GetObject(ctx context.Context, repoId, ref, path string, readUncommitted bool, w io.Writer) (*objects.GetObjectOK, error)
	UploadObject(ctx context.Context, repoId, branchId, path string, r io.Reader) (*models.ObjectStats, error)
	DeleteObject(ctx context.Context, repoId, branchId, path string) error

	DiffRefs(ctx context.Context, repoId, leftRef, rightRef string) ([]*models.Diff, error)
	Merge(ctx context.Context, repoId, leftRef, rightRef string) ([]*models.MergeResult, error)

	DiffBranch(ctx context.Context, repoId, branch string) ([]*models.Diff, error)
}

type Client interface {
	AuthClient
	RepositoryClient
}

type client struct {
	remote *genclient.Lakefs
	auth   runtime.ClientAuthInfoWriter
}

func (c *client) GetCurrentUser(ctx context.Context) (*models.User, error) {
	resp, err := c.remote.Auth.GetCurrentUser(&auth.GetCurrentUserParams{
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload().User, nil
}

func (c *client) GetUser(ctx context.Context, userId string) (*models.User, error) {
	resp, err := c.remote.Auth.GetUser(&auth.GetUserParams{
		UserID:  userId,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListUsers(ctx context.Context, after string, amount int) ([]*models.User, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUsers(&auth.ListUsersParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) DeleteUser(ctx context.Context, userId string) error {
	_, err := c.remote.Auth.DeleteUser(&auth.DeleteUserParams{
		UserID:  userId,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) CreateUser(ctx context.Context, userId string) (*models.User, error) {
	resp, err := c.remote.Auth.CreateUser(&auth.CreateUserParams{
		User: &models.UserCreation{
			ID: userId,
		},
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) GetGroup(ctx context.Context, groupId string) (*models.Group, error) {
	resp, err := c.remote.Auth.GetGroup(&auth.GetGroupParams{
		GroupID: groupId,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListGroups(ctx context.Context, after string, amount int) ([]*models.Group, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListGroups(&auth.ListGroupsParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreateGroup(ctx context.Context, groupId string) (*models.Group, error) {
	resp, err := c.remote.Auth.CreateGroup(&auth.CreateGroupParams{
		Group: &models.GroupCreation{
			ID: groupId,
		},
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) DeleteGroup(ctx context.Context, groupId string) error {
	_, err := c.remote.Auth.DeleteGroup(&auth.DeleteGroupParams{
		GroupID: groupId,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) ListPolicies(ctx context.Context, after string, amount int) ([]*models.Policy, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListPolicies(&auth.ListPoliciesParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreatePolicy(ctx context.Context, creation *models.PolicyCreation) (*models.Policy, error) {
	resp, err := c.remote.Auth.CreatePolicy(&auth.CreatePolicyParams{
		Policy:  creation,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) GetPolicy(ctx context.Context, policyId string) (*models.Policy, error) {
	resp, err := c.remote.Auth.GetPolicy(&auth.GetPolicyParams{
		PolicyID: policyId,
		Context:  ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeletePolicy(ctx context.Context, policyId string) error {
	_, err := c.remote.Auth.DeletePolicy(&auth.DeletePolicyParams{
		PolicyID: policyId,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) ListGroupMembers(ctx context.Context, groupId string, after string, amount int) ([]*models.User, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListGroupMembers(&auth.ListGroupMembersParams{
		Amount:  swag.Int64(int64(amount)),
		GroupID: groupId,
		After:   swag.String(after),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AddGroupMembership(ctx context.Context, groupId, userId string) error {
	_, err := c.remote.Auth.AddGroupMembership(&auth.AddGroupMembershipParams{
		GroupID: groupId,
		UserID:  userId,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteGroupMembership(ctx context.Context, groupId, userId string) error {
	_, err := c.remote.Auth.DeleteGroupMembership(&auth.DeleteGroupMembershipParams{
		GroupID: groupId,
		UserID:  userId,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) ListUserCredentials(ctx context.Context, userId string, after string, amount int) ([]*models.Credentials, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserCredentials(&auth.ListUserCredentialsParams{
		Amount:     swag.Int64(int64(amount)),
		After:      swag.String(after),
		UserID:     userId,
		Context:    ctx,
		HTTPClient: nil,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreateCredentials(ctx context.Context, userId string) (*models.CredentialsWithSecret, error) {
	resp, err := c.remote.Auth.CreateCredentials(&auth.CreateCredentialsParams{
		UserID:     userId,
		Context:    ctx,
		HTTPClient: nil,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) DeleteCredentials(ctx context.Context, userId, accessKeyId string) error {
	_, err := c.remote.Auth.DeleteCredentials(&auth.DeleteCredentialsParams{
		AccessKeyID: accessKeyId,
		UserID:      userId,
		Context:     ctx,
	}, c.auth)
	return err
}

func (c *client) GetCredentials(ctx context.Context, userId, accessKeyId string) (*models.Credentials, error) {
	resp, err := c.remote.Auth.GetCredentials(&auth.GetCredentialsParams{
		AccessKeyID: accessKeyId,
		UserID:      userId,
		Context:     ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListUserGroups(ctx context.Context, userId string, after string, amount int) ([]*models.Group, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserGroups(&auth.ListUserGroupsParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		UserID:  userId,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) ListUserPolicies(ctx context.Context, userId string, effective bool, after string, amount int) ([]*models.Policy, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserPolicies(&auth.ListUserPoliciesParams{
		After:     swag.String(after),
		Amount:    swag.Int64(int64(amount)),
		Effective: swag.Bool(effective),
		UserID:    userId,
		Context:   ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AttachPolicyToUser(ctx context.Context, userId, policyId string) error {
	_, err := c.remote.Auth.AttachPolicyToUser(&auth.AttachPolicyToUserParams{
		PolicyID: policyId,
		UserID:   userId,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) DetachPolicyFromUser(ctx context.Context, userId, policyId string) error {
	_, err := c.remote.Auth.DetachPolicyFromUser(&auth.DetachPolicyFromUserParams{
		PolicyID: policyId,
		UserID:   userId,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) ListGroupPolicies(ctx context.Context, groupId string, after string, amount int) ([]*models.Policy, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListGroupPolicies(&auth.ListGroupPoliciesParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		GroupID: groupId,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AttachPolicyToGroup(ctx context.Context, groupId, policyId string) error {
	_, err := c.remote.Auth.AttachPolicyToGroup(&auth.AttachPolicyToGroupParams{
		PolicyID: policyId,
		GroupID:  groupId,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) DetachPolicyFromGroup(ctx context.Context, groupId, policyId string) error {
	_, err := c.remote.Auth.DetachPolicyFromGroup(&auth.DetachPolicyFromGroupParams{
		PolicyID: policyId,
		GroupID:  groupId,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) ListRepositories(ctx context.Context, after string, amount int) ([]*models.Repository, *models.Pagination, error) {
	resp, err := c.remote.Repositories.ListRepositories(&repositories.ListRepositoriesParams{
		After:   swag.String(after),
		Amount:  swag.Int64(int64(amount)),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) GetRepository(ctx context.Context, repoId string) (*models.Repository, error) {
	resp, err := c.remote.Repositories.GetRepository(&repositories.GetRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListBranches(ctx context.Context, repoId string, after string, amount int) ([]*models.Ref, *models.Pagination, error) {
	resp, err := c.remote.Branches.ListBranches(&branches.ListBranchesParams{
		After:        swag.String(after),
		Amount:       swag.Int64(int64(amount)),
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreateRepository(ctx context.Context, repository *models.RepositoryCreation) error {
	_, err := c.remote.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteRepository(ctx context.Context, repoId string) error {
	_, err := c.remote.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) GetBranch(ctx context.Context, repoId, branchId string) (*models.Ref, error) {
	resp, err := c.remote.Branches.GetBranch(&branches.GetBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) CreateBranch(ctx context.Context, repoId string, branch *models.BranchCreation) (*models.Ref, error) {
	resp, err := c.remote.Branches.CreateBranch(&branches.CreateBranchParams{
		Branch:       branch,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeleteBranch(ctx context.Context, repoId, branchId string) error {
	_, err := c.remote.Branches.DeleteBranch(&branches.DeleteBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) RevertBranch(ctx context.Context, repoId, branchId string, revertProps *models.RevertCreation) error {
	_, err := c.remote.Branches.RevertBranch(&branches.RevertBranchParams{
		BranchID:     branchId,
		Revert:       revertProps,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) Commit(ctx context.Context, repoId, branchId, message string, metadata map[string]string) (*models.Commit, error) {
	commit, err := c.remote.Commits.Commit(&commits.CommitParams{
		BranchID: branchId,
		Commit: &models.CommitCreation{
			Message:  &message,
			Metadata: metadata,
		},
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommit(ctx context.Context, repoId, commitId string) (*models.Commit, error) {
	commit, err := c.remote.Commits.GetCommit(&commits.GetCommitParams{
		CommitID:     commitId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommitLog(ctx context.Context, repoId, branchId, after string, amount int) ([]*models.Commit, *models.Pagination, error) {
	resp, err := c.remote.Commits.GetBranchCommitLog(&commits.GetBranchCommitLogParams{
		Amount:       swag.Int64(int64(amount)),
		After:        swag.String(after),
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) DiffRefs(ctx context.Context, repoId, leftRef, rightRef string) ([]*models.Diff, error) {
	diff, err := c.remote.Refs.DiffRefs(&refs.DiffRefsParams{
		LeftRef:      leftRef,
		RightRef:     rightRef,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return diff.GetPayload().Results, nil
}

func (c *client) Merge(ctx context.Context, repoId, leftRef, rightRef string) ([]*models.MergeResult, error) {
	statusOK, err := c.remote.Refs.MergeIntoBranch(&refs.MergeIntoBranchParams{
		DestinationRef: leftRef,
		SourceRef:      rightRef,
		RepositoryID:   repoId,
		Context:        ctx,
	}, c.auth)

	if err == nil {
		return statusOK.Payload.Results, nil
	}
	conflict, ok := err.(*refs.MergeIntoBranchConflict)
	if ok {
		return conflict.Payload.Results, errors.ErrMergeConflict
	} else {
		return nil, err
	}
}

func (c *client) DiffBranch(ctx context.Context, repoId, branch string) ([]*models.Diff, error) {
	diff, err := c.remote.Branches.DiffBranch(&branches.DiffBranchParams{
		BranchID:     branch,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return diff.GetPayload().Results, nil
}

func (c *client) StatObject(ctx context.Context, repoId, ref, path string, readUncommitted bool) (*models.ObjectStats, error) {
	resp, err := c.remote.Objects.StatObject(&objects.StatObjectParams{
		Ref:             ref,
		Path:            path,
		RepositoryID:    repoId,
		Context:         ctx,
		ReadUncommitted: swag.Bool(readUncommitted),
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListObjects(ctx context.Context, repoId, ref, tree, after string, amount int, readUncommitted bool) ([]*models.ObjectStats, *models.Pagination, error) {
	resp, err := c.remote.Objects.ListObjects(&objects.ListObjectsParams{
		After:           swag.String(after),
		Amount:          swag.Int64(int64(amount)),
		Ref:             ref,
		RepositoryID:    repoId,
		Tree:            swag.String(tree),
		Context:         ctx,
		ReadUncommitted: swag.Bool(readUncommitted),
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) GetObject(ctx context.Context, repoId, ref, path string, readUncommitted bool, writer io.Writer) (*objects.GetObjectOK, error) {
	params := &objects.GetObjectParams{
		Ref:             ref,
		Path:            path,
		RepositoryID:    repoId,
		Context:         ctx,
		ReadUncommitted: swag.Bool(readUncommitted),
	}
	resp, err := c.remote.Objects.GetObject(params, c.auth, writer)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *client) UploadObject(ctx context.Context, repoId, branchId, path string, r io.Reader) (*models.ObjectStats, error) {
	resp, err := c.remote.Objects.UploadObject(&objects.UploadObjectParams{
		BranchID:     branchId,
		Content:      runtime.NamedReader("content", r),
		Path:         path,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeleteObject(ctx context.Context, repoId, branchId, path string) error {
	_, err := c.remote.Objects.DeleteObject(&objects.DeleteObjectParams{
		BranchID:     branchId,
		Path:         path,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func NewClient(endpointURL, accessKeyId, secretAccessKey string) (Client, error) {
	parsedUrl, err := url.Parse(endpointURL)
	if err != nil {
		return nil, err
	}
	if len(parsedUrl.Path) == 0 {
		parsedUrl.Path = path.Join(parsedUrl.Path, genclient.DefaultBasePath)
	}
	return &client{
		remote: genclient.New(httptransport.New(parsedUrl.Host, parsedUrl.Path, []string{parsedUrl.Scheme}), strfmt.Default),
		auth:   httptransport.BasicAuth(accessKeyId, secretAccessKey),
	}, nil
}
