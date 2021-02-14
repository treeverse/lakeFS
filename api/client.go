package api

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/auth"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/metadata"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/client/tags"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/catalog"
)

type AuthClient interface {
	GetCurrentUser(ctx context.Context) (*models.User, error)
	GetUser(ctx context.Context, userID string) (*models.User, error)
	ListUsers(ctx context.Context, after string, amount int) ([]*models.User, *models.Pagination, error)
	DeleteUser(ctx context.Context, userID string) error
	CreateUser(ctx context.Context, userID string) (*models.User, error)
	GetGroup(ctx context.Context, groupID string) (*models.Group, error)
	ListGroups(ctx context.Context, after string, amount int) ([]*models.Group, *models.Pagination, error)
	CreateGroup(ctx context.Context, groupID string) (*models.Group, error)
	DeleteGroup(ctx context.Context, groupID string) error
	ListPolicies(ctx context.Context, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	CreatePolicy(ctx context.Context, policy *models.Policy) (*models.Policy, error)
	GetPolicy(ctx context.Context, policyID string) (*models.Policy, error)
	DeletePolicy(ctx context.Context, policyID string) error
	ListGroupMembers(ctx context.Context, groupID string, after string, amount int) ([]*models.User, *models.Pagination, error)
	AddGroupMembership(ctx context.Context, groupID, userID string) error
	DeleteGroupMembership(ctx context.Context, groupID, userID string) error
	ListUserCredentials(ctx context.Context, userID string, after string, amount int) ([]*models.Credentials, *models.Pagination, error)
	CreateCredentials(ctx context.Context, userID string) (*models.CredentialsWithSecret, error)
	DeleteCredentials(ctx context.Context, userID, accessKeyID string) error
	GetCredentials(ctx context.Context, userID, accessKeyID string) (*models.Credentials, error)
	ListUserGroups(ctx context.Context, userID string, after string, amount int) ([]*models.Group, *models.Pagination, error)
	ListUserPolicies(ctx context.Context, userID string, effective bool, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	AttachPolicyToUser(ctx context.Context, userID, policyID string) error
	DetachPolicyFromUser(ctx context.Context, userID, policyID string) error
	ListGroupPolicies(ctx context.Context, groupID string, after string, amount int) ([]*models.Policy, *models.Pagination, error)
	AttachPolicyToGroup(ctx context.Context, groupID, policyID string) error
	DetachPolicyFromGroup(ctx context.Context, groupID, policyID string) error
}

type RepositoryClient interface {
	ListRepositories(ctx context.Context, after string, amount int) ([]*models.Repository, *models.Pagination, error)
	GetRepository(ctx context.Context, repository string) (*models.Repository, error)
	CreateRepository(ctx context.Context, repository *models.RepositoryCreation) error
	CreateBareRepository(ctx context.Context, repository *models.RepositoryCreation) error
	DeleteRepository(ctx context.Context, repository string) error

	ListBranches(ctx context.Context, repository string, from string, amount int) ([]*models.Ref, *models.Pagination, error)
	GetBranch(ctx context.Context, repository, branchID string) (string, error)
	CreateBranch(ctx context.Context, repository string, branch *models.BranchCreation) (string, error)
	DeleteBranch(ctx context.Context, repository, branchID string) error
	ResetBranch(ctx context.Context, repository, branchID string, resetProps *models.ResetCreation) error
	RevertBranch(ctx context.Context, repository, branchID string, commitRef string, parentNumber int) error

	ListTags(ctx context.Context, repository string, from string, amount int) ([]*models.Ref, *models.Pagination, error)
	GetTag(ctx context.Context, repository, tagID string) (string, error)
	CreateTag(ctx context.Context, repository string, tagID string, ref string) (string, error)
	DeleteTag(ctx context.Context, repository, tagID string) error

	Commit(ctx context.Context, repository, branchID, message string, metadata map[string]string) (*models.Commit, error)
	GetCommit(ctx context.Context, repository, commitID string) (*models.Commit, error)
	GetCommitLog(ctx context.Context, repository, branchID, after string, amount int) ([]*models.Commit, *models.Pagination, error)

	StatObject(ctx context.Context, repository, ref, path string) (*models.ObjectStats, error)
	ListObjects(ctx context.Context, repository, ref, prefix, from string, amount int) ([]*models.ObjectStats, *models.Pagination, error)
	GetObject(ctx context.Context, repository, ref, path string, w io.Writer) (*objects.GetObjectOK, error)
	UploadObject(ctx context.Context, repository, branchID, path string, r io.Reader) (*models.ObjectStats, error)
	DeleteObject(ctx context.Context, repository, branchID, path string) error

	DiffRefs(ctx context.Context, repository, leftRef, rightRef string, after string, amount int) ([]*models.Diff, *models.Pagination, error)
	Merge(ctx context.Context, repository, destinationBranch, sourceRef string) (*models.MergeResult, error)

	DiffBranch(ctx context.Context, repository, branch string, after string, amount int) ([]*models.Diff, *models.Pagination, error)

	Symlink(ctx context.Context, repoID, ref, path string) (string, error)

	RefsDump(ctx context.Context, repository string) (*models.RefsDump, error)
	RefsRestore(ctx context.Context, repository string, manifest *models.RefsDump) error
}

type Client interface {
	AuthClient
	RepositoryClient
}

type client struct {
	remote    *genclient.Lakefs
	transport *httptransport.Runtime
	auth      runtime.ClientAuthInfoWriter
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

func (c *client) GetUser(ctx context.Context, userID string) (*models.User, error) {
	resp, err := c.remote.Auth.GetUser(&auth.GetUserParams{
		UserID:  userID,
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

func (c *client) DeleteUser(ctx context.Context, userID string) error {
	_, err := c.remote.Auth.DeleteUser(&auth.DeleteUserParams{
		UserID:  userID,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) CreateUser(ctx context.Context, userID string) (*models.User, error) {
	resp, err := c.remote.Auth.CreateUser(&auth.CreateUserParams{
		User: &models.UserCreation{
			ID: swag.String(userID),
		},
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) GetGroup(ctx context.Context, groupID string) (*models.Group, error) {
	resp, err := c.remote.Auth.GetGroup(&auth.GetGroupParams{
		GroupID: groupID,
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

func (c *client) CreateGroup(ctx context.Context, groupID string) (*models.Group, error) {
	resp, err := c.remote.Auth.CreateGroup(&auth.CreateGroupParams{
		Group: &models.GroupCreation{
			ID: swag.String(groupID),
		},
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) DeleteGroup(ctx context.Context, groupID string) error {
	_, err := c.remote.Auth.DeleteGroup(&auth.DeleteGroupParams{
		GroupID: groupID,
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

func (c *client) CreatePolicy(ctx context.Context, policy *models.Policy) (*models.Policy, error) {
	resp, err := c.remote.Auth.CreatePolicy(&auth.CreatePolicyParams{
		Policy:  policy,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) GetPolicy(ctx context.Context, policyID string) (*models.Policy, error) {
	resp, err := c.remote.Auth.GetPolicy(&auth.GetPolicyParams{
		PolicyID: policyID,
		Context:  ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeletePolicy(ctx context.Context, policyID string) error {
	_, err := c.remote.Auth.DeletePolicy(&auth.DeletePolicyParams{
		PolicyID: policyID,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) ListGroupMembers(ctx context.Context, groupID string, after string, amount int) ([]*models.User, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListGroupMembers(&auth.ListGroupMembersParams{
		Amount:  swag.Int64(int64(amount)),
		GroupID: groupID,
		After:   swag.String(after),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AddGroupMembership(ctx context.Context, groupID, userID string) error {
	_, err := c.remote.Auth.AddGroupMembership(&auth.AddGroupMembershipParams{
		GroupID: groupID,
		UserID:  userID,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteGroupMembership(ctx context.Context, groupID, userID string) error {
	_, err := c.remote.Auth.DeleteGroupMembership(&auth.DeleteGroupMembershipParams{
		GroupID: groupID,
		UserID:  userID,
		Context: ctx,
	}, c.auth)
	return err
}

func (c *client) ListUserCredentials(ctx context.Context, userID string, after string, amount int) ([]*models.Credentials, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserCredentials(&auth.ListUserCredentialsParams{
		Amount:     swag.Int64(int64(amount)),
		After:      swag.String(after),
		UserID:     userID,
		Context:    ctx,
		HTTPClient: nil,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreateCredentials(ctx context.Context, userID string) (*models.CredentialsWithSecret, error) {
	resp, err := c.remote.Auth.CreateCredentials(&auth.CreateCredentialsParams{
		UserID:     userID,
		Context:    ctx,
		HTTPClient: nil,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), err
}

func (c *client) DeleteCredentials(ctx context.Context, userID, accessKeyID string) error {
	_, err := c.remote.Auth.DeleteCredentials(&auth.DeleteCredentialsParams{
		AccessKeyID: accessKeyID,
		UserID:      userID,
		Context:     ctx,
	}, c.auth)
	return err
}

func (c *client) GetCredentials(ctx context.Context, userID, accessKeyID string) (*models.Credentials, error) {
	resp, err := c.remote.Auth.GetCredentials(&auth.GetCredentialsParams{
		AccessKeyID: accessKeyID,
		UserID:      userID,
		Context:     ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListUserGroups(ctx context.Context, userID string, after string, amount int) ([]*models.Group, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserGroups(&auth.ListUserGroupsParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		UserID:  userID,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) ListUserPolicies(ctx context.Context, userID string, effective bool, after string, amount int) ([]*models.Policy, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListUserPolicies(&auth.ListUserPoliciesParams{
		After:     swag.String(after),
		Amount:    swag.Int64(int64(amount)),
		Effective: swag.Bool(effective),
		UserID:    userID,
		Context:   ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AttachPolicyToUser(ctx context.Context, userID, policyID string) error {
	_, err := c.remote.Auth.AttachPolicyToUser(&auth.AttachPolicyToUserParams{
		PolicyID: policyID,
		UserID:   userID,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) DetachPolicyFromUser(ctx context.Context, userID, policyID string) error {
	_, err := c.remote.Auth.DetachPolicyFromUser(&auth.DetachPolicyFromUserParams{
		PolicyID: policyID,
		UserID:   userID,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) ListGroupPolicies(ctx context.Context, groupID string, after string, amount int) ([]*models.Policy, *models.Pagination, error) {
	resp, err := c.remote.Auth.ListGroupPolicies(&auth.ListGroupPoliciesParams{
		Amount:  swag.Int64(int64(amount)),
		After:   swag.String(after),
		GroupID: groupID,
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) AttachPolicyToGroup(ctx context.Context, groupID, policyID string) error {
	_, err := c.remote.Auth.AttachPolicyToGroup(&auth.AttachPolicyToGroupParams{
		PolicyID: policyID,
		GroupID:  groupID,
		Context:  ctx,
	}, c.auth)
	return err
}

func (c *client) DetachPolicyFromGroup(ctx context.Context, groupID, policyID string) error {
	_, err := c.remote.Auth.DetachPolicyFromGroup(&auth.DetachPolicyFromGroupParams{
		PolicyID: policyID,
		GroupID:  groupID,
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

func (c *client) GetRepository(ctx context.Context, repository string) (*models.Repository, error) {
	resp, err := c.remote.Repositories.GetRepository(&repositories.GetRepositoryParams{
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListBranches(ctx context.Context, repository string, after string, amount int) ([]*models.Ref, *models.Pagination, error) {
	resp, err := c.remote.Branches.ListBranches(&branches.ListBranchesParams{
		After:      swag.String(after),
		Amount:     swag.Int64(int64(amount)),
		Repository: repository,
		Context:    ctx,
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

func (c *client) CreateBareRepository(ctx context.Context, repository *models.RepositoryCreation) error {
	_, err := c.remote.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
		Bare:       swag.Bool(true),
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteRepository(ctx context.Context, repository string) error {
	_, err := c.remote.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) GetBranch(ctx context.Context, repository, branchID string) (string, error) {
	resp, err := c.remote.Branches.GetBranch(&branches.GetBranchParams{
		Branch:     branchID,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return "", err
	}
	return swag.StringValue(resp.GetPayload().CommitID), nil
}

func (c *client) CreateBranch(ctx context.Context, repository string, branch *models.BranchCreation) (string, error) {
	resp, err := c.remote.Branches.CreateBranch(&branches.CreateBranchParams{
		Branch:     branch,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return "", err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeleteBranch(ctx context.Context, repository, branchID string) error {
	_, err := c.remote.Branches.DeleteBranch(&branches.DeleteBranchParams{
		Branch:     branchID,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) ResetBranch(ctx context.Context, repository, branchID string, resetProps *models.ResetCreation) error {
	_, err := c.remote.Branches.ResetBranch(branches.NewResetBranchParams().
		WithBranch(branchID).
		WithRepository(repository).
		WithContext(ctx).
		WithReset(resetProps), c.auth)
	return err
}

func (c *client) RevertBranch(ctx context.Context, repository, branchID string, commitRef string, parentNumber int) error {
	_, err := c.remote.Branches.Revert(branches.NewRevertParams().
		WithBranch(branchID).
		WithRepository(repository).
		WithContext(ctx).
		WithRevert(branches.RevertBody{Ref: commitRef, ParentNumber: int64(parentNumber)}), c.auth)
	return err
}

func (c *client) Commit(ctx context.Context, repository, branchID, message string, metadata map[string]string) (*models.Commit, error) {
	commit, err := c.remote.Commits.Commit(&commits.CommitParams{
		Branch: branchID,
		Commit: &models.CommitCreation{
			Message:  &message,
			Metadata: metadata,
		},
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommit(ctx context.Context, repository, commitID string) (*models.Commit, error) {
	commit, err := c.remote.Commits.GetCommit(&commits.GetCommitParams{
		CommitID:   commitID,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommitLog(ctx context.Context, repository, branchID, after string, amount int) ([]*models.Commit, *models.Pagination, error) {
	resp, err := c.remote.Commits.GetBranchCommitLog(&commits.GetBranchCommitLogParams{
		Amount:     swag.Int64(int64(amount)),
		After:      swag.String(after),
		Branch:     branchID,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) DiffRefs(ctx context.Context, repository, leftRef, rightRef, after string, amount int) ([]*models.Diff, *models.Pagination, error) {
	diff, err := c.remote.Refs.DiffRefs(&refs.DiffRefsParams{
		After:      swag.String(after),
		Amount:     swag.Int64(int64(amount)),
		LeftRef:    leftRef,
		Repository: repository,
		RightRef:   rightRef,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	payload := diff.GetPayload()
	return payload.Results, payload.Pagination, nil
}

func (c *client) Merge(ctx context.Context, repository, destinationBranch, sourceRef string) (*models.MergeResult, error) {
	statusOK, err := c.remote.Refs.MergeIntoBranch(&refs.MergeIntoBranchParams{
		DestinationBranch: destinationBranch,
		SourceRef:         sourceRef,
		Repository:        repository,
		Context:           ctx,
	}, c.auth)

	if err == nil {
		return statusOK.Payload, nil
	}
	conflict, ok := err.(*refs.MergeIntoBranchConflict)
	if ok {
		return conflict.Payload, catalog.ErrConflictFound
	}
	return nil, err
}

func (c *client) DiffBranch(ctx context.Context, repoID, branch string, after string, amount int) ([]*models.Diff, *models.Pagination, error) {
	diff, err := c.remote.Branches.DiffBranch(&branches.DiffBranchParams{
		After:      swag.String(after),
		Amount:     swag.Int64(int64(amount)),
		Branch:     branch,
		Repository: repoID,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	payload := diff.GetPayload()
	return payload.Results, payload.Pagination, nil
}

func (c *client) ListTags(ctx context.Context, repository string, from string, amount int) ([]*models.Ref, *models.Pagination, error) {
	resp, err := c.remote.Tags.ListTags(
		tags.NewListTagsParamsWithContext(ctx).
			WithRepository(repository).
			WithAfter(swag.String(from)).
			WithAmount(swag.Int64(int64(amount))),
		c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) GetTag(ctx context.Context, repository, tagID string) (string, error) {
	resp, err := c.remote.Tags.GetTag(
		tags.NewGetTagParamsWithContext(ctx).
			WithRepository(repository).
			WithTag(tagID),
		c.auth)
	if err != nil {
		return "", err
	}
	return swag.StringValue(resp.GetPayload().CommitID), nil
}

func (c *client) CreateTag(ctx context.Context, repository string, tagID string, ref string) (string, error) {
	resp, err := c.remote.Tags.CreateTag(
		tags.NewCreateTagParamsWithContext(ctx).
			WithRepository(repository).
			WithTag(&models.TagCreation{
				ID:  swag.String(tagID),
				Ref: swag.String(ref),
			}),
		c.auth)
	if err != nil {
		return "", err
	}
	return swag.StringValue(resp.GetPayload().CommitID), nil
}

func (c *client) DeleteTag(ctx context.Context, repository, tagID string) error {
	_, err := c.remote.Tags.DeleteTag(
		tags.NewDeleteTagParamsWithContext(ctx).
			WithRepository(repository).
			WithTag(tagID),
		c.auth)
	return err
}

func (c *client) Symlink(ctx context.Context, repoID, branch, path string) (string, error) {
	resp, err := c.remote.Metadata.CreateSymlink(&metadata.CreateSymlinkParams{
		Location:   swag.String(path),
		Branch:     branch,
		Repository: repoID,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return "", err
	}
	return resp.GetPayload(), nil
}

func (c *client) StatObject(ctx context.Context, repoID, ref, path string) (*models.ObjectStats, error) {
	resp, err := c.remote.Objects.StatObject(&objects.StatObjectParams{
		Ref:        ref,
		Path:       path,
		Repository: repoID,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListObjects(ctx context.Context, repoID, ref, prefix, after string, amount int) ([]*models.ObjectStats, *models.Pagination, error) {
	resp, err := c.remote.Objects.ListObjects(&objects.ListObjectsParams{
		After:      swag.String(after),
		Amount:     swag.Int64(int64(amount)),
		Ref:        ref,
		Repository: repoID,
		Prefix:     swag.String(prefix),
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) GetObject(ctx context.Context, repoID, ref, path string, writer io.Writer) (*objects.GetObjectOK, error) {
	params := &objects.GetObjectParams{
		Ref:        ref,
		Path:       path,
		Repository: repoID,
		Context:    ctx,
	}
	resp, err := c.remote.Objects.GetObject(params, c.auth, writer)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *client) UploadObject(ctx context.Context, repoID, branchID, path string, r io.Reader) (*models.ObjectStats, error) {
	resp, err := c.remote.Objects.UploadObject(&objects.UploadObjectParams{
		Branch:     branchID,
		Content:    runtime.NamedReader("content", r),
		Path:       path,
		Repository: repoID,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) DeleteObject(ctx context.Context, repository, branchID, path string) error {
	_, err := c.remote.Objects.DeleteObject(&objects.DeleteObjectParams{
		Branch:     branchID,
		Path:       path,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) RefsDump(ctx context.Context, repository string) (*models.RefsDump, error) {
	resp, err := c.remote.Refs.Dump(&refs.DumpParams{
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) RefsRestore(ctx context.Context, repository string, manifest *models.RefsDump) error {
	_, err := c.remote.Refs.Restore(&refs.RestoreParams{
		Manifest:   manifest,
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

type ClientOption func(*client)

func MaxIdleConnsPerHost(maxIdleConnsPerHost int) ClientOption {
	return func(c *client) {
		c.transport.Transport.(*http.Transport).MaxIdleConnsPerHost = maxIdleConnsPerHost
	}
}

func NewClient(endpointURL, accessKeyID, secretAccessKey string, opts ...ClientOption) (Client, error) {
	parsedURL, err := url.Parse(endpointURL)
	if err != nil {
		return nil, err
	}
	if len(parsedURL.Path) == 0 {
		parsedURL.Path = path.Join(parsedURL.Path, genclient.DefaultBasePath)
	}
	transport := httptransport.New(parsedURL.Host, parsedURL.Path, []string{parsedURL.Scheme})
	transport.Transport.(*http.Transport).MaxIdleConnsPerHost = 100 // make this configurable?
	c := &client{
		transport: transport,
		remote:    genclient.New(transport, strfmt.Default),
		auth:      httptransport.BasicAuth(accessKeyID, secretAccessKey),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}
