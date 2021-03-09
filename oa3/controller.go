package oa3

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/logging"

	"github.com/labstack/echo/v4/middleware"

	"github.com/labstack/echo/v4"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
)

var (
	DefaultMaxPerPage int = 1000
)

type Controller struct {
	Auth auth.Service
}

func getCtx(c echo.Context) context.Context {
	return c.Request().Context()
}

func paginationAfter(v *PaginationAfter) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationAmount(v *PaginationAmount) int {
	if v == nil {
		return DefaultMaxPerPage
	}
	i := int(*v)
	if i > DefaultMaxPerPage {
		return DefaultMaxPerPage
	}
	if i <= 0 {
		return DefaultMaxPerPage
	}
	return i
}

func (c *Controller) internalError(ctx echo.Context, e error) error {
	return ctx.JSON(http.StatusInternalServerError, Error{
		Message: e.Error(),
	})
}

func (c *Controller) paginationFor(next string, hasMore bool, length int) Pagination {
	return Pagination{
		HasMore:    hasMore,
		MaxPerPage: DefaultMaxPerPage,
		NextOffset: next,
		Results:    length,
	}
}
func (c *Controller) authorize(ctx echo.Context, perms []permissions.Permission) (bool, error) {
	user := ctx.Get("user").(*model.User)
	resp, err := c.Auth.Authorize(ctx.Request().Context(), &auth.AuthorizationRequest{
		Username:            user.Username,
		RequiredPermissions: perms,
	})
	if err != nil {
		return false, c.internalError(ctx, err)
	}
	if resp.Error != nil {
		return false, ctx.JSON(http.StatusUnauthorized, Error{
			Message: resp.Error.Error(),
		})
	}
	if !resp.Allowed {
		return false, ctx.JSON(http.StatusUnauthorized, Error{
			Message: "User does not have the required permissions",
		})
	}
	return true, nil
}

func (c *Controller) ListGroups(ctx echo.Context, params ListGroupsParams) error {
	if authorized, err := c.authorize(ctx, []permissions.Permission{
		{
			Action:   permissions.ListGroupsAction,
			Resource: permissions.All,
		},
	}); err != nil || !authorized {
		return nil
	}

	groups, paginator, err := c.Auth.ListGroups(getCtx(ctx), &model.PaginationParams{
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return c.internalError(ctx, err)
	}
	results := make([]Group, len(groups))
	for i, g := range groups {
		results[i] = Group{
			CreationDate: g.CreatedAt.Unix(),
			Id:           g.DisplayName,
		}
	}
	return ctx.JSON(http.StatusOK, GroupList{
		Pagination: c.paginationFor(paginator.NextPageToken, paginator.NextPageToken != "", len(groups)),
		Results:    results,
	})
}

func (c *Controller) CreateGroup(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) DeleteGroup(ctx echo.Context, groupId string) error {
	panic("implement me")
}

func (c *Controller) GetGroup(ctx echo.Context, groupId string) error {
	panic("implement me")
}

func (c *Controller) ListGroupMembers(ctx echo.Context, groupId string, params ListGroupMembersParams) error {
	panic("implement me")
}

func (c *Controller) DeleteGroupMembership(ctx echo.Context, groupId string, userId string) error {
	panic("implement me")
}

func (c *Controller) AddGroupMembership(ctx echo.Context, groupId string, userId string) error {
	panic("implement me")
}

func (c *Controller) ListGroupPolicies(ctx echo.Context, groupId string, params ListGroupPoliciesParams) error {
	panic("implement me")
}

func (c *Controller) DetachPolicyFromGroup(ctx echo.Context, groupId string, policyId string) error {
	panic("implement me")
}

func (c *Controller) AttachPolicyToGroup(ctx echo.Context, groupId string, policyId string) error {
	panic("implement me")
}

func (c *Controller) ListPolicies(ctx echo.Context, params ListPoliciesParams) error {
	panic("implement me")
}

func (c *Controller) CreatePolicy(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) DeletePolicy(ctx echo.Context, policyId string) error {
	panic("implement me")
}

func (c *Controller) GetPolicy(ctx echo.Context, policyId string) error {
	panic("implement me")
}

func (c *Controller) UpdatePolicy(ctx echo.Context, policyId string) error {
	panic("implement me")
}

func (c *Controller) ListUsers(ctx echo.Context, params ListUsersParams) error {
	panic("implement me")
}

func (c *Controller) CreateUser(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) DeleteUser(ctx echo.Context, userId string) error {
	panic("implement me")
}

func (c *Controller) GetUser(ctx echo.Context, userId string) error {
	panic("implement me")
}

func (c *Controller) ListUserCredentials(ctx echo.Context, userId string, params ListUserCredentialsParams) error {
	panic("implement me")
}

func (c *Controller) CreateCredentials(ctx echo.Context, userId string) error {
	panic("implement me")
}

func (c *Controller) DeleteCredentials(ctx echo.Context, userId string, accessKeyId string) error {
	panic("implement me")
}

func (c *Controller) GetCredentials(ctx echo.Context, userId string, accessKeyId string) error {
	panic("implement me")
}

func (c *Controller) ListUserGroups(ctx echo.Context, userId string, params ListUserGroupsParams) error {
	panic("implement me")
}

func (c *Controller) ListUserPolicies(ctx echo.Context, userId string, params ListUserPoliciesParams) error {
	panic("implement me")
}

func (c *Controller) DetachPolicyFromUser(ctx echo.Context, userId string, policyId string) error {
	panic("implement me")
}

func (c *Controller) AttachPolicyToUser(ctx echo.Context, userId string, policyId string) error {
	panic("implement me")
}

func (c *Controller) GetConfig(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) HealthCheck(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) ListRepositories(ctx echo.Context, params ListRepositoriesParams) error {
	panic("implement me")
}

func (c *Controller) CreateRepository(ctx echo.Context, params CreateRepositoryParams) error {
	panic("implement me")
}

func (c *Controller) DeleteRepository(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) GetRepository(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) ListRepositoryRuns(ctx echo.Context, repository string, params ListRepositoryRunsParams) error {
	panic("implement me")
}

func (c *Controller) GetRun(ctx echo.Context, repository string, runId string) error {
	panic("implement me")
}

func (c *Controller) ListRunHooks(ctx echo.Context, repository string, runId string, params ListRunHooksParams) error {
	panic("implement me")
}

func (c *Controller) GetRunHookOutput(ctx echo.Context, repository string, runId string, hookRunId string) error {
	panic("implement me")
}

func (c *Controller) ListBranches(ctx echo.Context, repository string, params ListBranchesParams) error {
	panic("implement me")
}

func (c *Controller) CreateBranch(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) DeleteBranch(ctx echo.Context, repository string, branch string) error {
	panic("implement me")
}

func (c *Controller) GetBranch(ctx echo.Context, repository string, branch string) error {
	panic("implement me")
}

func (c *Controller) ResetBranch(ctx echo.Context, repository string, branch string) error {
	panic("implement me")
}

func (c *Controller) Commit(ctx echo.Context, repository string, branch string) error {
	panic("implement me")
}

func (c *Controller) DiffBranch(ctx echo.Context, repository string, branch string, params DiffBranchParams) error {
	panic("implement me")
}

func (c *Controller) DeleteObject(ctx echo.Context, repository string, branch string, params DeleteObjectParams) error {
	panic("implement me")
}

func (c *Controller) UploadObject(ctx echo.Context, repository string, branch string, params UploadObjectParams) error {
	panic("implement me")
}

func (c *Controller) StageObject(ctx echo.Context, repository string, branch string, params StageObjectParams) error {
	panic("implement me")
}

func (c *Controller) RevertBranch(ctx echo.Context, repository string, branch string) error {
	panic("implement me")
}

func (c *Controller) GetCommit(ctx echo.Context, repository string, commitId string) error {
	panic("implement me")
}

func (c *Controller) ListCommitRuns(ctx echo.Context, repository string, commitId string) error {
	panic("implement me")
}

func (c *Controller) GetMetaRange(ctx echo.Context, repository string, metaRange string) error {
	panic("implement me")
}

func (c *Controller) GetRange(ctx echo.Context, repository string, pRange string) error {
	panic("implement me")
}

func (c *Controller) DumpRefs(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) RestoreRefs(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) CreateSymlinkFile(ctx echo.Context, repository string, branch string, params CreateSymlinkFileParams) error {
	panic("implement me")
}

func (c *Controller) DiffRefs(ctx echo.Context, repository string, leftRef string, rightRef string, params DiffRefsParams) error {
	panic("implement me")
}

func (c *Controller) LogCommits(ctx echo.Context, repository string, ref string, params LogCommitsParams) error {
	panic("implement me")
}

func (c *Controller) GetObject(ctx echo.Context, repository string, ref string, params GetObjectParams) error {
	panic("implement me")
}

func (c *Controller) ListObjects(ctx echo.Context, repository string, ref string, params ListObjectsParams) error {
	panic("implement me")
}

func (c *Controller) StatObject(ctx echo.Context, repository string, ref string, params StatObjectParams) error {
	panic("implement me")
}

func (c *Controller) GetUnderlyingProperties(ctx echo.Context, repository string, ref string, params GetUnderlyingPropertiesParams) error {
	panic("implement me")
}

func (c *Controller) MergeIntoBranch(ctx echo.Context, repository string, sourceRef string, destinationBranch string) error {
	panic("implement me")
}

func (c *Controller) ListTags(ctx echo.Context, repository string, params ListTagsParams) error {
	panic("implement me")
}

func (c *Controller) CreateTags(ctx echo.Context, repository string) error {
	panic("implement me")
}

func (c *Controller) DeleteTag(ctx echo.Context, repository string, tag string) error {
	panic("implement me")
}

func (c *Controller) GetTag(ctx echo.Context, repository string, tag string) error {
	panic("implement me")
}

func (c *Controller) Setup(ctx echo.Context) error {
	panic("implement me")
}

func (c *Controller) GetCurrentUser(ctx echo.Context) error {
	panic("implement me")
}

func NewServer(authService auth.Service) http.Handler {
	e := echo.New()
	logger := logging.Default()

	// set basic auth handler
	e.Use(middleware.BasicAuth(func(accessKey, secretKey string, c echo.Context) (bool, error) {
		ctx := c.Request().Context()
		credentials, err := authService.GetCredentials(ctx, accessKey)
		if err != nil {
			logger.WithError(err).WithField("access_key", accessKey).Debug("could not get access key for login")
			return false, nil
		}
		if secretKey != credentials.AccessSecretKey {
			logger.WithField("access_key", accessKey).Debug("access key secret does not match")
			return false, nil
		}
		user, err := authService.GetUserByID(ctx, credentials.UserID)
		if err != nil {
			logger.WithField("access_key", accessKey).Debug("could not find user for key pair")
			return false, nil
		}

		c.Set("user", user)
		return true, nil
	}))

	RegisterHandlersWithBaseURL(e, &Controller{
		Auth: authService,
	}, "/api/v2")
	return e
}

// ensure we meet ServerInterface
var _ ServerInterface = &Controller{}
