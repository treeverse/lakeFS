package api

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/mail"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/go-openapi/swag"
	"github.com/gorilla/sessions"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/oidc"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/templater"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
)

type contextKey string

const (
	// DefaultMaxPerPage is the maximum amount of results returned for paginated queries to the API
	DefaultMaxPerPage int        = 1000
	lakeFSPrefix                 = "symlinks"
	UserContextKey    contextKey = "user"

	actionStatusCompleted = "completed"
	actionStatusFailed    = "failed"
	actionStatusSkipped   = "skipped"

	entryTypeObject       = "object"
	entryTypeCommonPrefix = "common_prefix"

	setupStateInitialized    = "initialized"
	setupStateNotInitialized = "not_initialized"

	DefaultMaxDeleteObjects = 1000
)

type actionsHandler interface {
	GetRunResult(ctx context.Context, repositoryID string, runID string) (*actions.RunResult, error)
	GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*actions.TaskResult, error)
	ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (actions.RunResultIterator, error)
	ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (actions.TaskResultIterator, error)
}

type Controller struct {
	Config                *config.Config
	Catalog               catalog.Interface
	Authenticator         auth.Authenticator
	Auth                  auth.Service
	BlockAdapter          block.Adapter
	MetadataManager       auth.MetadataManager
	Migrator              db.Migrator
	Collector             stats.Collector
	CloudMetadataProvider cloud.MetadataProvider
	Actions               actionsHandler
	AuditChecker          AuditChecker
	Logger                logging.Logger
	Emailer               *email.Emailer
	Templater             templater.Service
	sessionStore          sessions.Store
	oidcAuthenticator     *oidc.Authenticator
}

func (c *Controller) GetAuthCapabilities(w http.ResponseWriter, _ *http.Request) {
	inviteSupported := c.Auth.IsInviteSupported()
	emailSupported := c.Emailer.Params.SMTPHost != ""
	writeResponse(w, http.StatusOK, AuthCapabilities{
		InviteUser:     &inviteSupported,
		ForgotPassword: &emailSupported,
	})
}

func (c *Controller) DeleteObjects(w http.ResponseWriter, r *http.Request, body DeleteObjectsJSONRequestBody, repository string, branch string) {
	ctx := r.Context()
	c.LogAction(ctx, "delete_objects")

	// limit check
	if len(body.Paths) > DefaultMaxDeleteObjects {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("%w, max paths is set to %d",
			ErrRequestSizeExceeded, DefaultMaxDeleteObjects))
		return
	}

	// delete all the files and collect responses
	var errs []ObjectError
	for _, objectPath := range body.Paths {
		// authorize this object deletion
		if !c.authorize(w, r, permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.DeleteObjectAction,
				Resource: permissions.ObjectArn(repository, objectPath),
			},
		}) {
			errs = append(errs, ObjectError{
				Path:       StringPtr(objectPath),
				StatusCode: http.StatusUnauthorized,
				Message:    http.StatusText(http.StatusUnauthorized),
			})
			continue
		}

		lg := c.Logger.WithField("path", objectPath)
		err := c.Catalog.DeleteEntry(ctx, repository, branch, objectPath)
		switch {
		case errors.Is(err, catalog.ErrNotFound):
			lg.Debug("tried to delete a non-existent object")
		case errors.Is(err, graveler.ErrWriteToProtectedBranch):
			errs = append(errs, ObjectError{
				Path:       StringPtr(objectPath),
				StatusCode: http.StatusForbidden,
				Message:    err.Error(),
			})
		case errors.Is(err, catalog.ErrPathRequiredValue):
			// issue #1706 - https://github.com/treeverse/lakeFS/issues/1706
			// Spark trying to delete the path "main/", which we map to branch "main" with an empty path.
			// Spark expects it to succeed (not deleting anything is a success), instead of returning an error.
			lg.Debug("tried to delete with an empty branch")
		case err != nil:
			lg.WithError(err).Error("failed deleting object")
			errs = append(errs, ObjectError{
				Path:       StringPtr(objectPath),
				StatusCode: http.StatusInternalServerError,
				Message:    err.Error(),
			})
		default:
			lg.Debug("object set for deletion")
		}
	}
	// no content in case there are no errors
	if len(errs) == 0 {
		writeResponse(w, http.StatusNoContent, nil)
		return
	}
	// status ok with list of errors
	response := ObjectErrorList{
		Errors: errs,
	}
	writeResponse(w, http.StatusOK, response)
}

// OauthCallback gets a code generated by an OIDC provider.
// It exchanges the code for an id token, and saves the claims from the ID token on a session.
func (c *Controller) OauthCallback(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	session, _ := c.sessionStore.Get(r, OIDCAuthSessionName)
	if r.URL.Query().Get("state") != session.Values[StateSessionKey] {
		http.Redirect(w, r, "/auth/login", http.StatusTemporaryRedirect)
		return
	}

	idTokenClaims, err := c.oidcAuthenticator.GetIDTokenClaims(ctx, r.URL.Query().Get("code"))
	if err != nil {
		http.Redirect(w, r, "/auth/login", http.StatusTemporaryRedirect)
		return
	}
	session.Values[IDTokenClaimsSessionKey] = idTokenClaims
	err = session.Save(r, w)
	if err != nil {
		c.Logger.WithError(err).Error("Failed to save OIDC session")
	}
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (c *Controller) Login(w http.ResponseWriter, r *http.Request, body LoginJSONRequestBody) {
	err := clearSession(w, r, c.sessionStore, OIDCAuthSessionName)
	if err != nil {
		c.Logger.WithError(err).Error("failed to perform OIDC logout")
	}
	ctx := r.Context()
	user, err := userByAuth(ctx, c.Logger, c.Authenticator, c.Auth, body.AccessKeyId, body.SecretAccessKey)
	if errors.Is(err, ErrAuthenticatingRequest) {
		writeResponse(w, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
		return
	}

	loginTime := time.Now()
	expires := loginTime.Add(DefaultLoginExpiration)
	secret := c.Auth.SecretStore().SharedSecret()

	tokenString, err := GenerateJWTLogin(secret, user.Username, loginTime, expires)
	if err != nil {
		writeError(w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}
	internalAuthSession, _ := c.sessionStore.Get(r, InternalAuthSessionName)
	internalAuthSession.Values[TokenSessionKeyName] = tokenString
	err = c.sessionStore.Save(r, w, internalAuthSession)
	if err != nil {
		c.Logger.WithError(err).Error("Failed to save internal auth session")
		writeError(w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}
	response := AuthenticationToken{
		Token: tokenString,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetPhysicalAddress(w http.ResponseWriter, r *http.Request, repository string, branch string, params GetPhysicalAddressParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "generate_physical_address")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	token, err := c.Catalog.GetStagingToken(ctx, repository, branch)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// Generate a name.
	name, err := nanoid.New()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	tokenPart := ""
	if token != nil {
		tokenPart = *token + "/"
	}
	qk, err := block.ResolveNamespace(repo.StorageNamespace, fmt.Sprintf("data/%s%s", tokenPart, name), block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := &StagingLocation{
		PhysicalAddress: StringPtr(qk.Format()),
		Token:           StringValue(token),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) LinkPhysicalAddress(w http.ResponseWriter, r *http.Request, body LinkPhysicalAddressJSONRequestBody, repository string, branch string, params LinkPhysicalAddressParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "stage_object")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// write metadata
	qk, err := block.ResolveNamespace(repo.StorageNamespace, params.Path, block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	blockStoreType := c.BlockAdapter.BlockstoreType()
	if qk.StorageType.BlockstoreType() != blockStoreType {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid storage type: %s: current block adapter is %s",
			qk.StorageType.BlockstoreType(),
			blockStoreType,
		))
		return
	}

	writeTime := time.Now()
	// Because CreateEntry tracks staging on a database with atomic operations,
	// _ignore_ the staging token here: no harm done even if a race was lost
	// against a commit.
	entryBuilder := catalog.NewDBEntryBuilder().
		CommonLevel(false).
		Path(params.Path).
		PhysicalAddress(*body.Staging.PhysicalAddress).
		AddressType(catalog.AddressTypeFull).
		CreationDate(writeTime).
		Size(body.SizeBytes).
		Checksum(body.Checksum).
		ContentType(StringValue(body.ContentType))
	if body.UserMetadata != nil {
		entryBuilder.Metadata(body.UserMetadata.AdditionalProperties)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	metadata := ObjectUserMetadata{AdditionalProperties: entry.Metadata}
	response := ObjectStats{
		Checksum:        entry.Checksum,
		ContentType:     &entry.ContentType,
		Metadata:        &metadata,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: entry.PhysicalAddress,
		SizeBytes:       Int64Ptr(entry.Size),
	}

	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListGroups(w http.ResponseWriter, r *http.Request, params ListGroupsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListGroupsAction,
			Resource: permissions.All,
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_groups")
	groups, paginator, err := c.Auth.ListGroups(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := GroupList{
		Results: make([]Group, 0, len(groups)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}

	for _, g := range groups {
		response.Results = append(response.Results, Group{
			Id:           g.DisplayName,
			CreationDate: g.CreatedAt.Unix(),
		})
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) CreateGroup(w http.ResponseWriter, r *http.Request, body CreateGroupJSONRequestBody) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateGroupAction,
			Resource: permissions.GroupArn(body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_group")

	g := &model.Group{
		CreatedAt:   time.Now().UTC(),
		DisplayName: body.Id,
	}
	err := c.Auth.CreateGroup(ctx, g)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := Group{
		CreationDate: g.CreatedAt.Unix(),
		Id:           g.DisplayName,
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) DeleteGroup(w http.ResponseWriter, r *http.Request, groupID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_group")
	err := c.Auth.DeleteGroup(ctx, groupID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "group not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetGroup(w http.ResponseWriter, r *http.Request, groupID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_group")
	g, err := c.Auth.GetGroup(ctx, groupID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "group not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := Group{
		Id:           g.DisplayName,
		CreationDate: g.CreatedAt.Unix(),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListGroupMembers(w http.ResponseWriter, r *http.Request, groupID string, params ListGroupMembersParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_group_users")
	users, paginator, err := c.Auth.ListGroupUsers(ctx, groupID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := UserList{
		Results: make([]User, 0, len(users)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, u := range users {
		response.Results = append(response.Results, User{
			Id:           u.Username,
			CreationDate: u.CreatedAt.Unix(),
			Email:        u.Email,
		})
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) DeleteGroupMembership(w http.ResponseWriter, r *http.Request, groupID string, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RemoveGroupMemberAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "remove_user_from_group")
	err := c.Auth.RemoveUserFromGroup(ctx, userID, groupID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) AddGroupMembership(w http.ResponseWriter, r *http.Request, groupID string, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AddGroupMemberAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "add_user_to_group")
	err := c.Auth.AddUserToGroup(ctx, userID, groupID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusCreated, nil)
}

func (c *Controller) ListGroupPolicies(w http.ResponseWriter, r *http.Request, groupID string, params ListGroupPoliciesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_group_policies")
	policies, paginator, err := c.Auth.ListGroupPolicies(ctx, groupID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := PolicyList{
		Results: make([]Policy, 0, len(policies)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}

	writeResponse(w, http.StatusOK, response)
}

func serializePolicy(p *model.Policy) Policy {
	stmts := make([]Statement, 0, len(p.Statement))
	for _, s := range p.Statement {
		stmts = append(stmts, Statement{
			Action:   s.Action,
			Effect:   s.Effect,
			Resource: s.Resource,
		})
	}
	createdAt := p.CreatedAt.Unix()
	return Policy{
		Id:           p.DisplayName,
		CreationDate: &createdAt, // TODO(barak): check if CreationDate should be required
		Statement:    stmts,
	}
}

func (c *Controller) DetachPolicyFromGroup(w http.ResponseWriter, r *http.Request, groupID string, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DetachPolicyAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "detach_policy_from_group")
	err := c.Auth.DetachPolicyFromGroup(ctx, policyID, groupID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) AttachPolicyToGroup(w http.ResponseWriter, r *http.Request, groupID string, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AttachPolicyAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "attach_policy_to_group")
	err := c.Auth.AttachPolicyToGroup(ctx, policyID, groupID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusCreated, nil)
}

func (c *Controller) ListPolicies(w http.ResponseWriter, r *http.Request, params ListPoliciesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListPoliciesAction,
			Resource: permissions.All,
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_policies")
	policies, paginator, err := c.Auth.ListPolicies(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := PolicyList{
		Results: make([]Policy, 0, len(policies)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) CreatePolicy(w http.ResponseWriter, r *http.Request, body CreatePolicyJSONRequestBody) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreatePolicyAction,
			Resource: permissions.PolicyArn(body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_policy")

	stmts := make(model.Statements, len(body.Statement))
	for i, apiStatement := range body.Statement {
		stmts[i] = model.Statement{
			Effect:   apiStatement.Effect,
			Action:   apiStatement.Action,
			Resource: apiStatement.Resource,
		}
	}

	p := &model.Policy{
		CreatedAt:   time.Now().UTC(),
		DisplayName: body.Id,
		Statement:   stmts,
	}

	err := c.Auth.WritePolicy(ctx, p)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	writeResponse(w, http.StatusCreated, serializePolicy(p))
}

func (c *Controller) DeletePolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeletePolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_policy")
	err := c.Auth.DeletePolicy(ctx, policyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "policy not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetPolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadPolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_policy")
	p, err := c.Auth.GetPolicy(ctx, policyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "policy not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := serializePolicy(p)
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) UpdatePolicy(w http.ResponseWriter, r *http.Request, body UpdatePolicyJSONRequestBody, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.UpdatePolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "update_policy")

	stmts := make(model.Statements, len(body.Statement))
	for i, apiStatement := range body.Statement {
		stmts[i] = model.Statement{
			Effect:   apiStatement.Effect,
			Action:   apiStatement.Action,
			Resource: apiStatement.Resource,
		}
	}

	p := &model.Policy{
		CreatedAt:   time.Now().UTC(),
		DisplayName: policyID,
		Statement:   stmts,
	}
	err := c.Auth.WritePolicy(ctx, p)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := serializePolicy(p)
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListUsers(w http.ResponseWriter, r *http.Request, params ListUsersParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListUsersAction,
			Resource: permissions.All,
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_users")
	users, paginator, err := c.Auth.ListUsers(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := UserList{
		Results: make([]User, 0, len(users)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, u := range users {
		response.Results = append(response.Results, User{
			Id:           u.Username,
			CreationDate: u.CreatedAt.Unix(),
			Email:        u.Email,
		})
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) generateResetPasswordToken(email string, duration time.Duration) (string, error) {
	secret := c.Auth.SecretStore().SharedSecret()
	currentTime := time.Now()
	return auth.GenerateJWTResetPassword(secret, email, currentTime, currentTime.Add(duration))
}

func (c *Controller) CreateUser(w http.ResponseWriter, r *http.Request, body CreateUserJSONRequestBody) {
	invite := swag.BoolValue(body.InviteUser)
	username := body.Id
	var parsedEmail *string
	if invite {
		addr, err := mail.ParseAddress(username)
		if err != nil {
			c.Logger.WithError(err).WithField("user_id", username).Warn("failed parsing email")
			writeError(w, http.StatusBadRequest, "Invalid email format")
			return
		}
		username = strings.ToLower(addr.Address)
		parsedEmail = &addr.Address
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateUserAction,
			Resource: permissions.UserArn(username),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_user")
	if invite {
		err := c.Auth.InviteUser(ctx, *parsedEmail)
		if c.handleAPIError(ctx, w, err) {
			c.Logger.WithError(err).WithField("email", *parsedEmail).Warn("failed creating user")
			return
		}
		writeResponse(w, http.StatusCreated, User{Id: *parsedEmail})
		return
	}
	u := &model.User{
		CreatedAt:    time.Now().UTC(),
		Username:     username,
		FriendlyName: nil,
		Source:       "internal",
		Email:        parsedEmail,
	}
	_, err := c.Auth.CreateUser(ctx, u)

	if c.handleAPIError(ctx, w, err) {
		c.Logger.WithError(err).WithField("username", u.Username).Warn("failed creating user")
		return
	}
	response := User{
		Id:           u.Username,
		CreationDate: u.CreatedAt.Unix(),
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) DeleteUser(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_user")
	err := c.Auth.DeleteUser(ctx, userID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetUser(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_user")
	u, err := c.Auth.GetUser(ctx, userID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := User{
		CreationDate: u.CreatedAt.Unix(),
		Id:           u.Username,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListUserCredentials(w http.ResponseWriter, r *http.Request, userID string, params ListUserCredentialsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_user_credentials")
	credentials, paginator, err := c.Auth.ListUserCredentials(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := CredentialsList{
		Results: make([]Credentials, 0, len(credentials)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, c := range credentials {
		response.Results = append(response.Results, Credentials{
			AccessKeyId:  c.AccessKeyID,
			CreationDate: c.IssuedDate.Unix(),
		})
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) CreateCredentials(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_credentials")
	credentials, err := c.Auth.CreateCredentials(ctx, userID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := CredentialsWithSecret{
		AccessKeyId:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		CreationDate:    credentials.IssuedDate.Unix(),
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) DeleteCredentials(w http.ResponseWriter, r *http.Request, userID string, accessKeyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_credentials")
	err := c.Auth.DeleteCredentials(ctx, userID, accessKeyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "credentials not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetCredentials(w http.ResponseWriter, r *http.Request, userID string, accessKeyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_credentials_for_user")
	credentials, err := c.Auth.GetCredentialsForUser(ctx, userID, accessKeyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "credentials not found")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := Credentials{
		AccessKeyId:  credentials.AccessKeyID,
		CreationDate: credentials.IssuedDate.Unix(),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListUserGroups(w http.ResponseWriter, r *http.Request, userID string, params ListUserGroupsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_user_groups")
	groups, paginator, err := c.Auth.ListUserGroups(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := GroupList{
		Results: make([]Group, 0, len(groups)),
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, g := range groups {
		response.Results = append(response.Results, Group{
			Id:           g.DisplayName,
			CreationDate: g.CreatedAt.Unix(),
		})
	}

	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListUserPolicies(w http.ResponseWriter, r *http.Request, userID string, params ListUserPoliciesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_user_policies")
	var listPolicies func(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
	if params.Effective != nil && *params.Effective {
		listPolicies = c.Auth.ListEffectivePolicies
	} else {
		listPolicies = c.Auth.ListUserPolicies
	}
	policies, paginator, err := listPolicies(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := PolicyList{
		Pagination: Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
		Results: make([]Policy, 0, len(policies)),
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) DetachPolicyFromUser(w http.ResponseWriter, r *http.Request, userID string, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DetachPolicyAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "detach_policy_from_user")
	err := c.Auth.DetachPolicyFromUser(ctx, policyID, userID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) AttachPolicyToUser(w http.ResponseWriter, r *http.Request, userID string, policyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AttachPolicyAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "attach_policy_to_user")
	err := c.Auth.AttachPolicyToUser(ctx, policyID, userID)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusCreated, nil)
}

func (c *Controller) GetStorageConfig(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadStorageConfiguration,
			Resource: permissions.All,
		},
	}) {
		return
	}
	info := c.BlockAdapter.GetStorageNamespaceInfo()
	response := StorageConfig{
		BlockstoreType:                   c.Config.GetBlockstoreType(),
		BlockstoreNamespaceValidityRegex: info.ValidityRegex,
		BlockstoreNamespaceExample:       info.Example,
		DefaultNamespacePrefix:           swag.String(c.Config.GetBlockstoreDefaultNamespacePrefix()),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) ListRepositories(w http.ResponseWriter, r *http.Request, params ListRepositoriesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListRepositoriesAction,
			Resource: permissions.All,
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_repos")

	repos, hasMore, err := c.Catalog.ListRepositories(ctx, paginationAmount(params.Amount), paginationPrefix(params.Prefix), paginationAfter(params.After))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("error listing repositories: %s", err))
		return
	}
	results := make([]Repository, 0, len(repos))
	for _, repo := range repos {
		creationDate := repo.CreationDate.Unix()
		r := Repository{
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
			CreationDate:     creationDate,
			DefaultBranch:    repo.DefaultBranch,
		}
		results = append(results, r)
	}
	repositoryList := RepositoryList{
		Pagination: paginationFor(hasMore, results, "Id"),
		Results:    results,
	}
	writeResponse(w, http.StatusOK, repositoryList)
}

func (c *Controller) CreateRepository(w http.ResponseWriter, r *http.Request, body CreateRepositoryJSONRequestBody, params CreateRepositoryParams) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateRepositoryAction,
					Resource: permissions.RepoArn(body.Name),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.AttachStorageNamespace,
					Resource: permissions.StorageNamespace(body.StorageNamespace),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_repo")

	defaultBranch := StringValue(body.DefaultBranch)
	if defaultBranch == "" {
		defaultBranch = "main"
	}

	if params.Bare != nil && *params.Bare {
		// create a bare repository. This is useful in conjunction with refs-restore to create a copy
		// of another repository by e.g. copying the _lakefs/ directory and restoring its refs
		repo, err := c.Catalog.CreateBareRepository(ctx,
			body.Name,
			body.StorageNamespace,
			defaultBranch)
		if c.handleAPIError(ctx, w, err) {
			return
		}
		response := Repository{
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
		}
		writeResponse(w, http.StatusOK, response)
		return
	}

	err := c.ensureStorageNamespace(ctx, body.StorageNamespace)
	if err != nil {
		reason := "unknown"
		var retErr error
		var urlErr *url.Error
		switch {
		case errors.As(err, &urlErr) && urlErr.Op == "parse":
			retErr = err
			reason = "bad_url"
		case errors.Is(err, block.ErrInvalidNamespace):
			retErr = fmt.Errorf("%w, must match: %s", err, c.BlockAdapter.BlockstoreType())
			reason = "invalid_namespace"
		case errors.Is(err, errStorageNamespaceInUse):
			retErr = err
			reason = "already_in_use"
		default:
			retErr = ErrFailedToAccessStorage
		}
		c.Logger.
			WithError(err).
			WithField("storage_namespace", body.StorageNamespace).
			WithField("reason", reason).
			Warn("Could not access storage namespace")
		writeError(w, http.StatusBadRequest, fmt.Errorf("failed to create repository: %w", retErr))
		return
	}

	newRepo, err := c.Catalog.CreateRepository(ctx, body.Name, body.StorageNamespace, defaultBranch)
	if err != nil {
		c.handleAPIError(ctx, w, fmt.Errorf("error creating repository: %w", err))
		return
	}

	response := Repository{
		CreationDate:     newRepo.CreationDate.Unix(),
		DefaultBranch:    newRepo.DefaultBranch,
		Id:               newRepo.Name,
		StorageNamespace: newRepo.StorageNamespace,
	}
	writeResponse(w, http.StatusCreated, response)
}

var errStorageNamespaceInUse = errors.New("lakeFS repositories can't share storage namespace")

func (c *Controller) ensureStorageNamespace(ctx context.Context, storageNamespace string) error {
	const (
		dummyKey  = "dummy"
		dummyData = "this is dummy data - created by lakeFS in order to check accessibility"
	)

	obj := block.ObjectPointer{StorageNamespace: storageNamespace, Identifier: dummyKey}
	objLen := int64(len(dummyData))
	if _, err := c.BlockAdapter.Get(ctx, obj, objLen); err == nil {
		return fmt.Errorf("found lakeFS objects in the storage namespace(%s): %w",
			storageNamespace, errStorageNamespaceInUse)
	} else if !errors.Is(err, adapter.ErrDataNotFound) {
		return err
	}

	if err := c.BlockAdapter.Put(ctx, obj, objLen, strings.NewReader(dummyData), block.PutOpts{}); err != nil {
		return err
	}

	_, err := c.BlockAdapter.Get(ctx, obj, objLen)
	return err
}

func (c *Controller) DeleteRepository(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_repo")
	err := c.Catalog.DeleteRepository(ctx, repository)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "repository not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetRepository(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_repo")
	repo, err := c.Catalog.GetRepository(ctx, repository)
	switch {
	case err == nil:
		response := Repository{
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
		}
		writeResponse(w, http.StatusOK, response)

	case errors.Is(err, catalog.ErrNotFound):
		writeError(w, http.StatusNotFound, "repository not found")

	case errors.Is(err, graveler.ErrRepositoryInDeletion):
		writeError(w, http.StatusGone, err)

	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (c *Controller) ListRepositoryRuns(w http.ResponseWriter, r *http.Request, repository string, params ListRepositoryRunsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_repository_runs")

	_, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	branch := StringValue(params.Branch)
	commitID := StringValue(params.Commit)
	runsIter, err := c.Actions.ListRunResults(ctx, repository, branch, commitID, paginationAfter(params.After))
	if c.handleAPIError(ctx, w, err) {
		return
	}
	defer runsIter.Close()

	response := ActionRunList{
		Pagination: Pagination{
			MaxPerPage: DefaultMaxPerPage,
		},
		Results: make([]ActionRun, 0),
	}
	amount := paginationAmount(params.Amount)
	for len(response.Results) < amount && runsIter.Next() {
		val := runsIter.Value()
		response.Results = append(response.Results, runResultToActionRun(val))
	}
	response.Pagination.Results = len(response.Results)
	if runsIter.Next() {
		response.Pagination.HasMore = true
		if len(response.Results) > 0 {
			lastRun := response.Results[len(response.Results)-1]
			response.Pagination.NextOffset = lastRun.RunId
		}
	}
	if err := runsIter.Err(); err != nil {
		writeResponse(w, http.StatusInternalServerError, err)
		return
	}
	writeResponse(w, http.StatusOK, response)
}

func runResultToActionRun(val *actions.RunResult) ActionRun {
	runResult := ActionRun{
		Branch:    val.BranchID,
		CommitId:  val.CommitID,
		RunId:     val.RunID,
		StartTime: val.StartTime,
		EndTime:   &val.EndTime,
		EventType: val.EventType,
	}
	if val.Passed {
		runResult.Status = actionStatusCompleted
	} else {
		runResult.Status = actionStatusFailed
	}
	return runResult
}

func (c *Controller) GetRun(w http.ResponseWriter, r *http.Request, repository string, runID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_get_run")
	_, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	runResult, err := c.Actions.GetRunResult(ctx, repository, runID)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	var status string
	if runResult.Passed {
		status = actionStatusCompleted
	} else {
		status = actionStatusFailed
	}
	response := ActionRun{
		RunId:     runResult.RunID,
		EventType: runResult.EventType,
		StartTime: runResult.StartTime,
		EndTime:   &runResult.EndTime,
		Status:    status,
		Branch:    runResult.BranchID,
		CommitId:  runResult.CommitID,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) ListRunHooks(w http.ResponseWriter, r *http.Request, repository string, runID string, params ListRunHooksParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_list_run_hooks")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	tasksIter, err := c.Actions.ListRunTaskResults(ctx, repo.Name, runID, paginationAfter(params.After))
	if c.handleAPIError(ctx, w, err) {
		return
	}
	defer tasksIter.Close()

	response := HookRunList{
		Results: make([]HookRun, 0),
		Pagination: Pagination{
			MaxPerPage: DefaultMaxPerPage,
		},
	}
	amount := paginationAmount(params.Amount)
	for len(response.Results) < amount && tasksIter.Next() {
		val := tasksIter.Value()
		hookRun := HookRun{
			HookRunId: val.HookRunID,
			Action:    val.ActionName,
			HookId:    val.HookID,
			StartTime: val.StartTime,
			EndTime:   &val.EndTime,
		}
		switch {
		case val.Passed:
			hookRun.Status = actionStatusCompleted
		case val.StartTime.IsZero(): // assumes that database values are only stored after run is finished
			hookRun.Status = actionStatusSkipped
			hookRun.EndTime = nil
		default:
			hookRun.Status = actionStatusFailed
		}
		response.Results = append(response.Results, hookRun)
	}
	if tasksIter.Next() {
		response.Pagination.HasMore = true
		if len(response.Results) > 0 {
			lastHookRun := response.Results[len(response.Results)-1]
			response.Pagination.NextOffset = lastHookRun.HookRunId
		}
	}
	if err := tasksIter.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	response.Pagination.Results = len(response.Results)
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetRunHookOutput(w http.ResponseWriter, r *http.Request, repository string, runID string, hookRunID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_run_hook_output")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	taskResult, err := c.Actions.GetTaskResult(ctx, repo.Name, runID, hookRunID)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	if taskResult.StartTime.IsZero() { // skipped task
		writeResponse(w, http.StatusOK, nil)
		return
	}

	logPath := taskResult.LogPath()
	reader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       logPath,
	}, -1)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	defer func() {
		_ = reader.Close()
	}()

	cd := mime.FormatMediaType("attachment", map[string]string{"filename": filepath.Base(logPath)})
	w.Header().Set("Content-Disposition", cd)
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, reader)
	if err != nil {
		c.Logger.WithError(err).WithField("log_path", logPath).Warn("Write run hook output")
	}
}

func (c *Controller) ListBranches(w http.ResponseWriter, r *http.Request, repository string, params ListBranchesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListBranchesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_branches")

	res, hasMore, err := c.Catalog.ListBranches(ctx, repository, paginationPrefix(params.Prefix), paginationAmount(params.Amount), paginationAfter(params.After))
	if c.handleAPIError(ctx, w, err) {
		return
	}

	refs := make([]Ref, 0, len(res))
	for _, branch := range res {
		refs = append(refs, Ref{
			CommitId: branch.Reference,
			Id:       branch.Name,
		})
	}
	response := RefList{
		Results:    refs,
		Pagination: paginationFor(hasMore, refs, "Id"),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) CreateBranch(w http.ResponseWriter, r *http.Request, body CreateBranchJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateBranchAction,
			Resource: permissions.BranchArn(repository, body.Name),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_branch")
	commitLog, err := c.Catalog.CreateBranch(ctx, repository, body.Name, body.Source)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(w, commitLog.Reference)
}

func (c *Controller) DeleteBranch(w http.ResponseWriter, r *http.Request, repository string, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_branch")
	err := c.Catalog.DeleteBranch(ctx, repository, branch)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetBranch(w http.ResponseWriter, r *http.Request, repository string, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_branch")
	reference, err := c.Catalog.GetBranchReference(ctx, repository, branch)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := Ref{
		CommitId: reference,
		Id:       branch,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) handleAPIError(ctx context.Context, w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, catalog.ErrNotFound),
		errors.Is(err, graveler.ErrNotFound),
		errors.Is(err, actions.ErrNotFound),
		errors.Is(err, auth.ErrNotFound),
		errors.Is(err, kv.ErrNotFound),
		errors.Is(err, db.ErrNotFound):
		writeError(w, http.StatusNotFound, err)

	case errors.Is(err, graveler.ErrDirtyBranch),
		errors.Is(err, graveler.ErrCommitMetaRangeDirtyBranch),
		errors.Is(err, catalog.ErrNoDifferenceWasFound),
		errors.Is(err, graveler.ErrNoChanges),
		errors.Is(err, permissions.ErrInvalidServiceName),
		errors.Is(err, permissions.ErrInvalidAction),
		errors.Is(err, model.ErrValidationError),
		errors.Is(err, graveler.ErrInvalidRef),
		errors.Is(err, graveler.ErrInvalidValue),
		errors.Is(err, actions.ErrParamConflict):
		writeError(w, http.StatusBadRequest, err)

	case errors.Is(err, graveler.ErrNotUnique),
		errors.Is(err, graveler.ErrConflictFound):
		writeError(w, http.StatusConflict, err)

	case errors.Is(err, catalog.ErrFeatureNotSupported):
		writeError(w, http.StatusNotImplemented, err)

	case errors.Is(err, graveler.ErrLockNotAcquired):
		writeError(w, http.StatusInternalServerError, "branch is currently locked, try again later")

	case errors.Is(err, adapter.ErrDataNotFound):
		writeError(w, http.StatusGone, "No data")

	case errors.Is(err, db.ErrAlreadyExists):
		writeError(w, http.StatusBadRequest, "Already exists")

	case err != nil:
		c.Logger.WithContext(ctx).WithError(err).Error("API call returned status internal server error")
		writeError(w, http.StatusInternalServerError, err)

	default:
		return false
	}

	return true
}

func (c *Controller) ResetBranch(w http.ResponseWriter, r *http.Request, body ResetBranchJSONRequestBody, repository string, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RevertBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "reset_branch")

	var err error
	switch body.Type {
	case entryTypeCommonPrefix:
		err = c.Catalog.ResetEntries(ctx, repository, branch, StringValue(body.Path))
	case "reset":
		err = c.Catalog.ResetBranch(ctx, repository, branch)
	case entryTypeObject:
		err = c.Catalog.ResetEntry(ctx, repository, branch, StringValue(body.Path))
	default:
		writeError(w, http.StatusNotFound, "reset type not found")
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) IngestRange(w http.ResponseWriter, r *http.Request, body IngestRangeJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ImportFromStorage,
					Resource: permissions.StorageNamespace(body.FromSourceURI),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.WriteObjectAction,
					Resource: permissions.ObjectArn(repository, body.Prepend),
				},
			},
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "ingest_range")

	contToken := swag.StringValue(body.ContinuationToken)
	info, mark, err := c.Catalog.WriteRange(r.Context(), repository, body.FromSourceURI, body.Prepend, body.After, contToken)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	writeResponse(w, http.StatusCreated, IngestRangeCreationResponse{
		Range: &RangeMetadata{
			Id:            string(info.ID),
			MinKey:        string(info.MinKey),
			MaxKey:        string(info.MaxKey),
			Count:         info.Count,
			EstimatedSize: int(info.EstimatedRangeSizeBytes),
		},
		Pagination: &ImportPagination{
			HasMore:           mark.HasMore,
			ContinuationToken: &mark.ContinuationToken,
			LastKey:           mark.LastKey,
		},
	})
}

func (c *Controller) CreateMetaRange(w http.ResponseWriter, r *http.Request, body CreateMetaRangeJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateMetaRangeAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "create_metarange")

	ranges := make([]*graveler.RangeInfo, 0, len(body.Ranges))
	for _, r := range body.Ranges {
		ranges = append(ranges, &graveler.RangeInfo{
			ID:                      graveler.RangeID(r.Id),
			MinKey:                  graveler.Key(r.MinKey),
			MaxKey:                  graveler.Key(r.MaxKey),
			Count:                   r.Count,
			EstimatedRangeSizeBytes: uint64(r.EstimatedSize),
		})
	}
	info, err := c.Catalog.WriteMetaRange(r.Context(), repository, ranges)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusCreated, MetaRangeCreationResponse{
		Id: StringPtr(string(info.ID)),
	})
}

func (c *Controller) Commit(w http.ResponseWriter, r *http.Request, body CommitJSONRequestBody, repository string, branch string, params CommitParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCommitAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_commit")
	user, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing user")
		return
	}
	var metadata map[string]string
	if body.Metadata != nil {
		metadata = body.Metadata.AdditionalProperties
	}
	committer := user.Username
	newCommit, err := c.Catalog.Commit(ctx, repository, branch, body.Message, committer, metadata, body.Date, params.SourceMetarange)
	var hookAbortErr *graveler.HookAbortError
	if errors.As(err, &hookAbortErr) {
		c.Logger.
			WithError(err).
			WithField("run_id", hookAbortErr.RunID).
			Warn("aborted by hooks")
		writeError(w, http.StatusPreconditionFailed, err)
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	newMetadata := Commit_Metadata{
		AdditionalProperties: map[string]string(newCommit.Metadata),
	}
	response := Commit{
		Committer:    newCommit.Committer,
		CreationDate: newCommit.CreationDate.Unix(),
		Id:           newCommit.Reference,
		Message:      newCommit.Message,
		MetaRangeId:  newCommit.MetaRangeID,
		Metadata:     &newMetadata,
		Parents:      newCommit.Parents,
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) DiffBranch(w http.ResponseWriter, r *http.Request, repository string, branch string, params DiffBranchParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "diff_workspace")

	diff, hasMore, err := c.Catalog.DiffUncommitted(
		ctx,
		repository,
		branch,
		paginationPrefix(params.Prefix),
		paginationDelimiter(params.Delimiter),
		paginationAmount(params.Amount),
		paginationAfter(params.After),
	)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	results := make([]Diff, 0, len(diff))
	for _, d := range diff {
		pathType := entryTypeObject
		if d.CommonLevel {
			pathType = entryTypeCommonPrefix
		}
		diff := Diff{
			Path:     d.Path,
			Type:     transformDifferenceTypeToString(d.Type),
			PathType: pathType,
		}
		if !d.CommonLevel {
			diff.SizeBytes = &d.Size
		}
		results = append(results, diff)
	}
	response := DiffList{
		Pagination: paginationFor(hasMore, results, "Path"),
		Results:    results,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) DeleteObject(w http.ResponseWriter, r *http.Request, repository string, branch string, params DeleteObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_object")

	err := c.Catalog.DeleteEntry(ctx, repository, branch, params.Path)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) UploadObject(w http.ResponseWriter, r *http.Request, repository string, branch string, params UploadObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "put_object")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// check if branch exists - it is still a possibility, but we don't want to upload large object when the branch was not there in the first place
	branchExists, err := c.Catalog.BranchExists(ctx, repository, branch)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	if !branchExists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("branch '%s' not found", branch))
		return
	}

	// before writing body, ensure preconditions - this means we essentially check for object existence twice:
	// once before uploading the body to save resources and time,
	//	and then graveler will check again when passed a WriteCondition.
	allowOverwrite := true
	if params.IfNoneMatch != nil {
		if StringValue(params.IfNoneMatch) != "*" {
			writeError(w, http.StatusBadRequest, "Unsupported value for If-None-Match - Only \"*\" is supported")
			return
		}
		// check if exists
		_, err := c.Catalog.GetEntry(ctx, repo.Name, branch, params.Path, catalog.GetEntryParams{ReturnExpired: true})
		if err == nil {
			writeError(w, http.StatusPreconditionFailed, "path already exists")
			return
		}
		if !errors.Is(err, catalog.ErrNotFound) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		allowOverwrite = false
	}

	// write the content
	file, handler, err := r.FormFile("content")
	if errors.Is(err, http.ErrMissingFile) {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("multipart uploads missing key 'content': %w", err))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer func() { _ = file.Close() }()
	contentType := handler.Header.Get("Content-Type")
	blob, err := upload.WriteBlob(ctx, c.BlockAdapter, repo.StorageNamespace, file, handler.Size, block.PutOpts{StorageClass: params.StorageClass})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// write metadata
	writeTime := time.Now()
	entryBuilder := catalog.NewDBEntryBuilder().
		Path(params.Path).
		PhysicalAddress(blob.PhysicalAddress).
		CreationDate(writeTime).
		Size(blob.Size).
		Checksum(blob.Checksum).
		ContentType(contentType)
	if blob.RelativePath {
		entryBuilder.AddressType(catalog.AddressTypeRelative)
	} else {
		entryBuilder.AddressType(catalog.AddressTypeFull)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry, graveler.IfAbsent(!allowOverwrite))
	if errors.Is(err, graveler.ErrPreconditionFailed) {
		writeError(w, http.StatusPreconditionFailed, "path already exists")
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}

	identifierType := block.IdentifierTypeFull
	if blob.RelativePath {
		identifierType = block.IdentifierTypeRelative
	}

	qk, err := block.ResolveNamespace(repo.StorageNamespace, blob.PhysicalAddress, identifierType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := ObjectStats{
		Checksum:        blob.Checksum,
		Mtime:           writeTime.Unix(),
		Path:            params.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       Int64Ptr(blob.Size),
		ContentType:     &contentType,
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) StageObject(w http.ResponseWriter, r *http.Request, body StageObjectJSONRequestBody, repository string, branch string, params StageObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "stage_object")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	// write metadata
	qk, err := block.ResolveNamespace(repo.StorageNamespace, body.PhysicalAddress, block.IdentifierTypeFull)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// see what storage type this is and whether it fits our configuration
	uriRegex := c.BlockAdapter.GetStorageNamespaceInfo().ValidityRegex
	if match, err := regexp.MatchString(uriRegex, body.PhysicalAddress); err != nil || !match {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("physical address is not valid for block adapter: %s",
			c.BlockAdapter.BlockstoreType(),
		))
		return
	}

	// take mtime from request, if any
	writeTime := time.Now()
	if body.Mtime != nil {
		writeTime = time.Unix(*body.Mtime, 0)
	}

	entryBuilder := catalog.NewDBEntryBuilder().
		CommonLevel(false).
		Path(params.Path).
		PhysicalAddress(body.PhysicalAddress).
		AddressType(catalog.AddressTypeFull).
		CreationDate(writeTime).
		Size(body.SizeBytes).
		Checksum(body.Checksum).
		ContentType(StringValue(body.ContentType))
	if body.Metadata != nil {
		entryBuilder.Metadata(body.Metadata.AdditionalProperties)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := ObjectStats{
		Checksum:        entry.Checksum,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       Int64Ptr(entry.Size),
		ContentType:     &entry.ContentType,
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) RevertBranch(w http.ResponseWriter, r *http.Request, body RevertBranchJSONRequestBody, repository string, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RevertBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "revert_branch")
	user, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not found")
		return
	}
	committer := user.Username
	err := c.Catalog.Revert(ctx, repository, branch, catalog.RevertParams{
		Reference:    body.Ref,
		Committer:    committer,
		ParentNumber: body.ParentNumber,
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetCommit(w http.ResponseWriter, r *http.Request, repository string, commitID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadCommitAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_commit")
	commit, err := c.Catalog.GetCommit(ctx, repository, commitID)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "commit not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	metadata := Commit_Metadata{
		AdditionalProperties: map[string]string(commit.Metadata),
	}
	response := Commit{
		Committer:    commit.Committer,
		CreationDate: commit.CreationDate.Unix(),
		Id:           commitID,
		Message:      commit.Message,
		MetaRangeId:  commit.MetaRangeID,
		Metadata:     &metadata,
		Parents:      commit.Parents,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetGarbageCollectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.GetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules, err := c.Catalog.GetGarbageCollectionRules(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	resp := GarbageCollectionRules{}
	resp.DefaultRetentionDays = int(rules.DefaultRetentionDays)
	for branchID, retentionDays := range rules.BranchRetentionDays {
		resp.Branches = append(resp.Branches, GarbageCollectionRule{BranchId: branchID, RetentionDays: int(retentionDays)})
	}
	writeResponse(w, http.StatusOK, resp)
}

func (c *Controller) SetGarbageCollectionRules(w http.ResponseWriter, r *http.Request, body SetGarbageCollectionRulesJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules := &graveler.GarbageCollectionRules{
		DefaultRetentionDays: int32(body.DefaultRetentionDays),
		BranchRetentionDays:  make(map[string]int32),
	}
	for _, rule := range body.Branches {
		rules.BranchRetentionDays[rule.BranchId] = int32(rule.RetentionDays)
	}
	err := c.Catalog.SetGarbageCollectionRules(ctx, repository, rules)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) PrepareGarbageCollectionCommits(w http.ResponseWriter, r *http.Request, body PrepareGarbageCollectionCommitsJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.PrepareGarbageCollectionCommitsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "prepare_garbage_collection_commits")
	gcRUnMetadata, err := c.Catalog.PrepareExpiredCommits(ctx, repository, swag.StringValue(body.PreviousRunId))
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusCreated, GarbageCollectionPrepareResponse{
		GcCommitsLocation:   gcRUnMetadata.CommitsCsvLocation,
		GcAddressesLocation: gcRUnMetadata.AddressLocation,
		RunId:               gcRUnMetadata.RunId,
	})
}

func (c *Controller) GetBranchProtectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.GetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules, err := c.Catalog.GetBranchProtectionRules(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	resp := make([]*BranchProtectionRule, 0, len(rules.BranchPatternToBlockedActions))
	for pattern := range rules.BranchPatternToBlockedActions {
		resp = append(resp, &BranchProtectionRule{
			Pattern: pattern,
		})
	}
	writeResponse(w, http.StatusOK, resp)
}

func (c *Controller) DeleteBranchProtectionRule(w http.ResponseWriter, r *http.Request, body DeleteBranchProtectionRuleJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	err := c.Catalog.DeleteBranchProtectionRule(ctx, repository, body.Pattern)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) CreateBranchProtectionRule(w http.ResponseWriter, r *http.Request, body CreateBranchProtectionRuleJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	// For now, all protected branches use the same default set of blocked actions. In the future this set will be user configurable.
	blockedActions := []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE, graveler.BranchProtectionBlockedAction_COMMIT}
	err := c.Catalog.CreateBranchProtectionRule(ctx, repository, body.Pattern, blockedActions)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetMetaRange(w http.ResponseWriter, r *http.Request, repository string, metaRange string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadRepositoryAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "metadata_get_metarange")

	metarange, err := c.Catalog.GetMetaRange(ctx, repository, metaRange)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := StorageURI{
		Location: string(metarange),
	}
	w.Header().Set("Location", string(metarange))
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetRange(w http.ResponseWriter, r *http.Request, repository string, pRange string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadRepositoryAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "metadata_get_range")

	rng, err := c.Catalog.GetRange(ctx, repository, pRange)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := StorageURI{
		Location: string(rng),
	}
	w.Header().Set("Location", string(rng))
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) DumpRefs(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListTagsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListBranchesAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListCommitsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "dump_repository_refs")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// dump all types:
	tagsID, err := c.Catalog.DumpTags(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	branchesID, err := c.Catalog.DumpBranches(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	commitsID, err := c.Catalog.DumpCommits(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	response := RefsDump{
		BranchesMetaRangeId: branchesID,
		CommitsMetaRangeId:  commitsID,
		TagsMetaRangeId:     tagsID,
	}

	// write this to the block store
	manifestBytes, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	err = c.BlockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       fmt.Sprintf("%s/refs_manifest.json", c.Config.GetCommittedBlockStoragePrefix()),
	}, int64(len(manifestBytes)), bytes.NewReader(manifestBytes), block.PutOpts{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) RestoreRefs(w http.ResponseWriter, r *http.Request, body RestoreRefsJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateTagAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateBranchAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "restore_repository_refs")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// ensure no refs currently found
	_, _, err = c.Catalog.ListCommits(ctx, repo.Name, repo.DefaultBranch, catalog.LogParams{
		PathList:      make([]catalog.PathRecord, 0),
		FromReference: "",
		Amount:        1,
	})
	if !errors.Is(err, graveler.ErrNotFound) {
		writeError(w, http.StatusBadRequest, "can only restore into a bare repository")
		return
	}

	// load commits
	err = c.Catalog.LoadCommits(ctx, repo.Name, body.CommitsMetaRangeId)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	err = c.Catalog.LoadBranches(ctx, repo.Name, body.BranchesMetaRangeId)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	err = c.Catalog.LoadTags(ctx, repo.Name, body.TagsMetaRangeId)
	if c.handleAPIError(ctx, w, err) {
		return
	}
}

func (c *Controller) CreateSymlinkFile(w http.ResponseWriter, r *http.Request, repository string, branch string, params CreateSymlinkFileParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_symlink")

	// read repo
	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// list entries
	var currentPath string
	var currentAddresses []string
	var after string
	var entries []*catalog.DBEntry
	hasMore := true
	for hasMore {
		entries, hasMore, err = c.Catalog.ListEntries(
			ctx,
			repository,
			branch,
			StringValue(params.Location),
			after,
			"",
			-1)
		if c.handleAPIError(ctx, w, err) {
			return
		}
		// loop all entries enter to map[path] physicalAddress
		for _, entry := range entries {
			qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
			if err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("error while resolving address: %s", err))
				return
			}
			idx := strings.LastIndex(entry.Path, "/")
			var path string
			if idx != -1 {
				path = entry.Path[0:idx]
			}
			if path != currentPath {
				// push current
				err := writeSymlink(ctx, repo, branch, path, currentAddresses, c.BlockAdapter)
				if err != nil {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("error while writing symlinks: %s", err))
					return
				}
				currentPath = path
				currentAddresses = []string{qk.Format()}
			} else {
				currentAddresses = append(currentAddresses, qk.Format())
			}
		}
		after = entries[len(entries)-1].Path
	}
	if len(currentAddresses) > 0 {
		err = writeSymlink(ctx, repo, branch, currentPath, currentAddresses, c.BlockAdapter)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("error while writing symlinks: %s", err))
			return
		}
	}
	metaLocation := fmt.Sprintf("%s/%s", repo.StorageNamespace, lakeFSPrefix)
	response := StorageURI{
		Location: metaLocation,
	}
	writeResponse(w, http.StatusCreated, response)
}

func writeSymlink(ctx context.Context, repo *catalog.Repository, branch string, path string, addresses []string, adapter block.Adapter) error {
	address := fmt.Sprintf("%s/%s/%s/%s/symlink.txt", lakeFSPrefix, repo.Name, branch, path)
	data := strings.Join(addresses, "\n")
	symlinkReader := aws.ReadSeekCloser(strings.NewReader(data))
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       address,
	}, int64(len(data)), symlinkReader, block.PutOpts{})
	return err
}

func (c *Controller) DiffRefs(w http.ResponseWriter, r *http.Request, repository string, leftRef string, rightRef string, params DiffRefsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "diff_refs")
	diffFunc := c.Catalog.Compare // default diff type is three-dot
	if params.Type != nil && *params.Type == "two_dot" {
		diffFunc = c.Catalog.Diff
	}

	diff, hasMore, err := diffFunc(ctx, repository, leftRef, rightRef, catalog.DiffParams{
		Limit:            paginationAmount(params.Amount),
		After:            paginationAfter(params.After),
		Prefix:           paginationPrefix(params.Prefix),
		Delimiter:        paginationDelimiter(params.Delimiter),
		AdditionalFields: nil,
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}
	results := make([]Diff, 0, len(diff))
	for _, d := range diff {
		pathType := entryTypeObject
		if d.CommonLevel {
			pathType = entryTypeCommonPrefix
		}
		diff := Diff{
			Path:     d.Path,
			Type:     transformDifferenceTypeToString(d.Type),
			PathType: pathType,
		}
		if !d.CommonLevel {
			diff.SizeBytes = &d.Size
		}
		results = append(results, diff)
	}
	response := DiffList{
		Pagination: paginationFor(hasMore, results, "Path"),
		Results:    results,
	}
	writeResponse(w, http.StatusOK, response)
}

// LogBranchCommits deprecated replaced by LogCommits
func (c *Controller) LogBranchCommits(w http.ResponseWriter, r *http.Request, repository string, branch string, params LogBranchCommitsParams) {
	c.logCommitsHelper(w, r, repository, branch, LogCommitsParams{
		After:  params.After,
		Amount: params.Amount,
	})
}

func (c *Controller) LogCommits(w http.ResponseWriter, r *http.Request, repository string, ref string, params LogCommitsParams) {
	c.logCommitsHelper(w, r, repository, ref, params)
}

func (c *Controller) logCommitsHelper(w http.ResponseWriter, r *http.Request, repository string, ref string, params LogCommitsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadBranchAction,
			Resource: permissions.BranchArn(repository, ref),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_branch_commit_log")

	// get commit log
	commitLog, hasMore, err := c.Catalog.ListCommits(ctx, repository, ref, catalog.LogParams{
		PathList:      resolvePathList(params.Objects, params.Prefixes),
		FromReference: paginationAfter(params.After),
		Amount:        paginationAmount(params.Amount),
		Limit:         swag.BoolValue(params.Limit),
	})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	serializedCommits := make([]Commit, 0, len(commitLog))
	for _, commit := range commitLog {
		metadata := Commit_Metadata{
			AdditionalProperties: commit.Metadata,
		}
		serializedCommits = append(serializedCommits, Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			Id:           commit.Reference,
			Message:      commit.Message,
			Metadata:     &metadata,
			MetaRangeId:  commit.MetaRangeID,
			Parents:      commit.Parents,
		})
	}

	response := CommitList{
		Pagination: paginationFor(hasMore, serializedCommits, "Id"),
		Results:    serializedCommits,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetObject(w http.ResponseWriter, r *http.Request, repository string, ref string, params GetObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_object")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// read the FS entry
	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{ReturnExpired: true})
	if c.handleAPIError(ctx, w, err) {
		return
	}
	c.Logger.Tracef("get repo %s ref %s path %s: %+v", repository, ref, params.Path, entry)
	if entry.Expired {
		writeError(w, http.StatusGone, "resource expired")
		return
	}

	// setup response
	reader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: entry.PhysicalAddress}, entry.Size)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	defer func() {
		_ = reader.Close()
	}()
	w.Header().Set("Content-Length", fmt.Sprint(entry.Size))
	etag := httputil.ETag(entry.Checksum)
	w.Header().Set("ETag", etag)
	lastModified := httputil.HeaderTimestamp(entry.CreationDate)
	w.Header().Set("Last-Modified", lastModified)
	cd := mime.FormatMediaType("attachment", map[string]string{"filename": filepath.Base(entry.Path)})
	w.Header().Set("Content-Disposition", cd)
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, reader)
	if err != nil {
		c.Logger.
			WithError(err).
			WithFields(logging.Fields{
				"storage_namespace": repo.StorageNamespace,
				"physical_address":  entry.PhysicalAddress,
			}).
			Debug("GetObject copy content")
	}
}

func (c *Controller) ListObjects(w http.ResponseWriter, r *http.Request, repository string, ref string, params ListObjectsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_objects")

	res, hasMore, err := c.Catalog.ListEntries(
		ctx,
		repository,
		ref,
		paginationPrefix(params.Prefix),
		paginationAfter(params.After),
		paginationDelimiter(params.Delimiter),
		paginationAmount(params.Amount),
	)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	objList := make([]ObjectStats, 0, len(res))
	for _, entry := range res {
		qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if entry.CommonLevel {
			objList = append(objList, ObjectStats{
				Path:     entry.Path,
				PathType: entryTypeCommonPrefix,
			})
		} else {
			var mtime int64
			if !entry.CreationDate.IsZero() {
				mtime = entry.CreationDate.Unix()
			}
			objStat := ObjectStats{
				Checksum:        entry.Checksum,
				Mtime:           mtime,
				Path:            entry.Path,
				PhysicalAddress: qk.Format(),
				PathType:        entryTypeObject,
				SizeBytes:       Int64Ptr(entry.Size),
				ContentType:     &entry.ContentType,
			}
			if (params.UserMetadata == nil || *params.UserMetadata) && entry.Metadata != nil {
				objStat.Metadata = &ObjectUserMetadata{AdditionalProperties: entry.Metadata}
			}
			objList = append(objList, objStat)
		}
	}
	response := ObjectStatsList{
		Pagination: Pagination{
			HasMore:    hasMore,
			MaxPerPage: DefaultMaxPerPage,
			Results:    len(objList),
		},
		Results: objList,
	}
	if len(objList) > 0 && hasMore {
		lastObj := objList[len(objList)-1]
		response.Pagination.NextOffset = lastObj.Path
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) StatObject(w http.ResponseWriter, r *http.Request, repository string, ref string, params StatObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "stat_object")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{ReturnExpired: true})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
	if c.handleAPIError(ctx, w, err) {
		return
	}

	objStat := ObjectStats{
		Checksum:        entry.Checksum,
		Mtime:           entry.CreationDate.Unix(),
		Path:            params.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       Int64Ptr(entry.Size),
		ContentType:     &entry.ContentType,
	}
	if (params.UserMetadata == nil || *params.UserMetadata) && entry.Metadata != nil {
		objStat.Metadata = &ObjectUserMetadata{AdditionalProperties: entry.Metadata}
	}
	code := http.StatusOK
	if entry.Expired {
		code = http.StatusGone
	}
	writeResponse(w, code, objStat)
}

func (c *Controller) GetUnderlyingProperties(w http.ResponseWriter, r *http.Request, repository string, ref string, params GetUnderlyingPropertiesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "object_underlying_properties")

	// read repo
	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, err) {
		return
	}

	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{})
	if c.handleAPIError(ctx, w, err) {
		return
	}

	// read object properties from underlying storage
	properties, err := c.BlockAdapter.GetProperties(ctx, block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: entry.PhysicalAddress})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := UnderlyingObjectProperties{
		StorageClass: properties.StorageClass,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) MergeIntoBranch(w http.ResponseWriter, r *http.Request, body MergeIntoBranchJSONRequestBody, repository string, sourceRef string, destinationBranch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCommitAction,
			Resource: permissions.BranchArn(repository, destinationBranch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "merge_branches")
	user, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not found")
		return
	}
	var metadata map[string]string
	if body.Metadata != nil {
		metadata = body.Metadata.AdditionalProperties
	}
	res, err := c.Catalog.Merge(ctx,
		repository, destinationBranch, sourceRef,
		user.Username,
		StringValue(body.Message),
		metadata,
		StringValue(body.Strategy))

	var hookAbortErr *graveler.HookAbortError
	switch {
	case errors.As(err, &hookAbortErr):
		c.Logger.WithError(err).WithField("run_id", hookAbortErr.RunID).Warn("aborted by hooks")
		writeError(w, http.StatusPreconditionFailed, err)
		return
	case errors.Is(err, catalog.ErrConflictFound) || errors.Is(err, graveler.ErrConflictFound):
		writeResponse(w, http.StatusConflict, MergeResult{Reference: res})
		return
	}
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusOK, MergeResult{Reference: res})
}

func (c *Controller) ListTags(w http.ResponseWriter, r *http.Request, repository string, params ListTagsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListTagsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_tags")

	res, hasMore, err := c.Catalog.ListTags(ctx, repository, paginationPrefix(params.Prefix), paginationAmount(params.Amount), paginationAfter(params.After))
	if c.handleAPIError(ctx, w, err) {
		return
	}

	results := make([]Ref, 0, len(res))
	for _, tag := range res {
		results = append(results, Ref{
			CommitId: tag.CommitID,
			Id:       tag.ID,
		})
	}
	response := RefList{
		Results:    results,
		Pagination: paginationFor(hasMore, results, "Id"),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) CreateTag(w http.ResponseWriter, r *http.Request, body CreateTagJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateTagAction,
			Resource: permissions.TagArn(repository, body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_tag")

	commitID, err := c.Catalog.CreateTag(ctx, repository, body.Id, body.Ref)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := Ref{
		CommitId: commitID,
		Id:       body.Id,
	}
	writeResponse(w, http.StatusCreated, response)
}

func (c *Controller) DeleteTag(w http.ResponseWriter, r *http.Request, repository string, tag string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteTagAction,
			Resource: permissions.TagArn(repository, tag),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_tag")
	err := c.Catalog.DeleteTag(ctx, repository, tag)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	writeResponse(w, http.StatusNoContent, nil)
}

func (c *Controller) GetTag(w http.ResponseWriter, r *http.Request, repository string, tag string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadTagAction,
			Resource: permissions.TagArn(repository, tag),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_tag")
	reference, err := c.Catalog.GetTag(ctx, repository, tag)
	if c.handleAPIError(ctx, w, err) {
		return
	}
	response := Ref{
		CommitId: reference,
		Id:       tag,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetSetupState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	initialized, err := c.MetadataManager.IsInitialized(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	state := setupStateNotInitialized
	if initialized || c.Config.IsAuthTypeAPI() {
		state = setupStateInitialized
	}
	oidcConfig := c.Config.GetAuthOIDCConfiguration()
	response := SetupState{
		State:            swag.String(state),
		OidcEnabled:      swag.Bool(oidcConfig.Enabled),
		OidcDefaultLogin: swag.Bool(oidcConfig.IsDefaultLogin),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) Setup(w http.ResponseWriter, r *http.Request, body SetupJSONRequestBody) {
	if len(body.Username) == 0 {
		writeError(w, http.StatusBadRequest, "empty user display name")
		return
	}

	// check if previous setup completed
	ctx := r.Context()
	initialized, err := c.MetadataManager.IsInitialized(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if initialized || c.Config.IsAuthTypeAPI() {
		writeError(w, http.StatusConflict, "lakeFS already initialized")
		return
	}

	// migrate the database if needed
	err = c.Migrator.Migrate(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var cred *model.Credential
	if body.Key == nil {
		cred, err = auth.CreateInitialAdminUser(ctx, c.Auth, c.MetadataManager, body.Username)
	} else {
		cred, err = auth.CreateInitialAdminUserWithKeys(ctx, c.Auth, c.MetadataManager, body.Username, &body.Key.AccessKeyId, &body.Key.SecretAccessKey)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	meta := stats.NewMetadata(ctx, c.Logger, c.BlockAdapter.BlockstoreType(), c.MetadataManager, c.CloudMetadataProvider)
	c.Collector.SetInstallationID(meta.InstallationID)
	c.Collector.CollectMetadata(meta)
	c.Collector.CollectEvent("global", "init")

	response := CredentialsWithSecret{
		AccessKeyId:     cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey,
		CreationDate:    cred.IssuedDate.Unix(),
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) GetCurrentUser(w http.ResponseWriter, r *http.Request) {
	u, ok := r.Context().Value(UserContextKey).(*model.User)
	var user User
	if ok {
		user.Id = u.Username
		user.CreationDate = u.CreatedAt.Unix()
		if u.FriendlyName != nil {
			user.FriendlyName = u.FriendlyName
		} else {
			user.FriendlyName = &u.Username
		}
	}
	response := CurrentUser{
		User: user,
	}
	writeResponse(w, http.StatusOK, response)
}

func (c *Controller) resetPasswordRequest(ctx context.Context, emailAddr string) error {
	user, err := c.Auth.GetUserByEmail(ctx, emailAddr)
	if err != nil {
		return err
	}
	emailAddr = StringValue(user.Email)
	token, err := c.generateResetPasswordToken(emailAddr, DefaultResetPasswordExpiration)
	if err != nil {
		c.Logger.WithError(err).WithField("email_address", emailAddr).Error("reset password - failed generating token")
		return err
	}
	params := map[string]string{
		"token": token,
	}
	err = c.Emailer.SendResetPasswordEmail([]string{emailAddr}, params)
	if err != nil {
		c.Logger.WithError(err).WithField("email_address", emailAddr).Error("reset password - failed sending email")
		return err
	}
	c.Logger.WithField("email", emailAddr).Info("reset password email sent")
	return nil
}

func (c *Controller) ForgotPassword(w http.ResponseWriter, r *http.Request, body ForgotPasswordJSONRequestBody) {
	addr, err := mail.ParseAddress(body.Email)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid email")
		return
	}
	err = c.resetPasswordRequest(r.Context(), addr.Address)
	if err != nil {
		c.Logger.WithError(err).WithField("email", body.Email).Debug("failed sending reset password email")
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *Controller) UpdatePassword(w http.ResponseWriter, r *http.Request, body UpdatePasswordJSONRequestBody) {
	claims, err := VerifyResetPasswordToken(r.Context(), c.Auth, body.Token)
	if err != nil {
		c.Logger.WithError(err).WithField("token", body.Token).Debug("failed to verify token")
		writeError(w, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}

	// verify provided email matched the token
	requestEmail := StringValue(body.Email)
	if requestEmail != "" && requestEmail != claims.Subject {
		c.Logger.WithError(err).WithFields(logging.Fields{
			"token":         body.Token,
			"request_email": requestEmail,
		}).Debug("requested email doesn't match the email provided in verified token")
	}

	user, err := c.Auth.GetUserByEmail(r.Context(), claims.Subject)
	if err != nil {
		c.Logger.WithError(err).WithField("email", claims.Subject).Warn("failed to retrieve user by email")
		writeError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
		return
	}
	err = c.Auth.HashAndUpdatePassword(r.Context(), user.Username, body.NewPassword)
	if err != nil {
		c.Logger.WithError(err).WithField("username", user.Username).Debug("failed to update password")
		writeError(w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (c *Controller) ExpandTemplate(w http.ResponseWriter, r *http.Request, templateLocation string, p ExpandTemplateParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.TemplateArn(templateLocation),
		},
	}) {
		return
	}
	ctx := r.Context()

	u, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok {
		writeError(w, http.StatusInternalServerError, "request performed with no user")
	}

	// Override bug in OpenAPI generated code: parameters do not show up
	// in p.Params.AdditionalProperties, so force them in there.
	if len(p.Params.AdditionalProperties) == 0 && len(r.URL.Query()) > 0 {
		p.Params.AdditionalProperties = make(map[string]string, len(r.Header))
		for k, v := range r.URL.Query() {
			p.Params.AdditionalProperties[k] = v[0]
		}
	}

	c.LogAction(ctx, "expand_template")
	err := c.Templater.Expand(ctx, w, u, templateLocation, p.Params.AdditionalProperties)
	if err != nil {
		c.Logger.WithError(err).WithField("location", templateLocation).Error("Template expansion failed")
	}

	if errors.Is(err, templater.ErrNotAuthorized) {
		writeError(w, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
		return
	}
	if errors.Is(err, templater.ErrNotFound) {
		writeError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "expansion failed")
		return
	}
	// Response already written during expansion.
}

func (c *Controller) GetLakeFSVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok || user == nil {
		writeError(w, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}
	// set upgrade recommended based on last security audit check
	var (
		upgradeRecommended *bool
		upgradeURL         *string
	)
	lastCheck, _ := c.AuditChecker.LastCheck()
	if lastCheck != nil {
		recommendedURL := lastCheck.UpgradeRecommendedURL()
		if recommendedURL != "" {
			upgradeRecommended = swag.Bool(true)
			upgradeURL = swag.String(recommendedURL)
		}
	}

	writeResponse(w, http.StatusOK, VersionConfig{
		UpgradeRecommended: upgradeRecommended,
		UpgradeUrl:         upgradeURL,
		Version:            swag.String(version.Version),
	})
}

func IsStatusCodeOK(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}

func StringPtr(s string) *string {
	return &s
}

func StringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func Int64Value(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func Int64Ptr(n int64) *int64 {
	return &n
}

func PaginationAmountPtr(a int) *PaginationAmount {
	amount := PaginationAmount(a)
	return &amount
}

func PaginationAfterPtr(a string) *PaginationAfter {
	after := PaginationAfter(a)
	return &after
}

func writeError(w http.ResponseWriter, code int, v interface{}) {
	apiErr := Error{
		Message: fmt.Sprint(v),
	}
	writeResponse(w, code, apiErr)
}

func writeResponse(w http.ResponseWriter, code int, response interface{}) {
	if response == nil {
		w.WriteHeader(code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		logging.Default().WithError(err).WithField("code", code).Debug("Failed to write encoded json response")
	}
}

func paginationAfter(v *PaginationAfter) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationPrefix(v *PaginationPrefix) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationDelimiter(v *PaginationDelimiter) string {
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

func resolvePathList(objects *[]string, prefixes *[]string) []catalog.PathRecord {
	var pathRecords []catalog.PathRecord
	if objects == nil && prefixes == nil {
		return make([]catalog.PathRecord, 0)
	}
	if objects != nil {
		for _, path := range *objects {
			if path != "" {
				path := path
				pathRecords = append(pathRecords, catalog.PathRecord{
					Path:     catalog.Path(StringValue(&path)),
					IsPrefix: false,
				})
			}
		}
	}
	if prefixes != nil {
		for _, path := range *prefixes {
			if path != "" {
				path := path
				pathRecords = append(pathRecords, catalog.PathRecord{
					Path:     catalog.Path(StringValue(&path)),
					IsPrefix: true,
				})
			}
		}
	}
	return pathRecords
}

func NewController(
	cfg *config.Config,
	catalog catalog.Interface,
	authenticator auth.Authenticator,
	authService auth.Service,
	blockAdapter block.Adapter,
	metadataManager auth.MetadataManager,
	migrator db.Migrator,
	collector stats.Collector,
	cloudMetadataProvider cloud.MetadataProvider,
	actions actionsHandler,
	auditChecker AuditChecker,
	logger logging.Logger,
	emailer *email.Emailer,
	templater templater.Service,
	oidcAuthenticator *oidc.Authenticator,
	sessionStore sessions.Store,
) *Controller {
	gob.Register(oidc.Claims{})
	return &Controller{
		Config:                cfg,
		Catalog:               catalog,
		Authenticator:         authenticator,
		Auth:                  authService,
		BlockAdapter:          blockAdapter,
		MetadataManager:       metadataManager,
		Migrator:              migrator,
		Collector:             collector,
		CloudMetadataProvider: cloudMetadataProvider,
		Actions:               actions,
		AuditChecker:          auditChecker,
		Logger:                logger,
		Emailer:               emailer,
		Templater:             templater,
		sessionStore:          sessionStore,
		oidcAuthenticator:     oidcAuthenticator,
	}
}

func (c *Controller) LogAction(ctx context.Context, action string) {
	c.Logger.WithContext(ctx).
		WithField("action", action).
		WithField("message_type", "action").
		Debug("performing API action")
	c.Collector.CollectEvent("api_server", action)
}

func paginationFor(hasMore bool, results interface{}, fieldName string) Pagination {
	pagination := Pagination{
		HasMore:    hasMore,
		MaxPerPage: DefaultMaxPerPage,
	}
	if results == nil {
		return pagination
	}
	if reflect.TypeOf(results).Kind() != reflect.Slice {
		panic("results is not a slice")
	}
	s := reflect.ValueOf(results)
	pagination.Results = s.Len()
	if !hasMore || pagination.Results == 0 {
		return pagination
	}
	v := s.Index(pagination.Results - 1)
	token := v.FieldByName(fieldName)
	pagination.NextOffset = token.String()
	return pagination
}

func (c *Controller) authorize(w http.ResponseWriter, r *http.Request, perms permissions.Node) bool {
	ctx := r.Context()
	user, ok := ctx.Value(UserContextKey).(*model.User)
	if !ok || user == nil {
		writeError(w, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return false
	}
	resp, err := c.Auth.Authorize(ctx, &auth.AuthorizationRequest{
		Username:            user.Username,
		RequiredPermissions: perms,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return false
	}
	if resp.Error != nil {
		writeError(w, http.StatusUnauthorized, resp.Error)
		return false
	}
	if !resp.Allowed {
		writeError(w, http.StatusInternalServerError, "User does not have the required permissions")
		return false
	}
	return true
}
