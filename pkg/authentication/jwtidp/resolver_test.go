package jwtidp_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication/jwtidp"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// fakeAuthService is a narrow stub of auth.Service used by the
// resolver tests. Unused auth.Service methods are not implemented;
// the embedded nil interface will panic if ResolveUser is ever
// refactored to call a method we don't expect -- a clear signal to
// extend the fake, not silently accept unexpected calls.
// Matches the convention used by pkg/auth/remoteauthenticator's test.
type fakeAuthService struct {
	auth.Service
	usersByExternalID map[string]*model.User
	createErr         error
	// createRaceWinner, if non-nil and used in conjunction with
	// createErr=auth.ErrAlreadyExists, is inserted into
	// usersByExternalID by the CreateUser stub before it returns the
	// conflict. This simulates a concurrent login landing its own
	// CreateUser just before ours.
	createRaceWinner  *model.User
	updateFriendlyErr error
	friendlyUpdates   []friendlyUpdate
	groupsAdded       []groupCall
}

type groupCall struct{ user, group string }
type friendlyUpdate struct{ user, name string }

func (f *fakeAuthService) GetUserByExternalID(_ context.Context, externalID string) (*model.User, error) {
	if u, ok := f.usersByExternalID[externalID]; ok {
		return u, nil
	}
	return nil, auth.ErrNotFound
}

func (f *fakeAuthService) CreateUser(_ context.Context, user *model.User) (string, error) {
	if f.createErr != nil {
		if f.createRaceWinner != nil && f.createRaceWinner.ExternalID != nil {
			if f.usersByExternalID == nil {
				f.usersByExternalID = map[string]*model.User{}
			}
			f.usersByExternalID[*f.createRaceWinner.ExternalID] = f.createRaceWinner
		}
		return "", f.createErr
	}
	if f.usersByExternalID == nil {
		f.usersByExternalID = map[string]*model.User{}
	}
	if user.ExternalID != nil {
		f.usersByExternalID[*user.ExternalID] = user
	}
	return user.Username, nil
}

func (f *fakeAuthService) AddUserToGroup(_ context.Context, user, group string) error {
	f.groupsAdded = append(f.groupsAdded, groupCall{user: user, group: group})
	return nil
}

func (f *fakeAuthService) UpdateUserFriendlyName(_ context.Context, user, name string) error {
	if f.updateFriendlyErr != nil {
		return f.updateFriendlyErr
	}
	f.friendlyUpdates = append(f.friendlyUpdates, friendlyUpdate{user: user, name: name})
	return nil
}

func newFakeService() *fakeAuthService {
	return &fakeAuthService{usersByExternalID: map[string]*model.User{}}
}

func makeClaims(m map[string]any) jwtidp.Claims {
	sub, _ := m["sub"].(string)
	return jwtidp.Claims{Raw: m, Subject: sub}
}

func mustCompile(t *testing.T, expr string) *jmespath.JMESPath {
	t.Helper()
	p, err := jmespath.Compile(expr)
	require.NoError(t, err)
	return p
}

// safeGroupsExpr uses the JMESPath not_null(..) idiom so the
// expression tolerates a missing `groups` claim without erroring.
const safeGroupsExpr = "contains(not_null(groups, `[]`), 'ext-eng') && ['Developers'] || ['Viewers']"

func TestResolveUser_GroupsExpression_MapsByMembership(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{InitialGroups: mustCompile(t, safeGroupsExpr)}
	c := makeClaims(map[string]any{
		"sub":    "alice",
		"groups": []any{"ext-eng", "ext-ops"},
	})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, "alice", user.Username)
	require.Equal(t, "alice", swag.StringValue(user.ExternalID))
	require.Equal(t, "jwt", user.Source, "new JWT users should be labeled with Source=\"jwt\"")
	require.Equal(t, []groupCall{{user: "alice", group: "Developers"}}, svc.groupsAdded)
}

func TestResolveUser_GroupsExpression_BakedInDefaultAppliesWhenGroupsMissing(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{InitialGroups: mustCompile(t, safeGroupsExpr)}
	c := makeClaims(map[string]any{"sub": "bob"})

	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, []groupCall{{user: "bob", group: "Viewers"}}, svc.groupsAdded)
}

// TestResolveUser_GroupsExpression_AssignsNoGroups exercises the
// four ways evalInitialGroups yields an empty group set: no compiled
// expression, a JMESPath runtime error (e.g. contains() on null), an
// expression that resolves to null, and a list with a non-string
// element. All must provision the user without calling AddUserToGroup.
func TestResolveUser_GroupsExpression_AssignsNoGroups(t *testing.T) {
	cases := map[string]struct {
		expr   string // "" for no compiled expression
		claims map[string]any
	}{
		"no compiled expression": {
			expr:   "",
			claims: map[string]any{"sub": "frank"},
		},
		"expression errors at runtime": {
			// contains(null, ...) is a JMESPath type error.
			expr:   "contains(groups, 'ext-eng') && ['Developers']",
			claims: map[string]any{"sub": "dave"},
		},
		"expression returns null": {
			expr:   "contains(not_null(groups, `[]`), 'ext-eng') && ['Developers']",
			claims: map[string]any{"sub": "carol"},
		},
		"array contains non-string element": {
			expr:   "mixed",
			claims: map[string]any{"sub": "erin", "mixed": []any{"Viewers", 42}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			svc := newFakeService()
			cfg := &jwtidp.Config{}
			if tc.expr != "" {
				cfg.InitialGroups = mustCompile(t, tc.expr)
			}
			_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, makeClaims(tc.claims))
			require.NoError(t, err)
			require.Empty(t, svc.groupsAdded)
		})
	}
}

func TestResolveUser_GroupsExpression_StringResultWrappedInSlice(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{InitialGroups: mustCompile(t, "'Viewers'")}
	c := makeClaims(map[string]any{"sub": "dan"})

	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, []groupCall{{user: "dan", group: "Viewers"}}, svc.groupsAdded)
}

func TestResolveUser_ExistingUserSkipsGroupAssignment(t *testing.T) {
	svc := newFakeService()
	externalID := "grace"
	svc.usersByExternalID[externalID] = &model.User{
		Username:   externalID,
		Source:     "jwt",
		ExternalID: &externalID,
	}
	cfg := &jwtidp.Config{InitialGroups: mustCompile(t, "['Developers']")}
	c := makeClaims(map[string]any{"sub": externalID})

	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Empty(t, svc.groupsAdded, "groups should only be assigned on first provision")
}

func TestResolveUser_CustomExternalUserIDClaim(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{ExternalUserIDClaimRef: "/oid"}
	c := makeClaims(map[string]any{"sub": "ignored", "oid": "entra-123"})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, "entra-123", user.Username)
	require.Equal(t, "entra-123", swag.StringValue(user.ExternalID))
}

func TestResolveUser_MissingExternalIDClaimErrors(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{}
	c := makeClaims(map[string]any{"name": "anon"})

	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.ErrorIs(t, err, jwtidp.ErrClaimMissing)
}

func TestResolveUser_PersistFriendlyName(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{
		FriendlyNameClaimRef: "/name",
		PersistFriendlyName:  true,
	}
	c := makeClaims(map[string]any{"sub": "henry", "name": "Henry H"})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, "Henry H", swag.StringValue(user.FriendlyName))
	// Persist happens on a subsequent login when the friendly name changes.
	require.Empty(t, svc.friendlyUpdates)

	svc.usersByExternalID["henry"].FriendlyName = swag.String("old name")
	_, err = jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, []friendlyUpdate{{user: "henry", name: "Henry H"}}, svc.friendlyUpdates)
}

func TestResolveUser_PersistFriendlyNameDisabled(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{FriendlyNameClaimRef: "/name", PersistFriendlyName: false}
	c := makeClaims(map[string]any{"sub": "iris", "name": "Iris I"})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, "Iris I", swag.StringValue(user.FriendlyName))

	svc.usersByExternalID["iris"].FriendlyName = swag.String("stale")
	_, err = jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Empty(t, svc.friendlyUpdates)
}

func TestNewResolverConfig_InvalidExpressionFails(t *testing.T) {
	_, err := jwtidp.NewResolverConfig(&config.JWT{InitialGroupsPath: "this is not valid jmespath !!"})
	require.ErrorIs(t, err, jwtidp.ErrConfig)
}

// TestResolveUser_EmptyExternalIDRejected guards the `externalID == ""`
// branch of the external-id extraction: a token whose `sub` is the
// empty string must be rejected rather than provision an unnamed user.
func TestResolveUser_EmptyExternalIDRejected(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{}
	c := makeClaims(map[string]any{"sub": ""})

	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.ErrorIs(t, err, jwtidp.ErrClaimMissing)
	require.Empty(t, svc.usersByExternalID, "empty sub must not provision a user")
}

// TestResolveUser_CreateRaceFallsBackToExistingUser exercises the
// ErrAlreadyExists branch: two concurrent first logins for the same
// subject race on CreateUser. The loser re-fetches and resolves to
// the same user the winner created, without double-assigning groups.
func TestResolveUser_CreateRaceFallsBackToExistingUser(t *testing.T) {
	svc := newFakeService()
	existingID := "race-winner"
	existing := &model.User{
		Username:   existingID,
		Source:     "jwt",
		ExternalID: &existingID,
	}
	// CreateUser is invoked *after* our own GetUserByExternalID
	// returned NotFound. The stub writes `existing` to the map and
	// then returns ErrAlreadyExists, modeling the winner's CreateUser
	// landing in between -- so the resolver's post-conflict re-fetch
	// returns `existing`.
	svc.createErr = auth.ErrAlreadyExists
	svc.createRaceWinner = existing

	cfg := &jwtidp.Config{InitialGroups: mustCompile(t, "['Developers']")}
	c := makeClaims(map[string]any{"sub": existingID})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, existing, user)
	require.Empty(t, svc.groupsAdded, "race loser must not reassign groups already handled by the winner")
}

// TestResolveUser_NonStringFriendlyNameSwallowed verifies that a
// malformed friendly-name claim (wrong type) does not fail the
// login; the resolver logs and proceeds with an empty friendly name.
// Critical because the friendly name is cosmetic and an IdP emitting
// an unexpected type must not lock users out.
func TestResolveUser_NonStringFriendlyNameSwallowed(t *testing.T) {
	svc := newFakeService()
	cfg := &jwtidp.Config{FriendlyNameClaimRef: "/name", PersistFriendlyName: true}
	c := makeClaims(map[string]any{
		"sub":  "jack",
		"name": map[string]any{"given": "Jack"}, // object, not string
	})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err)
	require.Equal(t, "jack", user.Username)
	require.Empty(t, swag.StringValue(user.FriendlyName))
	require.Empty(t, svc.friendlyUpdates)
}

// TestResolveUser_PersistFriendlyNameUpdateFailureTolerated mirrors
// the contrib/auth/acl reality where UpdateUserFriendlyName returns
// ErrNotImplemented: the resolver must log and still return the
// in-memory user rather than fail the login. Without this test the
// "log and continue" branch in enhanceFriendlyName is dead code, and
// a refactor that returned the error would silently break logins
// against any backend that doesn't implement the persistence call.
func TestResolveUser_PersistFriendlyNameUpdateFailureTolerated(t *testing.T) {
	svc := newFakeService()
	svc.updateFriendlyErr = errors.New("simulated backend failure")
	cfg := &jwtidp.Config{FriendlyNameClaimRef: "/name", PersistFriendlyName: true}
	id := "kate"
	svc.usersByExternalID[id] = &model.User{
		Username:     id,
		Source:       "jwt",
		ExternalID:   &id,
		FriendlyName: swag.String("old name"),
	}
	c := makeClaims(map[string]any{"sub": id, "name": "Kate K"})

	user, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, cfg, c)
	require.NoError(t, err, "persistence failure must not fail the login")
	require.Equal(t, "Kate K", swag.StringValue(user.FriendlyName), "in-memory user still carries the fresh name")
	require.Empty(t, svc.friendlyUpdates, "failed update must not be recorded as applied")
}

func TestResolveUser_NilConfigRejected(t *testing.T) {
	svc := newFakeService()
	_, err := jwtidp.ResolveUser(t.Context(), logging.ContextUnavailable(), svc, nil, makeClaims(map[string]any{"sub": "x"}))
	require.ErrorIs(t, err, jwtidp.ErrConfig)
}
