package esti

import (
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"net/http"
	"testing"
)

func TestUpdatePassword(t *testing.T) {
	ctx, _, _ := setupTest(t)

	adminClient := client
	// creating a new user should succeed
	email := "test@treeverse.io"
	resCreateUser, err := adminClient.CreateUserWithResponse(ctx, api.CreateUserJSONRequestBody{
		Id: email,
	})
	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	newPassword := "Aa123456"
	resUpdatePassword, err := adminClient.UpdatePasswordWithResponse(ctx, api.UpdatePasswordJSONRequestBody{Email: &email, NewPassword: newPassword})

	require.NoError(t, err, "Admin failed to update user's password")
	require.Equal(t, http.StatusOK, resUpdatePassword.StatusCode(), "Admin unexpectedly failed to update user's password")

	resGetUser, err := adminClient.GetUserWithResponse(ctx, email)

	require.NoError(t, err, "Admin failed to get user")
	require.Equal(t, http.StatusOK, resGetUser.StatusCode(), "Admin unexpectedly failed to get user")
	require.Equal(t, resGetUser.Body[1], newPassword, "Admin unexpectedly failed to change user's password")
}
