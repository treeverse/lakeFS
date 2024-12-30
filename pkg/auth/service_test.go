package auth_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/mock"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const creationDate = 12345678

func TestMain(m *testing.M) {
	logging.SetLevel("panic")
	code := m.Run()
	os.Exit(code)
}

func TestAPIAuthService_GetUserById(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		userID             string
		userIntID          int64
		users              []string
		expectedUserName   string
		expectedErr        error
	}{
		{
			name:               "one_user",
			responseStatusCode: http.StatusOK,
			userID:             "1",
			userIntID:          1,
			users:              []string{"one"},
			expectedUserName:   "one",
			expectedErr:        nil,
		},
		{
			name:               "no_users",
			responseStatusCode: http.StatusOK,
			userID:             "2",
			userIntID:          2,
			users:              []string{},
			expectedUserName:   "",
			expectedErr:        auth.ErrNotFound,
		},
		{
			name:               "two_responses",
			responseStatusCode: http.StatusOK,
			userID:             "3",
			userIntID:          3,
			users:              []string{"one", "two"},
			expectedUserName:   "",
			expectedErr:        auth.ErrNonUnique,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnedUsers := make([]auth.User, len(tt.users))
			for i, u := range tt.users {
				returnedUsers[i] = auth.User{
					Username: u,
				}
			}
			returnedUserList := &auth.UserList{
				Pagination: auth.Pagination{},
				Results:    returnedUsers,
			}

			const amount = 2
			paginationAmount := auth.PaginationAmount(amount)
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(),
				gomock.Eq(&auth.ListUsersParams{Id: &tt.userIntID, Amount: &paginationAmount}),
			).Return(&auth.ListUsersResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200:     returnedUserList,
				JSON401:     nil,
				JSONDefault: nil,
			}, nil)

			ctx := context.Background()
			gotUser, err := s.GetUserByID(ctx, tt.userID)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUserById(%s): expected err: %v got: %v", tt.userID, tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if gotUser.Username != tt.expectedUserName {
				t.Fatalf("expected user id:%s, got:%s", tt.expectedUserName, gotUser.Username)
			}
		})
	}
}

func TestAuthAPIUserPoliciesCache(t *testing.T) {
	resPolicies := authPoliciesForTesting
	policyList := auth.PolicyList{
		Pagination: auth.Pagination{},
		Results:    resPolicies,
	}
	mockClient, s := NewTestApiService(t, true)
	response := &auth.ListUserPoliciesResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &policyList,
	}
	const username = "username"
	mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
	res1, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
	testutil.Must(t, err)
	res2, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
	testutil.Must(t, err)
	if diff := deep.Equal(res1, res2); diff != nil {
		t.Error("cache returned different result than api", diff)
	}
}

func TestAuthApiGetCredentialsCache(t *testing.T) {
	ctx := context.Background()
	mockClient, s := NewTestApiService(t, true)
	const username = "foo"
	accessKey := "ACCESS"
	secretKey := "SECRET"
	response := &auth.GetCredentialsResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusOK,
		},
		JSON200: &auth.CredentialsWithSecret{
			AccessKeyId:     accessKey,
			SecretAccessKey: secretKey,
		},
	}
	mockClient.EXPECT().GetCredentialsWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
	mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusOK,
		},
		JSON200: &auth.UserList{
			Pagination: auth.Pagination{},
			Results: []auth.User{{
				Username: username,
			}},
		},
	}, nil)

	res1, err := s.GetCredentials(ctx, accessKey)
	testutil.Must(t, err)
	res2, err := s.GetCredentials(ctx, accessKey)
	testutil.Must(t, err)
	if diff := deep.Equal(res1, res2); diff != nil {
		t.Error("cache returned different result than api", diff)
	}
}

func TestAuthApiGetUserCache(t *testing.T) {
	ctx := context.Background()
	mockClient, s := NewTestApiService(t, true)
	const userID = "123"

	const username = "foo"
	userMail := "foo@test.com"
	externalId := "1234"
	userResult := auth.User{
		Username:   username,
		Email:      &userMail,
		ExternalId: &externalId,
	}

	returnedUserList := &auth.UserList{
		Pagination: auth.Pagination{},
		Results: []auth.User{
			userResult,
		},
	}
	t.Run("get_user_by_id", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)

		res1, err := s.GetUserByID(ctx, userID)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByID(ctx, userID)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user_by_email", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)
		res1, err := s.GetUserByEmail(ctx, userMail)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByEmail(ctx, userMail)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user", func(t *testing.T) {
		mockClient.EXPECT().GetUserWithResponse(gomock.Any(), gomock.Any()).Return(&auth.GetUserResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: &userResult,
		}, nil)
		res1, err := s.GetUser(ctx, username)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUser(ctx, username)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)
		res1, err := s.GetUserByExternalID(ctx, externalId)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByExternalID(ctx, externalId)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
}

func TestAPIAuthService_CreateUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusCreated,
			expectedResponseID: "foo",
			expectedErr:        nil,
		},
		{
			name:               "invalid_user",
			userName:           "",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "user_exists",
			userName:           "existingUser",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusConflict,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			userName:           "user",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.User{
					Username: tt.userName,
				},
			}
			mockClient.EXPECT().CreateUserWithResponse(gomock.Any(), auth.CreateUserJSONRequestBody{
				Email:        &tt.email,
				FriendlyName: &tt.friendlyName,
				Source:       &tt.source,
				Username:     tt.userName,
			}).Return(response, nil)
			ctx := context.Background()
			res, err := s.CreateUser(ctx, &model.User{
				Username:     tt.userName,
				FriendlyName: &tt.friendlyName,
				Email:        &tt.email,
				Source:       tt.source,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if res != tt.expectedResponseID {
				t.Fatalf("CreateUser: expected user.id: %s got: %s", tt.expectedResponseID, res)
			}
		})
	}
}

func TestAPIAuthService_DeleteUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		responseStatusCode int
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			responseStatusCode: http.StatusNoContent,
			expectedErr:        nil,
		},
		{
			name:               "non_existing_user",
			userName:           "nobody",
			responseStatusCode: http.StatusNotFound,
			expectedErr:        auth.ErrNotFound, // TODO(Guys): change this once we change this to the right error
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().DeleteUserWithResponse(gomock.Any(), tt.userName).Return(response, nil)
			err := s.DeleteUser(ctx, tt.userName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("DeleteUser: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_GetUserByEmail(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)

	tests := []struct {
		name               string
		responseStatusCode int
		users              []string
		email              string
		expectedUserName   string
		expectedErr        error
	}{
		{
			name:               "one_user",
			responseStatusCode: http.StatusOK,
			users:              []string{"one"},
			email:              "one@test.com",
			expectedUserName:   "one",
			expectedErr:        nil,
		},
		{
			name:               "no_users",
			responseStatusCode: http.StatusOK,
			users:              []string{},
			email:              "noone@test.com",
			expectedUserName:   "",
			expectedErr:        auth.ErrNotFound,
		},
		{
			name:               "two_responses",
			responseStatusCode: http.StatusOK,
			email:              "both@test.com",
			users:              []string{"one", "two"},
			expectedUserName:   "",
			expectedErr:        auth.ErrNonUnique,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnedUsers := make([]auth.User, len(tt.users))
			for i, u := range tt.users {
				returnedUsers[i] = auth.User{
					Username: u,
					Email:    &tt.email,
				}
			}
			returnedUserList := &auth.UserList{
				Pagination: auth.Pagination{},
				Results:    returnedUsers,
			}
			const amount = 2
			paginationAmount := auth.PaginationAmount(amount)
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(),
				gomock.Eq(&auth.ListUsersParams{Email: swag.String(tt.email), Amount: &paginationAmount}),
			).Return(&auth.ListUsersResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200:     returnedUserList,
				JSON401:     nil,
				JSONDefault: nil,
			}, nil)

			ctx := context.Background()
			gotUser, err := s.GetUserByEmail(ctx, tt.email)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUserByEmail(%s): expected err: %v got: %v", tt.email, tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if gotUser.Username != tt.expectedUserName {
				t.Fatalf("expected username:%s, got:%s", tt.expectedUserName, gotUser.Username)
			}
		})
	}
}

func NewTestApiService(t *testing.T, withCache bool) (*mock.MockClientWithResponsesInterface, *auth.APIAuthService) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClient := mock.NewMockClientWithResponsesInterface(ctrl)
	secretStore := crypt.NewSecretStore([]byte("secret"))
	cacheParams := authparams.ServiceCache{}
	if withCache {
		cacheParams.Enabled = true
		cacheParams.Size = 100
		cacheParams.TTL = time.Minute
		cacheParams.Jitter = time.Minute
	}
	s, err := auth.NewAPIAuthServiceWithClient(mockClient, true, secretStore, cacheParams, logging.ContextUnavailable())
	if err != nil {
		t.Fatalf("failed initiating API service with mock")
	}
	return mockClient, s
}

func TestAPIAuthService_GetUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		email              string
		friendlyName       string
		source             string
		encryptedPassword  []byte
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			email:              "foo@gmail.com",
			encryptedPassword:  []byte("password"),
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_user",
			userName:           "",
			email:              "",
			encryptedPassword:  nil,
			friendlyName:       "",
			source:             "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "internal_error",
			userName:           "user",
			email:              "",
			encryptedPassword:  nil,
			source:             "",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.User{
					Username:          tt.responseName,
					Email:             &tt.email,
					FriendlyName:      &tt.friendlyName,
					Source:            &tt.source,
					EncryptedPassword: tt.encryptedPassword,
				},
			}
			mockClient.EXPECT().GetUserWithResponse(gomock.Any(), tt.userName).Return(response, nil)
			ctx := context.Background()
			user, err := s.GetUser(ctx, tt.userName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if user.Username != tt.responseName {
				t.Errorf("expected response user name:%s, got:%s", tt.responseName, user.Username)
			}
			if swag.StringValue(user.Email) != tt.email {
				t.Errorf("expected response email :%s, got:%s", tt.responseName, swag.StringValue(user.Email))
			}
			if swag.StringValue(user.FriendlyName) != tt.friendlyName {
				t.Errorf("expected response friendly name :%s, got:%s", tt.responseName, swag.StringValue(user.FriendlyName))
			}
			if user.Source != tt.source {
				t.Errorf("expected response source :%s, got:%s", tt.responseName, user.Source)
			}
			if !bytes.Equal(user.EncryptedPassword, tt.encryptedPassword) {
				t.Errorf("expected response password :%s, got:%s", tt.encryptedPassword, user.EncryptedPassword)
			}
		})
	}
}

func TestAPIAuthService_GetGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                string
		groupName           string
		groupDescription    string
		responseStatusCode  int
		responseName        string
		responseDescription string
		expectedErr         error
	}{
		{
			name:                "successful",
			groupName:           "foo",
			groupDescription:    "foo group",
			responseName:        "foo",
			responseDescription: "foo group",
			responseStatusCode:  http.StatusOK,
			expectedErr:         nil,
		},
		{
			name:               "invalid_group",
			groupName:          "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:        "internal_error",
			groupName:   "group",
			expectedErr: auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetGroupResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Group{
					Name:        tt.responseName,
					Description: swag.String(tt.responseDescription),
				},
			}
			mockClient.EXPECT().GetGroupWithResponse(gomock.Any(), tt.groupName).Return(response, nil)
			ctx := context.Background()
			group, err := s.GetGroup(ctx, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetGroup: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if group.DisplayName != tt.responseName {
				t.Errorf("expected response group name:%s, got:%s", tt.responseName, group.DisplayName)
			}
			if swag.StringValue(group.Description) != tt.responseDescription {
				t.Errorf("expected response group description:%s, got:%s", tt.responseDescription, swag.StringValue(group.Description))
			}
		})
	}
}

func TestAPIAuthService_GetCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		reqAccessKey       string
		accessKey          string
		secretKey          string
		username           string
		expectedErr        error
	}{
		{
			name:               "successful",
			reqAccessKey:       "AKIA",
			accessKey:          "AKIA",
			secretKey:          "SECRET",
			username:           "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_credentials",
			reqAccessKey:       "AKIA",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:         "internal_error",
			reqAccessKey: "AKIA",
			expectedErr:  auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.accessKey,
					SecretAccessKey: tt.secretKey,
				},
			}
			mockClient.EXPECT().GetCredentialsWithResponse(gomock.Any(), tt.accessKey).Return(response, nil)
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(&auth.ListUsersResponse{
				HTTPResponse: &http.Response{
					StatusCode: http.StatusOK,
				},
				JSON200: &auth.UserList{
					Pagination: auth.Pagination{},
					Results: []auth.User{{
						Username: tt.username,
					}},
				},
			}, nil)
			ctx := context.Background()
			credentials, err := s.GetCredentials(ctx, tt.accessKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if credentials.AccessKeyID != tt.accessKey {
				t.Errorf("expected response accessKeyID:%s, got:%s", tt.accessKey, credentials.AccessKeyID)
			}
			if credentials.SecretAccessKey != tt.secretKey {
				t.Errorf("expected response SecretAccessKey:%s, got:%s", tt.accessKey, credentials.SecretAccessKey)
			}
			if credentials.Username != tt.username {
				t.Errorf("expected response username:%s, got:%s", tt.username, credentials.Username)
			}
		})
	}
}

func TestAPIAuthService_GetCredentialsForUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		reqAccessKey       string
		accessKey          string
		secretKey          string
		username           string
		expectedErr        error
	}{
		{
			name:               "successful",
			reqAccessKey:       "AKIA",
			accessKey:          "AKIA",
			secretKey:          "SECRET",
			username:           "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_credentials",
			reqAccessKey:       "AKIA",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:         "internal_error",
			reqAccessKey: "AKIA",
			expectedErr:  auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetCredentialsForUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Credentials{
					AccessKeyId: tt.accessKey,
				},
			}
			mockClient.EXPECT().GetCredentialsForUserWithResponse(gomock.Any(), tt.username, tt.accessKey).Return(response, nil)
			ctx := context.Background()
			credentials, err := s.GetCredentialsForUser(ctx, tt.username, tt.accessKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetCredentialsForUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if credentials.AccessKeyID != tt.accessKey {
				t.Errorf("expected response accessKeyID:%s, got:%s", tt.accessKey, credentials.AccessKeyID)
			}
			if credentials.Username != tt.username {
				t.Errorf("expected response username:%s, got:%s", tt.username, credentials.Username)
			}
		})
	}
}

func TestAPIAuthService_ListGroups(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const groupNamePrefix = "groupNamePrefix"
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			groups := make([]auth.Group, amount)
			for i := 0; i < amount; i++ {
				groups[i] = auth.Group{
					CreationDate: creationDate,
					Name:         fmt.Sprintf("%s-%d", groupNamePrefix, i),
					Description:  swag.String(fmt.Sprintf("%s-%d desc", groupNamePrefix, i)),
				}
			}
			groupList := auth.GroupList{
				Pagination: auth.Pagination{},
				Results:    groups,
			}
			response := &auth.ListGroupsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &groupList
			paginationParams := model.PaginationParams{}
			paginationPrefix := auth.PaginationPrefix(paginationParams.Prefix)
			paginationAfter := auth.PaginationAfter(paginationParams.After)
			paginationAmount := auth.PaginationAmount(paginationParams.Amount)
			mockClient.EXPECT().ListGroupsWithResponse(gomock.Any(), gomock.Eq(&auth.ListGroupsParams{
				Prefix: &paginationPrefix,
				After:  &paginationAfter,
				Amount: &paginationAmount,
			})).Return(response, nil)
			group, _, err := s.ListGroups(context.Background(), &paginationParams)
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range group {
				if g == nil {
					t.Fatalf("got nil group")
				}
				expectedName := fmt.Sprintf("%s-%d", groupNamePrefix, i)
				if g.DisplayName != expectedName {
					t.Errorf("ListGroups item %d, expected displayName:%s got:%s", i, g.DisplayName, expectedName)
				}
				expectedDescription := fmt.Sprintf("%s-%d desc", groupNamePrefix, i)
				if swag.StringValue(g.Description) != expectedDescription {
					t.Errorf("ListGroups item %d, expected expectedDescription:%s got:%s", i, swag.StringValue(g.Description), expectedName)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("eListGroups item %d, expected created date:%s got:%s for %s", i, g.CreatedAt, creationTime, expectedName)
				}
			}
		})
	}
}

func TestAPIAuthService_ListUsers(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const userNamePrefix = "userNamePrefix"
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			users := make([]auth.User, amount)
			for i := 0; i < amount; i++ {
				users[i] = auth.User{
					CreationDate: creationDate,
					Username:     fmt.Sprintf("%s-%d", userNamePrefix, i),
				}
			}
			userList := auth.UserList{
				Pagination: auth.Pagination{},
				Results:    users,
			}
			response := &auth.ListUsersResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &userList
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
			user, _, err := s.ListUsers(context.Background(), &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range user {
				if g == nil {
					t.Fatalf("got nil user")
				}
				expected := fmt.Sprintf("%s-%d", userNamePrefix, i)
				if g.Username != expected {
					t.Errorf("expected displayName:%s got:%s", g.Username, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_ListGroupUsers(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const userNamePrefix = "userNamePrefix"
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			users := make([]auth.User, amount)
			for i := 0; i < amount; i++ {
				users[i] = auth.User{
					CreationDate: creationDate,
					Username:     fmt.Sprintf("%s-%d", userNamePrefix, i),
				}
			}
			userList := auth.UserList{
				Pagination: auth.Pagination{},
				Results:    users,
			}
			response := &auth.ListGroupMembersResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &userList
			const groupDisplayName = "groupFoo"
			mockClient.EXPECT().ListGroupMembersWithResponse(gomock.Any(), groupDisplayName, gomock.Any()).Return(response, nil)
			user, _, err := s.ListGroupUsers(context.Background(), groupDisplayName, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}
			creationTime := time.Unix(creationDate, 0)
			for i, g := range user {
				if g == nil {
					t.Fatalf("got nil user")
				}
				expected := fmt.Sprintf("%s-%d", userNamePrefix, i)
				if g.Username != expected {
					t.Errorf("expected displayName:%s got:%s", g.Username, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_AddUserToGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			username:    "user_ame",
			mockErr:     nil,
			statusCode:  http.StatusCreated,
			expectedErr: nil,
		},
		{
			name:        "api_internal_error",
			groupName:   "gname",
			username:    "uname",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "no_group",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.AddGroupMembershipResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AddGroupMembershipWithResponse(gomock.Any(), tt.groupName, tt.username).Return(response, tt.mockErr)
			err := s.AddUserToGroup(context.Background(), tt.username, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DeleteGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			groupName:   "group_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "no_group",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteGroupWithResponse(gomock.Any(), tt.groupName).Return(response, tt.mockErr)
			err := s.DeleteGroup(context.Background(), tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_RemoveUserFromGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteGroupMembershipResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteGroupMembershipWithResponse(gomock.Any(), tt.groupName, tt.username).Return(response, tt.mockErr)
			err := s.RemoveUserFromGroup(context.Background(), tt.username, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_ListUserGroups(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const groupNamePrefix = "groupNamePrefix"
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			groups := make([]auth.Group, amount)
			for i := 0; i < amount; i++ {
				groups[i] = auth.Group{
					CreationDate: creationDate,
					Name:         fmt.Sprintf("%s-%d", groupNamePrefix, i),
				}
			}
			groupList := auth.GroupList{
				Pagination: auth.Pagination{},
				Results:    groups,
			}
			response := &auth.ListUserGroupsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &groupList
			const username = "userFoo"
			mockClient.EXPECT().ListUserGroupsWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
			group, _, err := s.ListUserGroups(context.Background(), username, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range group {
				if g == nil {
					t.Fatalf("item %d - got nil group", i)
				}
				expected := fmt.Sprintf("%s-%d", groupNamePrefix, i)
				if g.DisplayName != expected {
					t.Errorf("expected displayName:%s got:%s", g.DisplayName, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_ListUserCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const accessKeyPrefix = "AKIA"
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			credentials := make([]auth.Credentials, amount)
			for i := 0; i < amount; i++ {
				credentials[i] = auth.Credentials{
					CreationDate: creationDate,
					AccessKeyId:  fmt.Sprintf("%s-%d", accessKeyPrefix, i),
				}
			}
			credentialsList := auth.CredentialsList{
				Pagination: auth.Pagination{},
				Results:    credentials,
			}
			response := &auth.ListUserCredentialsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &credentialsList
			const username = "userFoo"
			mockClient.EXPECT().ListUserCredentialsWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
			resCredentials, _, err := s.ListUserCredentials(context.Background(), username, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			for i, g := range resCredentials {
				if g == nil {
					t.Fatalf("item %d - got nil credentials", i)
				}
				expected := fmt.Sprintf("%s-%d", accessKeyPrefix, i)
				if g.AccessKeyID != expected {
					t.Errorf("expected AccessKeyID:%s got:%s", expected, g.AccessKeyID)
				}
			}
		})
	}
}

func TestAPIAuthService_WritePolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                   string
		policyName             string
		responseStatusCode     int
		firstStatementResource string
		firstStatementEffect   string
		firstStatementAction   []string
		responseName           string
		expectedErr            error
		overwrite              bool
	}{
		{
			name:                   "successful",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "foo",
			responseName:           "foo",
			responseStatusCode:     http.StatusCreated,
			expectedErr:            nil,
		},
		{
			name:                   "invalid_policy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "",
			responseStatusCode:     http.StatusBadRequest,
			expectedErr:            model.ErrValidationError, // TODO(Guys): change this once we change this to the right error
		},
		{
			name:                   "create_policy_exists",
			policyName:             "existingPolicy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseStatusCode:     http.StatusConflict,
			expectedErr:            auth.ErrAlreadyExists,
		},
		{
			name:                   "update_policy_exists",
			policyName:             "existingPolicy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseStatusCode:     http.StatusOK,
			overwrite:              true,
		},
		{
			name:                   "update_policy_not_exists",
			policyName:             "NewPolicy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseStatusCode:     http.StatusNotFound,
			expectedErr:            auth.ErrNotFound,
			overwrite:              true,
		},
		{
			name:                   "internal_error",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "policy",
			responseStatusCode:     http.StatusInternalServerError,
			expectedErr:            auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		acl := ""
		t.Run(tt.name, func(t *testing.T) {
			creationTime := time.Unix(123456789, 0)

			if tt.overwrite {
				response := &auth.UpdatePolicyResponse{
					HTTPResponse: &http.Response{
						StatusCode: tt.responseStatusCode,
					},
					JSON200: &auth.Policy{
						Name: tt.responseName,
					},
				}
				mockClient.EXPECT().UpdatePolicyWithResponse(gomock.Any(), tt.policyName, gomock.Eq(auth.UpdatePolicyJSONRequestBody{
					Acl:          &acl,
					CreationDate: swag.Int64(creationTime.Unix()),
					Name:         tt.policyName,
					Statement: []auth.Statement{
						{
							Action:   tt.firstStatementAction,
							Effect:   tt.firstStatementEffect,
							Resource: tt.firstStatementResource,
						},
					},
				})).MaxTimes(1).Return(response, nil)
			} else {
				response := &auth.CreatePolicyResponse{
					HTTPResponse: &http.Response{
						StatusCode: tt.responseStatusCode,
					},
					JSON201: &auth.Policy{
						Name: tt.responseName,
					},
				}
				mockClient.EXPECT().CreatePolicyWithResponse(gomock.Any(), gomock.Eq(auth.CreatePolicyJSONRequestBody{
					Acl:          &acl,
					CreationDate: swag.Int64(creationTime.Unix()),
					Name:         tt.policyName,
					Statement: []auth.Statement{
						{
							Action:   tt.firstStatementAction,
							Effect:   tt.firstStatementEffect,
							Resource: tt.firstStatementResource,
						},
					},
				})).MaxTimes(1).Return(response, nil)
			}
			ctx := context.Background()
			err := s.WritePolicy(ctx, &model.Policy{
				DisplayName: tt.policyName,
				CreatedAt:   creationTime,
				Statement: []model.Statement{
					{
						Action:   tt.firstStatementAction,
						Effect:   tt.firstStatementEffect,
						Resource: tt.firstStatementResource,
					},
				},
			}, tt.overwrite)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreatePolicy: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_GetPolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                   string
		policyName             string
		firstStatementResource string
		firstStatementEffect   string
		firstStatementAction   []string
		responseStatusCode     int
		responseName           string
		expectedErr            error
	}{
		{
			name:                   "successful",
			policyName:             "foo",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseName:           "foo",
			responseStatusCode:     http.StatusOK,
			expectedErr:            nil,
		},
		{
			name:               "invalid_policy",
			policyName:         "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:        "internal_error",
			policyName:  "policy",
			expectedErr: auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetPolicyResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Policy{
					Name: tt.responseName,
					Statement: []auth.Statement{
						{
							Action:   tt.firstStatementAction,
							Effect:   tt.firstStatementEffect,
							Resource: tt.firstStatementResource,
						},
					},
				},
			}
			mockClient.EXPECT().GetPolicyWithResponse(gomock.Any(), tt.policyName).Return(response, nil)
			ctx := context.Background()
			policy, err := s.GetPolicy(ctx, tt.policyName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetPolicy: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if response.JSON200 == nil {
				t.Fatal("GetPolicy: unexpected return of nil policy")
			}
			policyEquals(t, *response.JSON200, policy)
		})
	}
}

var authPoliciesForTesting = []auth.Policy{
	{
		Statement: []auth.Statement{
			{
				Action:   []string{"auth:DeleteUser"},
				Resource: "arn:lakefs:auth:::user/foobar",
				Effect:   model.StatementEffectAllow,
			},
			{
				Action:   []string{"auth:*"},
				Resource: "*",
				Effect:   model.StatementEffectDeny,
			},
		},
	},
}

func statementEquals(t *testing.T, authStatement []auth.Statement, modalStatement []model.Statement) {
	t.Helper()
	if len(authStatement) != len(modalStatement) {
		t.Errorf(" amoumt of statements:  (authPolicy)%d != (modelPolicy)%d", len(authStatement), len(modalStatement))
		return
	}
	for i, authS := range authStatement {
		if authS.Effect != modalStatement[i].Effect {
			t.Errorf("Effect  (authStatement)%s != (modelStatement)%s", modalStatement[i].Effect, authS.Effect)
		}
		if authS.Resource != modalStatement[i].Resource {
			t.Errorf("Resource  (authStatement)%s != (modelStatement)%s", modalStatement[i].Resource, authS.Resource)
		}
		if diff := deep.Equal(authS.Action, modalStatement[i].Action); diff != nil {
			t.Errorf("Action diff %s", diff)
		}
	}
}

func policyEquals(t *testing.T, authPolicy auth.Policy, modelPolicy *model.Policy) {
	t.Helper()
	if authPolicy.Name == "" && modelPolicy == nil {
		return
	}
	if modelPolicy == nil {
		t.Errorf("got nil modelPolicy nil comparing to authPolicy:%s", authPolicy.Name)
		return
	}
	if authPolicy.Name != modelPolicy.DisplayName {
		t.Errorf("non equal name %s != %s", authPolicy.Name, modelPolicy.DisplayName)
	}
	statementEquals(t, authPolicy.Statement, modelPolicy.Statement)
}

func policyListsEquals(t *testing.T, authPolicies []auth.Policy, modelPolicies []*model.Policy) {
	t.Helper()
	if len(authPolicies) != len(modelPolicies) {
		t.Fatalf("got %d policies expected:%d", len(authPolicies), len(modelPolicies))
	}
	for i, ap := range authPolicies {
		policyEquals(t, ap, modelPolicies[i])
	}
}

func TestAPIAuthService_ListUserPolicies(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	resPolicies := authPoliciesForTesting
	policyList := auth.PolicyList{
		Pagination: auth.Pagination{},
		Results:    resPolicies,
	}

	t.Run("policies", func(t *testing.T) {
		response := &auth.ListPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		mockClient.EXPECT().ListPoliciesWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
		policies, _, err := s.ListPolicies(context.Background(), &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})

	t.Run("user policies", func(t *testing.T) {
		username := "username"
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListUserPolicies(context.Background(), username, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("group policies", func(t *testing.T) {
		response := &auth.ListGroupPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const groupName = "groupName"
		mockClient.EXPECT().ListGroupPoliciesWithResponse(gomock.Any(), groupName, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListGroupPolicies(context.Background(), groupName, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("effective policies", func(t *testing.T) {
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const username = "username"
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("all effective policies", func(t *testing.T) {
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const username = "username"
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
}

func TestAPIAuthService_DeletePolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		policyName  string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			policyName:  "policy_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			policyName:  "policy_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			policyName:  "no_policy",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeletePolicyResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeletePolicyWithResponse(gomock.Any(), tt.policyName).Return(response, tt.mockErr)
			err := s.DeletePolicy(context.Background(), tt.policyName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DetachPolicyFrom(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		fromName    string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			fromName:    "no_from",
			username:    "userName",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run("from user "+tt.name, func(t *testing.T) {
			response := &auth.DetachPolicyFromUserResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DetachPolicyFromUserWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.DetachPolicyFromUser(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
		t.Run("from group "+tt.name, func(t *testing.T) {
			response := &auth.DetachPolicyFromGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DetachPolicyFromGroupWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.DetachPolicyFromGroup(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_AttachPolicyTo(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		fromName    string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusCreated,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			fromName:    "no_from",
			username:    "userName",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run("to user "+tt.name, func(t *testing.T) {
			response := &auth.AttachPolicyToUserResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AttachPolicyToUserWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.AttachPolicyToUser(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
		t.Run("to group "+tt.name, func(t *testing.T) {
			response := &auth.AttachPolicyToGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AttachPolicyToGroupWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.AttachPolicyToGroup(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DeleteCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name            string
		username        string
		credentialsName string
		mockErr         error
		statusCode      int
		expectedErr     error
	}{
		{
			name:            "no_error",
			username:        "username",
			credentialsName: "credentials_name",
			mockErr:         nil,
			statusCode:      http.StatusNoContent,
			expectedErr:     nil,
		},
		{
			name:            "invalid_user",
			username:        "",
			credentialsName: "credentials_name",
			mockErr:         mockErr,
			statusCode:      http.StatusInternalServerError,
			expectedErr:     mockErr,
		},
		{
			name:            "api_error",
			username:        "username",
			credentialsName: "credentials_name",
			mockErr:         mockErr,
			statusCode:      http.StatusInternalServerError,
			expectedErr:     mockErr,
		},
		{
			name:            "not_found",
			username:        "username",
			credentialsName: "no_credentials",
			mockErr:         nil,
			statusCode:      http.StatusNotFound,
			expectedErr:     auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteCredentialsResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteCredentialsWithResponse(gomock.Any(), tt.username, tt.credentialsName).Return(response, tt.mockErr)
			err := s.DeleteCredentials(context.Background(), tt.username, tt.credentialsName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_CreateGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                string
		groupName           string
		groupDescription    string
		responseStatusCode  int
		responseName        string
		responseDescription string
		expectedErr         error
	}{
		{
			name:                "successful",
			groupName:           "foo",
			groupDescription:    "foo description",
			responseName:        "foo",
			responseDescription: "foo description",
			responseStatusCode:  http.StatusCreated,
			expectedErr:         nil,
		},
		{
			name:               "invalid_group",
			groupName:          "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "group_exists",
			groupName:          "existingGroup",
			responseStatusCode: http.StatusConflict,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			groupName:          "group",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateGroupResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.Group{
					Name:        tt.responseName,
					Description: swag.String(tt.responseDescription),
				},
			}
			mockClient.EXPECT().CreateGroupWithResponse(gomock.Any(), auth.CreateGroupJSONRequestBody{
				Id: tt.groupName,
			}).Return(response, nil)
			ctx := context.Background()
			_, err := s.CreateGroup(ctx, &model.Group{
				DisplayName: tt.groupName,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateGroup: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_CreateCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		username           string
		returnedAccessKey  string
		returnedSecretKey  string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			username:           "foo",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusCreated,
			expectedErr:        nil,
		},
		{
			name:               "invalid_username",
			username:           "",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "internal_error",
			username:           "credentials",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.returnedAccessKey,
					SecretAccessKey: tt.returnedSecretKey,
				},
			}
			mockClient.EXPECT().CreateCredentialsWithResponse(gomock.Any(), tt.username, &auth.CreateCredentialsParams{}).Return(response, nil)
			ctx := context.Background()
			resCredentials, err := s.CreateCredentials(ctx, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if resCredentials.AccessKeyID != tt.returnedAccessKey {
				t.Errorf("expected accessKeyID:%s, got:%s", tt.returnedAccessKey, resCredentials.AccessKeyID)
			}
			if resCredentials.SecretAccessKey != tt.returnedSecretKey {
				t.Errorf("expected secretKeyID:%s, got:%s", tt.returnedSecretKey, resCredentials.SecretAccessKey)
			}
		})
	}
}

func TestAPIAuthService_AddCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		username           string
		returnedAccessKey  string
		returnedSecretKey  string
		accessKey          string
		secretKey          string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			username:           "foo",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusCreated,
			expectedErr:        nil,
		},
		{
			name:               "invalid_username",
			username:           "",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			accessKey:          "",
			secretKey:          "",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "credentials_exists",
			username:           "existingCredentials",
			returnedAccessKey:  "",
			returnedSecretKey:  "",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusConflict,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			username:           "credentials",
			returnedAccessKey:  "",
			returnedSecretKey:  "",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.returnedAccessKey,
					SecretAccessKey: tt.returnedSecretKey,
				},
			}
			mockClient.EXPECT().CreateCredentialsWithResponse(gomock.Any(), tt.username, &auth.CreateCredentialsParams{
				AccessKey: &tt.accessKey,
				SecretKey: &tt.secretKey,
			}).Return(response, nil)
			ctx := context.Background()
			resCredentials, err := s.AddCredentials(ctx, tt.username, tt.accessKey, tt.secretKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if resCredentials.AccessKeyID != tt.returnedAccessKey {
				t.Errorf("expected accessKeyID:%s, got:%s", tt.returnedAccessKey, resCredentials.AccessKeyID)
			}
			if resCredentials.SecretAccessKey != tt.returnedSecretKey {
				t.Errorf("expected secretKeyID:%s, got:%s", tt.returnedSecretKey, resCredentials.SecretAccessKey)
			}
		})
	}
}

func TestAPIAuthService_CreateUserExternalPrincipal(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)

	tests := []struct {
		name               string
		userID             string
		principalID        string
		responseStatusCode int
		expectedErr        error
	}{
		{
			name:               "successful_principal1",
			userID:             "user1",
			principalID:        "arn:aws:sts::123:assumed-role/MyRole/SessionName",
			responseStatusCode: http.StatusCreated,
		},
		{
			name:               "successful_principal2",
			userID:             "user1",
			principalID:        "arn:aws:sts::456:assumed-role/OtherRole",
			responseStatusCode: http.StatusCreated,
		},
		{
			name:               "err_existing_principal",
			userID:             "user2",
			principalID:        "arn:aws:sts::456:assumed-role/OtherRole",
			responseStatusCode: http.StatusConflict,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "successful_principal3",
			userID:             "user2",
			principalID:        "arn:aws:sts::456:assumed-role/Principal3",
			responseStatusCode: http.StatusCreated,
		},
		{
			name:               "err_no_such_user",
			userID:             "no-user",
			principalID:        "arn:aws:sts::456:assumed-role/Principal3",
			responseStatusCode: http.StatusNotFound,
			expectedErr:        auth.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.userID, func(t *testing.T) {
			ctx := context.Background()
			response := &auth.CreateUserExternalPrincipalResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
			}
			reqParams := gomock.Eq(&auth.CreateUserExternalPrincipalParams{PrincipalId: tt.principalID})
			mockClient.EXPECT().CreateUserExternalPrincipalWithResponse(gomock.Any(), tt.userID, reqParams).Return(response, nil)
			err := s.CreateUserExternalPrincipal(ctx, tt.userID, tt.principalID)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateUserExternalPrincipal: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
		})
	}
}
func TestAPIAuthService_ReusePrincipalAfterDelete(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	userA := "user_a"
	userB := "user_b"
	principalId := "arn:aws:sts::123:assumed-role/MyRole/SessionName"
	ctx := context.Background()

	// create principal A for user1
	reqParams := gomock.Eq(&auth.CreateUserExternalPrincipalParams{PrincipalId: principalId})
	mockClient.EXPECT().CreateUserExternalPrincipalWithResponse(gomock.Any(), userA, reqParams).Return(&auth.CreateUserExternalPrincipalResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusCreated,
		},
	}, nil)
	err := s.CreateUserExternalPrincipal(ctx, userA, principalId)
	require.NoErrorf(t, err, "creating initial principal for user %s", userA)

	// delete principal A for user1
	reqDelParams := gomock.Eq(&auth.DeleteUserExternalPrincipalParams{PrincipalId: principalId})
	mockClient.EXPECT().DeleteUserExternalPrincipalWithResponse(gomock.Any(), userA, reqDelParams).Return(&auth.DeleteUserExternalPrincipalResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusNoContent,
		},
	}, nil)
	err = s.DeleteUserExternalPrincipal(ctx, userA, principalId)
	require.NoErrorf(t, err, "deleting principal for user %s", userA)

	// re-use principal A again for user2
	mockClient.EXPECT().CreateUserExternalPrincipalWithResponse(gomock.Any(), userB, reqParams).Return(&auth.CreateUserExternalPrincipalResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusCreated,
		},
	}, nil)
	err = s.CreateUserExternalPrincipal(ctx, userB, principalId)
	require.NoErrorf(t, err, "re-using principal for user %s", userB)
}

func TestAPIAuthService_DeleteExternalPrincipalAttachedToUserDelete(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	userId := "user"
	principalId := "arn:aws:sts::123:assumed-role/MyRole/SessionName"
	ctx := context.Background()

	// create userA and principalA
	reqParams := gomock.Eq(&auth.CreateUserExternalPrincipalParams{PrincipalId: principalId})
	mockClient.EXPECT().CreateUserExternalPrincipalWithResponse(gomock.Any(), userId, reqParams).Return(&auth.CreateUserExternalPrincipalResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusCreated,
		},
	}, nil)
	err := s.CreateUserExternalPrincipal(ctx, userId, principalId)
	require.NoError(t, err)

	// delete user A
	mockClient.EXPECT().DeleteUserWithResponse(gomock.Any(), userId).Return(&auth.DeleteUserResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusNoContent,
		},
	}, nil)
	err = s.DeleteUser(ctx, userId)
	require.NoError(t, err)
	// get principalA and expect error

	mockClient.EXPECT().GetExternalPrincipalWithResponse(gomock.Any(), gomock.Eq(&auth.GetExternalPrincipalParams{
		PrincipalId: principalId,
	})).Return(
		&auth.GetExternalPrincipalResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusNotFound,
			},
		}, auth.ErrNotFound)

	_, err = s.GetExternalPrincipal(ctx, principalId)
	require.Errorf(t, err, "principal should not exist if a user is deleted")
}

func TestAPIService_RequestIDPropagation(t *testing.T) {
	const requestID = "the-quick-brown-fox-jumps-over-the-lazy-dog"
	called := false
	innerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle (only) fake deleteUser, by returning 204.
		if actual := r.Header.Get("X-Request-ID"); actual != requestID {
			t.Errorf("Got request ID %s expecting %s", actual, requestID)
		}
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	secretStore := crypt.NewSecretStore([]byte("secret"))
	cacheParams := authparams.ServiceCache{}
	service, err := auth.NewAPIAuthService(innerServer.URL, "token", true, secretStore, cacheParams, logging.ContextUnavailable())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), httputil.RequestIDContextKey, requestID)

	require.NoError(t, service.DeleteUser(ctx, "foo"))
	if !called {
		t.Error("Expected inner server to be called but it wasn't")
	}
}
