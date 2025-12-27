package authentication_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/authentication/mock"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestAPIAuthService_STSLogin(t *testing.T) {
	someErr := errors.New("some error")
	tests := []struct {
		name                 string
		responseStatusCode   int
		expectedErr          error
		error                error
		additionalClaim      string
		additionalClaimValue string
		validateClaim        string
		validateClaimValue   string
		returnedSubject      string
	}{
		{
			name:               "ok",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
			returnedSubject:    "external_user_id",
		},
		{
			name:                 "With additional claim",
			responseStatusCode:   http.StatusOK,
			expectedErr:          nil,
			additionalClaim:      "additional_claim",
			additionalClaimValue: "additional_claim_value",
			validateClaim:        "additional_claim",
			validateClaimValue:   "additional_claim_value",
			returnedSubject:      "external_user_id",
		},
		{
			name:                 "Non matching additional claim",
			responseStatusCode:   http.StatusOK,
			expectedErr:          authentication.ErrInsufficientPermissions,
			additionalClaim:      "additional_claim",
			additionalClaimValue: "additional_claim_value",
			validateClaim:        "additional_claim",
			validateClaimValue:   "additional_claim_value2",
			returnedSubject:      "external_user_id",
		},
		{
			name:               "Missing subject",
			responseStatusCode: http.StatusOK,
			expectedErr:        authentication.ErrInsufficientPermissions,
		},
		{
			name:               "Not authorized",
			responseStatusCode: http.StatusUnauthorized,
			expectedErr:        authentication.ErrInsufficientPermissions,
		},
		{
			name:               "Internal server error",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        authentication.ErrUnexpectedStatusCode,
		},
		{
			name:        "Other error",
			error:       someErr,
			expectedErr: someErr,
		},
	}
	code := "some_code"
	state := "some_state"
	redirectURI := "some_redirect_uri"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validateTokenClaims := map[string]string{tt.validateClaim: tt.validateClaimValue}
			mockClient, s := NewTestApiService(t, validateTokenClaims, false)
			ctx := t.Context()
			requestEq := gomock.Eq(apiclient.STSLoginJSONRequestBody{
				RedirectUri: redirectURI,
				Code:        code,
				State:       state,
			})

			loginResponse := &apiclient.STSLoginResponse{
				Body:         nil,
				HTTPResponse: &http.Response{StatusCode: tt.responseStatusCode},
				JSON200:      nil,
				JSON401:      nil,
				JSONDefault:  nil,
			}
			if tt.responseStatusCode == http.StatusOK {
				loginResponse.JSON200 = &apiclient.OidcTokenData{
					Claims: apiclient.OidcTokenData_Claims{
						AdditionalProperties: map[string]string{tt.additionalClaim: tt.additionalClaimValue},
					},
				}
				if tt.returnedSubject != "" {
					loginResponse.JSON200.Claims.AdditionalProperties["sub"] = tt.returnedSubject
				}
			}
			mockClient.EXPECT().STSLoginWithResponse(gomock.Any(), requestEq).Return(loginResponse, tt.error)
			externalUserID, err := s.ValidateSTS(ctx, code, redirectURI, state)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("ValidateSTS: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if externalUserID != tt.returnedSubject {
				t.Fatalf("expected subject to be 'external_user_id', got %s", externalUserID)
			}
		})
	}
}

func NewTestApiService(t *testing.T, validateIDTokenClaims map[string]string, externalPrincipalsEnabled bool) (*mock.MockClientWithResponsesInterface, *authentication.APIService) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClient := mock.NewMockClientWithResponsesInterface(ctrl)
	s, err := authentication.NewAPIServiceWithClients(mockClient, logging.ContextUnavailable(), validateIDTokenClaims, externalPrincipalsEnabled)
	if err != nil {
		t.Fatalf("failed initiating API service with mock")
	}
	return mockClient, s
}

func TestAPIAuthService_ExternalLogin(t *testing.T) {
	mockClient, s := NewTestApiService(t, map[string]string{}, true)
	ctx := t.Context()
	principalId := "arn"
	externalLoginInfo := map[string]any{"IdentityToken": "Token"}

	mockClient.EXPECT().ExternalPrincipalLoginWithResponse(gomock.Any(), gomock.Eq(externalLoginInfo)).Return(
		&apiclient.ExternalPrincipalLoginResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: &apiclient.ExternalPrincipal{Id: principalId},
		}, nil)

	resp, err := s.ExternalPrincipalLogin(ctx, externalLoginInfo)
	require.NoError(t, err)
	require.Equal(t, principalId, resp.Id)
}

func TestAPIService_RequestIDPropagation(t *testing.T) {
	const requestID = "test-request-id-12345"
	called := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if actual := r.Header.Get("X-Request-ID"); actual != requestID {
			t.Errorf("Got request ID %q, expected %q", actual, requestID)
		}
		// Return a valid STS login response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"claims": {"sub": "test-user"}}`))
	}))
	defer server.Close()

	service, err := authentication.NewAPIService(server.URL, nil, logging.ContextUnavailable(), false)
	require.NoError(t, err)

	ctx := context.WithValue(t.Context(), httputil.RequestIDContextKey, requestID)
	_, err = service.ValidateSTS(ctx, "code", "redirect", "state")
	require.NoError(t, err)
	require.True(t, called, "Expected server to be called")
}
