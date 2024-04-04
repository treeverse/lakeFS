package authentication_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/authentication/mock"
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
			mockClient, s := NewTestApiService(t, validateTokenClaims)
			ctx := context.Background()
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

func NewTestApiService(t *testing.T, validateIDTokenClaims map[string]string) (*mock.MockClientWithResponsesInterface, *authentication.APIService) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClient := mock.NewMockClientWithResponsesInterface(ctrl)
	s, err := authentication.NewAPIServiceWithClients(mockClient, logging.ContextUnavailable(), validateIDTokenClaims)
	if err != nil {
		t.Fatalf("failed initiating API service with mock")
	}
	return mockClient, s
}
