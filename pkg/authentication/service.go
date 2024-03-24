package authentication

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package apiclient -generate "types,client" -o apiclient/client.gen.go  ../../api/authentication.yml
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_authentication_client.go github.com/treeverse/lakefs/pkg/authentication/apiclient ClientWithResponsesInterface

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Service interface {
	ValidateSTS(ctx context.Context, code, redirectURI, state string) (*STSResponseData, error)
}

type DummyService struct{}

func NewDummyService() *DummyService {
	return &DummyService{}
}
func (d DummyService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (*STSResponseData, error) {
	return nil, ErrNotImplemented
}

type APIService struct {
	validateIDTokenClaims map[string]string
	apiClient             apiclient.ClientWithResponsesInterface
	logger                logging.Logger
}

func NewAPIService(apiEndpoint string, validateIDTokenClaims map[string]string, logger logging.Logger) (*APIService, error) {
	client, err := apiclient.NewClientWithResponses(apiEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication api client: %w", err)
	}

	res := &APIService{
		validateIDTokenClaims: validateIDTokenClaims,
		apiClient:             client,
		logger:                logger,
	}
	return res, nil
}

func NewAPIServiceWithClients(apiClient apiclient.ClientWithResponsesInterface, logger logging.Logger, validateIDTokenClaims map[string]string) (*APIService, error) {
	return &APIService{
		apiClient:             apiClient,
		logger:                logger,
		validateIDTokenClaims: validateIDTokenClaims,
	}, nil
}

// validateResponse returns ErrUnexpectedStatusCode if the response status code is not as expected
func (s *APIService) validateResponse(resp openapi3filter.StatusCoder, expectedStatusCode int) error {
	statusCode := resp.StatusCode()
	if statusCode == expectedStatusCode {
		return nil
	}
	switch statusCode {
	case http.StatusBadRequest:
		return ErrInvalidRequest
	case http.StatusConflict:
		return ErrAlreadyExists
	case http.StatusUnauthorized:
		return ErrInsufficientPermissions
	default:
		return fmt.Errorf("%w - got %d expected %d", ErrUnexpectedStatusCode, statusCode, expectedStatusCode)
	}
}

type STSResponseData struct {
	ExternalUserID    string
	ExpiresAtUnixTime int64
}

// ValidateSTS calls the external authentication service to validate the STS parameters
// validates the required claims and returns the external user id and expiration time
func (s *APIService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (*STSResponseData, error) {
	res, err := s.apiClient.STSLoginWithResponse(ctx, apiclient.STSLoginJSONRequestBody{
		Code:        code,
		RedirectUri: redirectURI,
		State:       state,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate user: %w", err)
	}

	if err := s.validateResponse(res, http.StatusOK); err != nil {
		return nil, fmt.Errorf("failed to authenticate user: %w", err)
	}

	// validate claims
	claims := res.JSON200.Claims
	for claim, expectedValue := range s.validateIDTokenClaims {
		if claimValue, found := claims.Get(claim); !found || claimValue != expectedValue {
			return nil, fmt.Errorf("claim %s has unexpected value %s: %w", claim, claimValue, ErrInvalidSTS)
		}
	}
	subject, found := claims.Get("sub")
	if !found {
		return nil, fmt.Errorf("missing subject in claims: %w", ErrInvalidSTS)
	}
	expiresAt, found := claims.Get("exp")
	if !found {
		return nil, fmt.Errorf("missing expiration in claims: %w", ErrInvalidSTS)
	}
	expiresAtInt, err := strconv.ParseFloat(expiresAt, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expiration time: %w", err)
	}
	return &STSResponseData{
		ExternalUserID:    subject,
		ExpiresAtUnixTime: int64(expiresAtInt),
	}, nil
}
