package authentication

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package apiclient -generate "types,client" -o apiclient/client.gen.go  ../../api/authentication.yml
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_authentication_client.go github.com/treeverse/lakefs/pkg/authentication/apiclient ClientWithResponsesInterface

import (
	"context"
	"fmt"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Service interface {
	IsExternalPrincipalsEnabled() bool
	ExternalPrincipalLogin(ctx context.Context, identityRequest map[string]interface{}) (*apiclient.ExternalPrincipal, error)
	// ValidateSTS validates the STS parameters and returns the external user ID
	ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error)
}

type DummyService struct{}

func NewDummyService() *DummyService {
	return &DummyService{}
}

func (d DummyService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error) {
	return "", ErrNotImplemented
}

func (d DummyService) ExternalPrincipalLogin(_ context.Context, _ map[string]interface{}) (*apiclient.ExternalPrincipal, error) {
	return nil, ErrNotImplemented
}

func (d DummyService) IsExternalPrincipalsEnabled() bool {
	return false
}

type APIService struct {
	validateIDTokenClaims     map[string]string
	apiClient                 apiclient.ClientWithResponsesInterface
	logger                    logging.Logger
	externalPrincipalsEnabled bool
}

func NewAPIService(apiEndpoint string, validateIDTokenClaims map[string]string, logger logging.Logger, externalPrincipalsEnabled bool) (*APIService, error) {
	client, err := apiclient.NewClientWithResponses(apiEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication api client: %w", err)
	}

	res := &APIService{
		validateIDTokenClaims:     validateIDTokenClaims,
		apiClient:                 client,
		logger:                    logger,
		externalPrincipalsEnabled: externalPrincipalsEnabled,
	}
	return res, nil
}

func NewAPIServiceWithClients(apiClient apiclient.ClientWithResponsesInterface, logger logging.Logger, validateIDTokenClaims map[string]string, externalPrincipalsEnabled bool) (*APIService, error) {
	return &APIService{
		apiClient:                 apiClient,
		logger:                    logger,
		validateIDTokenClaims:     validateIDTokenClaims,
		externalPrincipalsEnabled: externalPrincipalsEnabled,
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

// ValidateSTS calls the external authentication service to validate the STS parameters
// validates the required claims and returns the external user id and expiration time
func (s *APIService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error) {
	res, err := s.apiClient.STSLoginWithResponse(ctx, apiclient.STSLoginJSONRequestBody{
		Code:        code,
		RedirectUri: redirectURI,
		State:       state,
	})
	if err != nil {
		return "", fmt.Errorf("failed to authenticate user: %w", err)
	}

	if err := s.validateResponse(res, http.StatusOK); err != nil {
		return "", fmt.Errorf("invalid authentication response: %w", err)
	}

	// validate claims
	claims := res.JSON200.Claims
	for claim, expectedValue := range s.validateIDTokenClaims {
		if claimValue, found := claims.Get(claim); !found || claimValue != expectedValue {
			return "", fmt.Errorf("claim %s has unexpected value %s: %w", claim, claimValue, ErrInsufficientPermissions)
		}
	}
	subject, found := claims.Get("sub")
	if !found {
		return "", fmt.Errorf("missing subject in claims: %w", ErrInsufficientPermissions)
	}
	return subject, nil
}

func (s *APIService) ExternalPrincipalLogin(ctx context.Context, identityRequest map[string]interface{}) (*apiclient.ExternalPrincipal, error) {
	if !s.IsExternalPrincipalsEnabled() {
		return nil, fmt.Errorf("external principals disabled: %w", ErrInvalidRequest)
	}
	resp, err := s.apiClient.ExternalPrincipalLoginWithResponse(ctx, identityRequest)
	if err != nil {
		return nil, fmt.Errorf("calling authenticate user: %w", err)
	}
	if resp.StatusCode() != http.StatusOK {
		switch resp.StatusCode() {
		case http.StatusBadRequest:
			return nil, ErrInvalidRequest
		case http.StatusUnauthorized:
			return nil, ErrInvalidTokenFormat
		case http.StatusForbidden:
			return nil, ErrSessionExpired
		default:
			return nil, fmt.Errorf("%w - got %d expected %d", ErrUnexpectedStatusCode, resp.StatusCode(), http.StatusOK)
		}
	}
	return resp.JSON200, nil
}

func (s *APIService) IsExternalPrincipalsEnabled() bool {
	return s.externalPrincipalsEnabled
}
