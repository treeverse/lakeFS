package internalidp

import (
	"context"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/logging"
)

type JWTAuthClient struct {
	Client *apigen.ClientWithResponses
}

// LoginClient has all the methods used during a token-based login.
type LoginClient interface {
	// GetToken returns a valid token, refreshing it if needed.
	GetToken(ctx context.Context) (string, error)
}

// FixedLoginClient just returns the same token each time, which it does not refresh.
type FixedLoginClient struct {
	token string
}

func NewFixedLoginClient(token string) *FixedLoginClient {
	return &FixedLoginClient{token}
}

func (c *FixedLoginClient) GetToken(_ context.Context) (string, error) {
	return c.token, nil
}

// WithLoginTokenAuth adds an authentication provider into the GetClient request, which authenticates
// using the JWT token.  It can update its token.
//
// BUG(ariels): It does not (yet) update its token.
func WithLoginTokenAuth(logger logging.Logger, client LoginClient) apigen.ClientOption {
	intercept := NewJWTClientInterceptor(logger, client)
	return apigen.WithRequestEditorFn(intercept)
}

func NewJWTClientInterceptor(logger logging.Logger, client LoginClient) apigen.RequestEditorFn {
	return func(ctx context.Context, req *http.Request) error {
		token, err := client.GetToken(ctx)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		return nil
	}
}
