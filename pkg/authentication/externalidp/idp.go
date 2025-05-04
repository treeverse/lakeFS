package authentication

import (
	"context"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type LoginResponse struct {
	Token *apigen.AuthenticationToken
}
type ExternalPrincipalLoginClient interface {
	ExternalPrincipalLoginWithResponse(ctx context.Context, body apigen.ExternalPrincipalLoginJSONRequestBody, reqEditors ...apigen.RequestEditorFn) (*apigen.ExternalPrincipalLoginResponse, error)
}
type Provider interface {
	Login() (LoginResponse, error)
}
