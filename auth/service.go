package auth

type AuthenticationRequest struct {
	Credentials string
}

type AuthenticationResponse struct {
	AuthenticationError error
	ClientID            string
}

type AuthorizationRequest struct {
	ClientID string
}

type AuthorizationResponse struct {
}

type AuthService interface {
	Authenticate(req *AuthenticationRequest) (*AuthenticationResponse, error)
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}
