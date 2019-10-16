package gateway

import (
	"net/http"
	"regexp"
	"versio-index/auth/model"
)

const (
	AuthorizationHeader = "Authorization"
)

var (
	AuthorizationHeaderRegexp = regexp.MustCompile("^AWS (?P<accessKeyId>[\\w\\-]+):(?P<signature>[a-fA-F]+)$")
)

// server implementation of sigv2 and sigv4
type UserScope struct {
	Client *model.Client
}

// this is the signature we're using to extract credentials from API requests
type credentialExtractor func(req *http.Request) (*UserScope, error)

// SIGv2 implementation
func extractCredentialsSigv2(req *http.Request) (*UserScope, error) {
	// extract header information
	//authorization := req.Header.Get(AuthorizationHeader)
	//subs := AuthorizationHeaderRegexp.FindStringSubmatch(authorization)
	//result := make(map[string]string
	//for i, name := range AuthorizationHeaderRegexp.SubexpNames() {
	//	if i != 0 && name != "" {
	//		result[name] = subs[i]
	//	}
	//}
	//
	//accessKeyId, exists := result["accessKeyId"]
	//if !exists {
	//	return nil, ErrAuthenticationMissing
	//}
	//signatureString, exists := result["signature"]
	//if !exists {
	//	return nil, ErrAuthenticationMissing
	//}
	//
	//// generate a signature ourselves and compare it to the secret
	//if !strings.EqualFold(v2Sign(req), strings.ToLower(signatureString)) {
	//	return nil, ErrAuthenticationMissing
	//}
	return nil, ErrAuthenticationMissing
}

// SIGv4 implementation
func extractCredentialsSigv4(req *http.Request) (*UserScope, error) {
	return &UserScope{
		Client: &model.Client{
			Id:   "foobar",
			Name: "Foo Bar",
		},
	}, nil
}

// composition of SIGv4 and SIGv2
func extractCredentials(req *http.Request) (*UserScope, error) {
	credentials, err := extractCredentialsSigv4(req)
	if err != nil {
		credentials, err = extractCredentialsSigv2(req)
		if err != nil {
			return nil, err
		}
	}
	return credentials, nil
}

func getScope(req *http.Request) *UserScope {
	return &UserScope{
		Client: &model.Client{
			Id:   "foobar",
			Name: "Foo Bar",
		},
	}
}
