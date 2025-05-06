package externalidp

import "github.com/treeverse/lakefs/pkg/authentication/externalidp/awsiam"

type Provider interface {
	NewRequest() *TokenInfo
}
type TokenInfo struct {
	AWSInfo *awsiam.AWSIdentityTokenInfo
}
