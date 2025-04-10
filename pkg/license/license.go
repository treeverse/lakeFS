/*
Package license contains enterprise licensing functionality. For OSS version this is a minimal implementation
*/
package license

import (
	"errors"
)

type Manager interface {
	ValidateLicense() error
	GetToken() (string, error)
	InstallationID() string
}

var ErrNotImplemented = errors.New("not implemented")

// NopLicenseManager is a No-Operation implementation of license manager i.e. does not actually check license restrictions
type NopLicenseManager struct {
}

func (n *NopLicenseManager) ValidateLicense() error {
	return nil
}

func (n *NopLicenseManager) GetToken() (string, error) {
	return "", ErrNotImplemented
}

func (n *NopLicenseManager) InstallationID() string {
	return ""
}
