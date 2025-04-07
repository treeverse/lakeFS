/*
Package for licensing functionality. For OSS version this is a minimal implementation
*/
package license

type Manager interface {
	ValidateLicense() error
	GetToken() (string, error)
	InstallationID() string
}

// NewNopLicenseManager creates and returns a new instance of NopLicenseManager.
func NewNopLicenseManager() *NopLicenseManager {
	return &NopLicenseManager{}
}

type NopLicenseManager struct {
}

func (n *NopLicenseManager) ValidateLicense() error {
	return nil
}

func (n *NopLicenseManager) GetToken() (string, error) {
	return "", nil
}

func (n *NopLicenseManager) InstallationID() string {
	return ""
}
