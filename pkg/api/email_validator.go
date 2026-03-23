package api

import (
	"net/mail"
	"strings"

	emailverifier "github.com/AfterShip/email-verifier"
	"golang.org/x/net/publicsuffix"
)

var verifier = emailverifier.NewVerifier().
	DisableSMTPCheck().
	DisableGravatarCheck().
	DisableDomainSuggest()

func domainFromEmail(email string) string {
	index := strings.LastIndex(email, "@")
	if index < 0 {
		return ""
	}
	return strings.ToLower(email[index+1:])
}

func domainHasKnownTLD(domain string) bool {
	eTLD, icann := publicsuffix.PublicSuffix(domain)
	if icann {
		return true
	}
	// Privately managed domains (eTLD contains a dot) still have a valid TLD.
	// Only reject unmanaged/made-up TLDs (single-label, no dot).
	return strings.IndexByte(eTLD, '.') >= 0
}

func isDisposableDomain(domain string) bool {
	return verifier.IsDisposable(domain)
}

func isFreeDomain(domain string) bool {
	return verifier.IsFreeDomain(domain)
}

func validateEmail(email string) error {
	if email == "" {
		return ErrEmailRequired
	}
	e, err := mail.ParseAddress(email)
	if err != nil {
		return ErrInvalidEmailAddress
	}
	// Reject RFC 5322 display names (e.g. "Alice <alice@example.com>").
	// We expect a bare address from form input.
	if e.Name != "" {
		return ErrInvalidEmailAddress
	}
	domain := domainFromEmail(e.Address)
	if !domainHasKnownTLD(domain) {
		return ErrInvalidEmailAddress
	}
	if isDisposableDomain(domain) {
		return ErrInvalidEmailAddress
	}
	if isFreeDomain(domain) {
		return ErrFreeEmailAddress
	}
	return nil
}
