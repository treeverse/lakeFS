package email

import (
	_ "embed"
	"html/template"
	"net/url"
	"path"
	"strings"
)

const (
	ResetPasswordURLPath = "/auth/resetpassword" //#nosec
	InviteUserURLPath    = "/auth/users/create"  //#nosec

	resetPasswordEmailSubject = "Reset Password Request for your lakeFS account"
	inviteUserWEmailSubject   = "You have been invited to lakeFS"
)

var (
	//go:embed invite_user_template.html
	invite string
	//go:embed reset_email_template.html
	reset              string
	resetEmailTemplate = template.Must(template.New("resetEmailTemplate").Parse(reset))
	inviteUserTemplate = template.Must(template.New("inviteUserTemplate").Parse(invite))
)

type TemplateParams struct {
	URL string
}

// buildURL takes in the baseURL that was configured for email purposes and adds to it the relevant path and params to
// create a URL string that directs to the proper page, while passing the nessacery params
func buildURL(baseURL string, pth string, values map[string]string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, pth)
	params := u.Query()
	for prm, value := range values {
		params.Add(prm, value)
	}
	u.RawQuery = params.Encode()
	url := u.String()
	return url, nil
}

func buildEmailTemplate(tmpl *template.Template, host string, path string, token string) (string, error) {
	params := map[string]string{
		"token": token,
	}
	url, err := buildURL(host, path, params)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	l := TemplateParams{URL: url}
	err = tmpl.Execute(&builder, l)
	if err != nil {
		return "", err
	}
	t := builder.String()
	return t, nil
}

func ConstructResetPasswordEmailTemplate(host string, token string) (string, error) {
	return buildEmailTemplate(resetEmailTemplate, host, ResetPasswordURLPath, token)
}

func ConstructInviteUserEmailTemplate(host string, token string) (string, error) {
	return buildEmailTemplate(inviteUserTemplate, host, InviteUserURLPath, token)
}
