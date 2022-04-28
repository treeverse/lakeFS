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
	inviteEmailContent string
	inviteUserTemplate = template.Must(template.New("inviteUserTemplate").Parse(inviteEmailContent))

	//go:embed reset_email_template.html
	resetEmailContent  string
	resetEmailTemplate = template.Must(template.New("resetEmailTemplate").Parse(resetEmailContent))
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
	return u.String(), nil
}

func buildEmailByTemplate(tmpl *template.Template, host string, path string, params map[string]string) (string, error) {
	u, err := buildURL(host, path, params)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	l := TemplateParams{URL: u}
	err = tmpl.Execute(&builder, l)
	if err != nil {
		return "", err
	}
	t := builder.String()
	return t, nil
}
