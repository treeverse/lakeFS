package email

import (
	"embed"
	"html/template"
	"net/url"
	"path"
	"strings"
)

const (
	ResetPasswordPath = "/auth/resetpassword" //#nosec
	InviteUserPath    = "/auth/users/create"  //#nosec

	ResetPasswordEmailSubject = "Reset Password Request for your lakeFS account"
	InviteUserWEmailSubject   = "You have been invited to lakeFS"
)

var (
	//go:embed invite_user_template.html reset_email_template.html
	f                  embed.FS
	inviteTemplate, _  = f.ReadFile("invite_user_template.html")
	resetTemplate, _   = f.ReadFile("reset_email_template.html")
	resetEmailTemplate = template.Must(template.New("resetEmailTemplate").Parse(string(resetTemplate)))
	inviteUserTemplate = template.Must(template.New("inviteUserTemplate").Parse(string(inviteTemplate)))
)

type TemplateParams struct {
	URL string
}

// buildURL takes in the baseURL that was configured for email purposes and adds to it the relevant path and params to
// create a URL string that directs to the proper page, while passing the nessacery params
func buildURL(baseURL string, pth string, qParams map[string]string) (*string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, pth)
	params := u.Query()
	for prm, value := range qParams {
		params.Add(prm, value)
	}
	u.RawQuery = params.Encode()
	url := u.String()
	return &url, nil
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
	l := TemplateParams{URL: *url}
	err = tmpl.Execute(&builder, l)
	if err != nil {
		return "", err
	}
	t := builder.String()
	return t, nil
}

func ConstructResetPasswordEmailTemplate(host string, token string) (string, error) {
	return buildEmailTemplate(resetEmailTemplate, host, ResetPasswordPath, token)
}

func ConstructInviteUserEmailTemplate(host string, token string) (string, error) {
	return buildEmailTemplate(inviteUserTemplate, host, InviteUserPath, token)
}
