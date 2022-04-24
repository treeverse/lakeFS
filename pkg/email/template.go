package email

import (
	"html/template"
	"net/url"
	"strings"
)

const (
	ResetPasswordPath = "/auth/resetpassword" //#nosec
	InviteUserPath    = "/auth/users/create"  //#nosec

	ResetPasswordEmailSubject = "Reset Password Request for your LakeFS account"
	InviteUserWEmailSubject   = "You have been invited to LakeFS"

	inviteUserTemplate = `Hello, <br>
You have been invited to join LakeFS <br>
Click on this link to set up your account <br>
{{.URL}} <br>
Thanks <br>
The LakeFS team <br>
`

	resetEmailTemplate = `Hello, <br>
A request has been received to change the password to your account,  <br>
Click on this link to reset your password <br>
{{.URL}} <br>
If you did not initiate this request you can please disregard this email. <br>
Thanks  <br>
The LakeFS team <br>
	`
)

var (
	templ              = template.New("template")
	ResetEmailTemplate = template.Must(templ.Parse(resetEmailTemplate))
	InviteUserTemplate = template.Must(templ.Parse(inviteUserTemplate))
)

type Link struct {
	URL string
}

func buildURL(host string, path string, qParams map[string]string) (*string, error) {
	baseURL, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	baseURL.Path = path
	params := url.Values{}
	for prm, value := range qParams {
		params.Add(prm, value)
	}
	baseURL.RawQuery = params.Encode()
	url := baseURL.String()
	return &url, nil
}

func BuildEmailTemplate(tmpl *template.Template, host string, path string, token string) (*string, error) {
	builder := &strings.Builder{}
	params := make(map[string]string)
	params["token"] = token
	url, err := buildURL(host, path, params)
	if err != nil {
		return nil, err
	}
	l := Link{URL: *url}
	err = tmpl.Execute(builder, l)
	if err != nil {
		return nil, err
	}
	t := builder.String()
	return &t, nil
}
