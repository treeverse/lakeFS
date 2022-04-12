package template

import (
	"strings"
	"text/template"
)

const (
	ResetPasswordPath  = "/auth/resetpassword?token="
	ResetEmailTemplate = `Hello, <br>
	A request has been received to change the password to your account,  <br>
	Click on this link to reset your password <br>
	{{.Host}}{{.ResetPasswordPath}}{{.Tkn}} <br>
	If you did not initiate this request you can please diregard this email. <br>
	Thanks  <br>
	The LakeFS team <br>
	`
	ResetPasswordEmailSubject = "Reset Password Request for your Lakefs account"
)

type Link struct {
	Host              string
	ResetPasswordPath string
	Tkn               string
}

func BuildResetPasswordEmailTemplate(tpl string, host string, token string) (string, error) {
	templ := template.New("resetPasswordtemplate")
	t := template.Must(templ.Parse(tpl))
	builder := &strings.Builder{}
	l := Link{Host: host, ResetPasswordPath: ResetPasswordPath, Tkn: token}
	err := t.Execute(builder, l)
	return builder.String(), err
}
