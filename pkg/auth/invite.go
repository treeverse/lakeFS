package auth

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

type InviteHandler interface {
	InviteUser(ctx context.Context, email string) error
	IsInviteSupported() bool
}

type EmailInviteHandler struct {
	svc     Service
	log     logging.Logger
	emailer *email.Emailer
}

func NewEmailInviteHandler(svc Service, log logging.Logger, emailer *email.Emailer) *EmailInviteHandler {
	return &EmailInviteHandler{svc: svc, log: log, emailer: emailer}
}

const (
	DefaultInvitePasswordExpiration = 6 * time.Hour
)

func (i *EmailInviteHandler) inviteUserRequest(emailAddr string) error {
	secret := i.svc.SecretStore().SharedSecret()
	currentTime := time.Now()
	token, err := GenerateJWTResetPassword(secret, emailAddr, currentTime, currentTime.Add(DefaultInvitePasswordExpiration))
	if err != nil {
		return err
	}
	params := map[string]string{
		"token": token,
		"email": emailAddr,
	}
	err = i.emailer.SendInviteUserEmail([]string{emailAddr}, params)
	if err != nil {
		return err
	}
	i.log.WithField("email", emailAddr).Info("invite email sent")
	return nil
}

func (i *EmailInviteHandler) InviteUser(ctx context.Context, email string) error {
	u := &model.User{
		CreatedAt: time.Now().UTC(),
		Username:  email,
		Source:    "internal",
		Email:     &email,
	}
	_, err := i.svc.CreateUser(ctx, u)
	if err != nil {
		return err
	}
	return i.inviteUserRequest(email)
}

func (i *EmailInviteHandler) IsInviteSupported() bool {
	return i.emailer != nil && i.emailer.Params.SMTPHost != ""
}
