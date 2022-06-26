package auth

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

type InviteHandler struct {
	secretStore crypt.SecretStore
	svc         Service
	log         logging.Logger
	emailer     *email.Emailer
}

const (
	DefaultInvitePasswordExpiration = 6 * time.Hour
)

func (i *InviteHandler) inviteUserRequest(emailAddr string) error {
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

func (i *InviteHandler) InviteUser(ctx context.Context, email string) error {
	u := &model.BaseUser{
		CreatedAt:    time.Now().UTC(),
		Username:     email,
		FriendlyName: nil,
		Source:       "internal",
		Email:        &email,
	}
	_, err := i.svc.CreateUser(ctx, u)
	if err != nil {
		return err
	}
	return i.inviteUserRequest(*u.Email)
}

func (i *InviteHandler) IsInviteSupported() bool {
	return i.emailer != nil
}
