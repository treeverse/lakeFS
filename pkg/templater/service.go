package templater

import (
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"

	"context"
	"fmt"
	"io"
	"io/fs"
)

type Service interface {
	Expand(ctx context.Context, w io.Writer, user *model.User, templateName string, query map[string]string) error
}

type service struct {
	auth      AuthService
	expanders *ExpanderMap
}

func NewService(fs fs.FS, cfg *config.Config, auth AuthService) Service {
	return &service{auth: auth, expanders: NewExpanderMap(fs, cfg, auth)}
}

func (s *service) Expand(ctx context.Context, w io.Writer, user *model.User, templateName string, query map[string]string) error {
	e, err := s.expanders.Get(ctx, user.Username, templateName)
	if err != nil {
		return err
	}

	params := &Params{
		Controlled: &ControlledParams{
			Ctx:  ctx,
			Auth: s.auth,
			User: user,
		},
		Data: &UncontrolledData{
			Username: user.Username,
			Query:    query,
		},
	}
	err = e.Prepare(params)
	if err != nil {
		return err
	}
	err = e.Expand(w, params)
	if err != nil {
		return fmt.Errorf("writing template: %w", err)
	}
	return nil
}
