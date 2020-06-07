package model

import (
	"time"

	"github.com/jackc/pgtype"
)

type PaginationParams struct {
	After  string
	Amount int
}

type Paginator struct {
	Amount        int
	NextPageToken string
}

type User struct {
	Id          int       `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type Group struct {
	Id          int       `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type PolicyDBImpl struct {
	Id          int              `db:"id"`
	CreatedAt   time.Time        `db:"created_at"`
	DisplayName string           `db:"display_name" json:"display_name"`
	Action      pgtype.TextArray `db:"action" json:"action"`
	Resource    string           `db:"resource" json:"resource"`
	Effect      bool             `db:"effect" json:"effect"`
}

func (p *PolicyDBImpl) ToModel() *Policy {
	var actions []string
	_ = p.Action.AssignTo(&actions)
	return &Policy{
		Id:          p.Id,
		CreatedAt:   p.CreatedAt,
		DisplayName: p.DisplayName,
		Action:      actions,
		Resource:    p.Resource,
		Effect:      p.Effect,
	}
}

type Policy struct {
	Id          int
	CreatedAt   time.Time
	DisplayName string
	Action      []string
	Resource    string
	Effect      bool
}

func (p *Policy) ToDBImpl() *PolicyDBImpl {
	actions := pgtype.TextArray{}
	_ = actions.Set(p.Action)
	return &PolicyDBImpl{
		Id:          p.Id,
		CreatedAt:   p.CreatedAt,
		DisplayName: p.DisplayName,
		Action:      actions,
		Resource:    p.Resource,
		Effect:      p.Effect,
	}
}

type Credential struct {
	AccessKeyId                   string    `db:"access_key_id"`
	AccessSecretKey               string    `json:"-"`
	AccessSecretKeyEncryptedBytes []byte    `db:"access_secret_key" json:"-"`
	IssuedDate                    time.Time `db:"issued_date"`
	UserId                        int       `db:"user_id"`
}

// For JSON serialization:
type CredentialKeys struct {
	AccessKeyId     string `json:"access_key_id"`
	AccessSecretKey string `json:"access_secret_key"`
}
