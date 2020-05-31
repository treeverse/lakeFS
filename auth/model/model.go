package model

import (
	"time"
)

const (
	RoleAdmin = "AdminRole"
)

type PaginationParams struct {
	PageToken string
	Amount    int
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

type Role struct {
	Id          int       `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type Policy struct {
	Id          int       `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
	Permission  string    `db:"permission" json:"permission"`
	Arn         string    `db:"arn" json:"arn"`
}

type UserGroups struct {
	UserId  int `db:"user_id"`
	GroupId int `db:"group_id"`
}

type UserRoles struct {
	UserId int `db:"user_id"`
	RoleId int `db:"role_id"`
}

type GroupRoles struct {
	GroupId int `db:"group_id"`
	RoleId  int `db:"role_id"`
}

type RolePolicies struct {
	RoleId   int `db:"role_id"`
	PolicyId int `db:"policy_id"`
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
