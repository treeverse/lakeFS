package model

import "time"

type User struct {
	Id       int    `db:"id"`
	Email    string `db:"email"`
	FullName string `db:"full_name"`
}

type Application struct {
	Id          int    `db:"id"`
	DisplayName string `db:"display_name"`
}

type Group struct {
	Id          int    `db:"id"`
	DisplayName string `db:"display_name"`
}

type Role struct {
	Id          int    `db:"id"`
	DisplayName string `db:"display_name"`
}

type UserGroups struct {
	UserId  int `db:"user_id"`
	GroupId int `db:"group_id"`
}

type ApplicationGroups struct {
	ApplicationId int `db:"application_id"`
	GroupId       int `db:"group_id"`
}

type UserRoles struct {
	UserId int `db:"user_id"`
	RoleId int `db:"role_id"`
}

type ApplicationRoles struct {
	ApplicationId int `db:"application_id"`
	RoleId        int `db:"role_id"`
}

type GroupRoles struct {
	GroupId int `db:"group_id"`
	RoleId  int `db:"role_id"`
}

type Policy struct {
	Id         int    `db:"id"`
	Permission string `db:"permission"`
	Arn        string `db:"arn"`
}

type RolePolicies struct {
	RoleId   int `db:"role_id"`
	PolicyId int `db:"policy_id"`
}

const (
	CredentialTypeUser        = "user"
	CredentialTypeApplication = "application"
)

type Credential struct {
	AccessKeyId     string    `db:"access_key_id"`
	AccessSecretKey string    `db:"access_secret_key"`
	Type            string    `db:"credentials_type"`
	IssuedDate      time.Time `db:"issued_date"`
	UserId          *int      `db:"user_id"`
	ApplicationId   *int      `db:"application_id"`
}
