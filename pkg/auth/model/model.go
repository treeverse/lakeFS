package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	StatementEffectAllow = "allow"
	StatementEffectDeny  = "deny"
)

var (
	ErrInvalidStatementSrcFormat = errors.New("invalid statements src format")
)

type PaginationParams struct {
	Prefix string
	After  string
	Amount int
}

// Paginator describes the parameters of a slice of data from a database.
type Paginator struct {
	Amount        int
	NextPageToken string
}

// SuperuserConfiguration requests a particular configuration for a superuser.
type SuperuserConfiguration struct {
	User
	AccessKeyID     string
	SecretAccessKey string
}

type BaseUser struct {
	CreatedAt time.Time `db:"created_at"`
	Username  string    `db:"display_name" json:"display_name"`
	// FriendlyName, if set, is a shorter name for the user than
	// Username.  Unlike Username it does not identify the user (it
	// might not be unique); use it in the user's GUI rather than in
	// backend code.
	FriendlyName      *string `db:"friendly_name" json:"friendly_name"`
	Email             *string `db:"email" json:"email"`
	EncryptedPassword []byte  `db:"encrypted_password" json:"encrypted_password"`
	Source            string  `db:"source" json:"source"`
}

type User struct {
	ID string
	BaseUser
}

type DBUser struct {
	ID int64 `db:"id"`
	BaseUser
}

func ConvertUser(u *DBUser) *User {
	return &User{
		ID:       ConvertDBID(u.ID),
		BaseUser: u.BaseUser,
	}
}

func ConvertDBID(id int64) string {
	const base = 10
	return strconv.FormatInt(id, base)
}

type BaseGroup struct {
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type Group struct {
	ID string
	BaseGroup
}

type DBGroup struct {
	ID int `db:"id"`
	BaseGroup
}

type BasePolicy struct {
	CreatedAt   time.Time  `db:"created_at"`
	DisplayName string     `db:"display_name" json:"display_name"`
	Statement   Statements `db:"statement"`
}

type DBPolicy struct {
	ID int `db:"id"`
	BasePolicy
}

type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource string   `json:"Resource"`
}

type Statements []Statement

type BaseCredential struct {
	AccessKeyID                   string    `db:"access_key_id"`
	SecretAccessKey               string    `db:"-" json:"-"`
	SecretAccessKeyEncryptedBytes []byte    `db:"secret_access_key" json:"-"`
	IssuedDate                    time.Time `db:"issued_date"`
}

type Credential struct {
	UserID string
	BaseCredential
}

type DBCredential struct {
	UserID int64 `db:"user_id"`
	BaseCredential
}

// CredentialKeys - For JSON serialization:
type CredentialKeys struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func (u *BaseUser) UpdatePassword(password string) error {
	pw, err := HashPassword(password)
	if err != nil {
		return err
	}
	u.EncryptedPassword = pw
	return nil
}

// HashPassword generates a hashed password from a plaintext string
func HashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// Authenticate a user from a password Returns nil on success, or an error on failure.
func (u *BaseUser) Authenticate(password string) error {
	return bcrypt.CompareHashAndPassword(u.EncryptedPassword, []byte(password))
}

func (s Statements) Value() (driver.Value, error) {
	if s == nil {
		return json.Marshal([]struct{}{})
	}
	return json.Marshal(s)
}

func (s *Statements) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	data, ok := src.([]byte)
	if !ok {
		return ErrInvalidStatementSrcFormat
	}
	return json.Unmarshal(data, s)
}

// authPrefix - key prefix in the KV model
const authPrefix = "auth"

//TODO - delete no lint comments when function is used
//nolint
func kvUserPath(userName string) string {
	return kv.FormatPath(authPrefix, "users", userName)
}

//nolint
func userFromProto(pb *UserData) *User {
	return &User{
		ID: string(pb.Id),
		BaseUser: BaseUser{
			CreatedAt:         pb.CreatedAt.AsTime(),
			Username:          pb.Username,
			FriendlyName:      &pb.FriendlyName,
			Email:             &pb.Email,
			EncryptedPassword: pb.EncryptedPassword,
			Source:            pb.Source,
		},
	}
}

//nolint
func protoFromUser(u *User) *UserData {
	return &UserData{
		Id:                []byte(u.ID),
		CreatedAt:         timestamppb.New(u.CreatedAt),
		Username:          u.Username,
		FriendlyName:      *u.FriendlyName,
		Email:             *u.Email,
		EncryptedPassword: u.EncryptedPassword,
		Source:            u.Source,
	}
}

//nolint
func kvGroupPath(displayName string) string {
	return kv.FormatPath(authPrefix, "groups", displayName)
}

//nolint
func groupFromProto(pb *GroupData) *Group {
	return &Group{
		ID: string(pb.Id),
		BaseGroup: BaseGroup{
			CreatedAt:   pb.CreatedAt.AsTime(),
			DisplayName: pb.DisplayName,
		},
	}
}

//nolint
func protoFromGroup(g *Group) *GroupData {
	return &GroupData{
		Id:          []byte(g.ID),
		CreatedAt:   timestamppb.New(g.CreatedAt),
		DisplayName: g.DisplayName,
	}
}

//nolint
func PolicyPath(DisplayName string) string {
	return kv.FormatPath(authPrefix, "policies", DisplayName)
}

//nolint
func policyFromProto(pb *PolicyData) *BasePolicy {
	return &BasePolicy{
		CreatedAt:   pb.CreatedAt.AsTime(),
		DisplayName: pb.DisplayName,
		Statement:   *statementsFromProto(pb.Statements),
	}
}

//nolint
func protoFromPolicy(p *BasePolicy) *PolicyData {
	return &PolicyData{
		CreatedAt:   timestamppb.New(p.CreatedAt),
		DisplayName: p.DisplayName,
		Statements:  protoFromStatements(&p.Statement),
	}
}

//nolint
func kvCredentialPath(userName string, accessKeyID string) string {
	return kv.FormatPath(authPrefix, userName, "credentials", accessKeyID)
}

//nolint
func credentialFromProto(pb *CredentialData) *Credential {
	return &Credential{
		UserID: string(pb.UserId),
		BaseCredential: BaseCredential{
			AccessKeyID:                   pb.AccessKeyId,
			SecretAccessKey:               pb.SecretAccessKey,
			SecretAccessKeyEncryptedBytes: pb.SecretAccessKeyEncryptedBytes,
			IssuedDate:                    pb.IssuedDate.AsTime(),
		},
	}
}

//nolint
func protoFromCredential(c *Credential) *CredentialData {
	return &CredentialData{
		AccessKeyId:                   c.AccessKeyID,
		SecretAccessKey:               c.SecretAccessKey,
		SecretAccessKeyEncryptedBytes: c.SecretAccessKeyEncryptedBytes,
		IssuedDate:                    timestamppb.New(c.IssuedDate),
		UserId:                        []byte(c.UserID),
	}
}

//nolint
func statementFromProto(pb *StatementData) *Statement {
	return &Statement{
		Effect:   pb.Effect,
		Action:   pb.Action,
		Resource: pb.Resource,
	}
}

//nolint
func protoFromStatement(s *Statement) *StatementData {
	return &StatementData{
		Effect:   s.Effect,
		Action:   s.Action,
		Resource: s.Resource,
	}
}

//nolint
func statementsFromProto(pb *StatementsData) *Statements {
	statements := make(Statements, len(pb.Statement))
	for i := range pb.Statement {
		statements[i] = *statementFromProto(pb.Statement[i])
	}
	return &statements
}

//nolint
func protoFromStatements(s *Statements) *StatementsData {
	statements := make([]*StatementData, len(*s))
	x := *s
	for i := range *s {
		statements[i] = protoFromStatement(&x[i])
	}
	return &StatementsData{
		Statement: statements,
	}
}

//nolint
func kvUserToGroup(userName string, groupDisplayName string) string {
	return kv.FormatPath(authPrefix, "groups", groupDisplayName, "users", userName)
}

//nolint
func PolicyToUser(policyDisplayName string, userName string) string {
	return kv.FormatPath(authPrefix, "users", userName, "policies", policyDisplayName)
}

//nolint
func PolicyToGroup(policyDisplayName string, groupDisplayName string) string {
	return kv.FormatPath(authPrefix, "groups", groupDisplayName, "policies", policyDisplayName)
}

func ConvertUsersList(users []*DBUser) []*User {
	kvUsers := make([]*User, 0, len(users))
	for _, u := range users {
		kvUsers = append(kvUsers, ConvertUser(u))
	}
	return kvUsers
}

func ConvertCredList(creds []*DBCredential) []*Credential {
	res := make([]*Credential, 0, len(creds))
	for _, c := range creds {
		res = append(res, ConvertCreds(c))
	}
	return res
}

func ConvertCreds(c *DBCredential) *Credential {
	return &Credential{
		UserID:         ConvertDBID(c.UserID),
		BaseCredential: c.BaseCredential,
	}
}

func ConvertGroupList(groups []*DBGroup) []*Group {
	res := make([]*Group, 0, len(groups))
	for _, g := range groups {
		res = append(res, ConvertGroup(g))
	}
	return res
}

func ConvertGroup(g *DBGroup) *Group {
	return &Group{
		ID:        strconv.Itoa(g.ID),
		BaseGroup: g.BaseGroup,
	}
}
