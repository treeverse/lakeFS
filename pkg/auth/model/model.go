package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
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
	KvUser
	AccessKeyID     string
	SecretAccessKey string
}

type User struct {
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

type KvUser struct {
	ID string
	User
}

type DBUser struct {
	ID int64 `db:"id"`
	User
}

type Group struct {
	ID          string    `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type KvGroup struct {
	ID string `default:"0"`
	Group
}

type DbGroup struct {
	ID int `db:"id"`
	Group
}

type Policy struct {
	CreatedAt   time.Time  `db:"created_at"`
	DisplayName string     `db:"display_name" json:"display_name"`
	Statement   Statements `db:"statement"`
}

type KvPolicy struct {
	ID string `default:"0"`
	Policy
}

type DbPolicy struct {
	ID int `db:"id"`
	Policy
}

type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource string   `json:"Resource"`
}

type Statements []Statement

type Credential struct {
	AccessKeyID                   string    `db:"access_key_id"`
	SecretAccessKey               string    `db:"-" json:"-"`
	SecretAccessKeyEncryptedBytes []byte    `db:"secret_access_key" json:"-"`
	IssuedDate                    time.Time `db:"issued_date"`
}

type KvCredential struct {
	UserID string `default:"0"`
	Credential
}

type DbCredential struct {
	UserID int64 `db:"user_id"`
	Credential
}

// CredentialKeys - For JSON serialization:
type CredentialKeys struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func (u *User) UpdatePassword(password string) error {
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
func (u *User) Authenticate(password string) error {
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
func userFromProto(pb *UserData) *KvUser {
	return &KvUser{
		ID: string(pb.Id),
		User: User{
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
func protoFromUser(u *KvUser) *UserData {
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
func groupFromProto(pb *GroupData) *KvGroup {
	return &KvGroup{
		ID: string(pb.Id),
		Group: Group{
			CreatedAt:   pb.CreatedAt.AsTime(),
			DisplayName: pb.DisplayName,
		},
	}
}

//nolint
func protoFromGroup(g *KvGroup) *GroupData {
	return &GroupData{
		Id:          []byte(g.ID),
		CreatedAt:   timestamppb.New(g.CreatedAt),
		DisplayName: g.DisplayName,
	}
}

//nolint
func kvPolicyPath(DisplayName string) string {
	return kv.FormatPath(authPrefix, "policies", DisplayName)
}

//nolint
func policyFromProto(pb *PolicyData) *KvPolicy {
	return &KvPolicy{
		ID: string(pb.Id),
		Policy: Policy{
			CreatedAt:   pb.CreatedAt.AsTime(),
			DisplayName: pb.DisplayName,
			Statement:   *statementsFromProto(pb.Statements),
		},
	}
}

//nolint
func protoFromPolicy(p *KvPolicy) *PolicyData {
	return &PolicyData{
		Id:          []byte(p.ID),
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
func credentialFromProto(pb *CredentialData) *KvCredential {
	return &KvCredential{
		UserID: string(pb.UserId),
		Credential: Credential{
			AccessKeyID:                   pb.AccessKeyId,
			SecretAccessKey:               pb.SecretAccessKey,
			SecretAccessKeyEncryptedBytes: pb.SecretAccessKeyEncryptedBytes,
			IssuedDate:                    pb.IssuedDate.AsTime(),
		},
	}
}

//nolint
func protoFromCredential(c *KvCredential) *CredentialData {
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
func kvPolicyToUser(policyDisplayName string, userName string) string {
	return kv.FormatPath(authPrefix, "users", userName, "policies", policyDisplayName)
}

//nolint
func kvPolicyToGroup(policyDisplayName string, groupDisplayName string) string {
	return kv.FormatPath(authPrefix, "groups", groupDisplayName, "policies", policyDisplayName)
}
