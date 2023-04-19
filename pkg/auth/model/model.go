package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-openapi/swag"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/kv"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	StatementEffectAllow   = "allow"
	StatementEffectDeny    = "deny"
	PartitionKey           = "auth"
	PackageName            = "auth"
	groupsPrefix           = "groups"
	groupsUsersPrefix      = "gUsers"
	groupsPoliciesPrefix   = "gPolicies"
	usersPrefix            = "users"
	policiesPrefix         = "policies"
	usersPoliciesPrefix    = "uPolicies"
	usersCredentialsPrefix = "uCredentials" // #nosec G101 -- False positive: this is only a kv key prefix
	credentialsPrefix      = "credentials"
	expiredTokensPrefix    = "expiredTokens"
	metadataPrefix         = "installation_metadata"
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType("auth", "users", (&UserData{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", "policies", (&PolicyData{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", "groups", (&GroupData{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", kv.FormatPath("uCredentials", "*", "credentials"), (&CredentialData{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", kv.FormatPath("gUsers", "*", "users"), (&kv.SecondaryIndex{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", kv.FormatPath("gPolicies", "*", "policies"), (&kv.SecondaryIndex{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", kv.FormatPath("uPolicies", "*", "policies"), (&kv.SecondaryIndex{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", "expiredTokens", (&TokenData{}).ProtoReflect().Type())
	kv.MustRegisterType("auth", "installation_metadata", nil)
}

func UserPath(userName string) []byte {
	return []byte(kv.FormatPath(usersPrefix, userName))
}

func PolicyPath(displayName string) []byte {
	return []byte(kv.FormatPath(policiesPrefix, displayName))
}

func GroupPath(displayName string) []byte {
	return []byte(kv.FormatPath(groupsPrefix, displayName))
}

func CredentialPath(userName string, accessKeyID string) []byte {
	return []byte(kv.FormatPath(usersCredentialsPrefix, userName, credentialsPrefix, accessKeyID))
}

func GroupUserPath(groupDisplayName string, userName string) []byte {
	return []byte(kv.FormatPath(groupsUsersPrefix, groupDisplayName, usersPrefix, userName))
}

func UserPolicyPath(userName string, policyDisplayName string) []byte {
	return []byte(kv.FormatPath(usersPoliciesPrefix, userName, policiesPrefix, policyDisplayName))
}

func GroupPolicyPath(groupDisplayName string, policyDisplayName string) []byte {
	return []byte(kv.FormatPath(groupsPoliciesPrefix, groupDisplayName, policiesPrefix, policyDisplayName))
}

func ExpiredTokenPath(tokenID string) []byte {
	return []byte(kv.FormatPath(expiredTokensPrefix, tokenID))
}

func ExpiredTokensPath() []byte {
	return ExpiredTokenPath("")
}

func MetadataKeyPath(key string) string {
	return kv.FormatPath(metadataPrefix, key)
}

var ErrInvalidStatementSrcFormat = errors.New("invalid statements src format")

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

type User struct {
	CreatedAt time.Time `db:"created_at"`
	// Username is a unique identifier for the user. In password-based authentication, it is the email.
	Username string `db:"display_name" json:"display_name"`
	// FriendlyName, if set, is a shorter name for the user than
	// Username.  Unlike Username it does not identify the user (it
	// might not be unique); use it in the user's GUI rather than in
	// backend code.
	FriendlyName      *string `db:"friendly_name" json:"friendly_name"`
	Email             *string `db:"email" json:"email"`
	EncryptedPassword []byte  `db:"encrypted_password" json:"encrypted_password"`
	Source            string  `db:"source" json:"source"`
	ExternalID        *string `db:"external_id" json:"external_id"`
}

type DBUser struct {
	ID int64 `db:"id"`
	User
}

func ConvertDBID(id int64) string {
	const base = 10
	return strconv.FormatInt(id, base)
}

type Group struct {
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type DBGroup struct {
	ID int `db:"id"`
	Group
}

type ACLPermission string

type ACL struct {
	Permission ACLPermission `json:"permission"`
}

type Policy struct {
	CreatedAt   time.Time  `db:"created_at"`
	DisplayName string     `db:"display_name" json:"display_name"`
	Statement   Statements `db:"statement"`
	ACL         ACL        `db:"acl" json:"acl,omitempty"`
}

type DBPolicy struct {
	ID int `db:"id"`
	Policy
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
	Username string
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

func UserFromProto(pb *UserData) *User {
	return &User{
		CreatedAt:         pb.CreatedAt.AsTime(),
		Username:          pb.Username,
		FriendlyName:      &pb.FriendlyName,
		Email:             &pb.Email,
		EncryptedPassword: pb.EncryptedPassword,
		Source:            pb.Source,
		ExternalID:        &pb.ExternalId,
	}
}

func ProtoFromUser(u *User) *UserData {
	return &UserData{
		CreatedAt:         timestamppb.New(u.CreatedAt),
		Username:          u.Username,
		FriendlyName:      swag.StringValue(u.FriendlyName),
		Email:             swag.StringValue(u.Email),
		EncryptedPassword: u.EncryptedPassword,
		Source:            u.Source,
		ExternalId:        swag.StringValue(u.ExternalID),
	}
}

func GroupFromProto(pb *GroupData) *Group {
	return &Group{
		CreatedAt:   pb.CreatedAt.AsTime(),
		DisplayName: pb.DisplayName,
	}
}

func ProtoFromGroup(g *Group) *GroupData {
	return &GroupData{
		CreatedAt:   timestamppb.New(g.CreatedAt),
		DisplayName: g.DisplayName,
	}
}

func PolicyFromProto(pb *PolicyData) *Policy {
	policy := &Policy{
		CreatedAt:   pb.CreatedAt.AsTime(),
		DisplayName: pb.DisplayName,
		Statement:   *statementsFromProto(pb.Statements),
	}
	if pb.Acl != nil {
		policy.ACL = ACL{
			Permission: ACLPermission(pb.Acl.Permission),
		}
	}
	return policy
}

func ProtoFromPolicy(p *Policy) *PolicyData {
	return &PolicyData{
		CreatedAt:   timestamppb.New(p.CreatedAt),
		DisplayName: p.DisplayName,
		Statements:  protoFromStatements(&p.Statement),
		Acl: &ACLData{
			Permission: string(p.ACL.Permission),
		},
	}
}

func CredentialFromProto(s crypt.SecretStore, pb *CredentialData) (*Credential, error) {
	secret, err := DecryptSecret(s, pb.SecretAccessKeyEncryptedBytes)
	if err != nil {
		return nil, err
	}
	return &Credential{
		Username: string(pb.UserId),
		BaseCredential: BaseCredential{
			AccessKeyID:                   pb.AccessKeyId,
			SecretAccessKey:               secret,
			SecretAccessKeyEncryptedBytes: pb.SecretAccessKeyEncryptedBytes,
			IssuedDate:                    pb.IssuedDate.AsTime(),
		},
	}, nil
}

func ProtoFromCredential(c *Credential) *CredentialData {
	return &CredentialData{
		AccessKeyId:                   c.AccessKeyID,
		SecretAccessKeyEncryptedBytes: c.SecretAccessKeyEncryptedBytes,
		IssuedDate:                    timestamppb.New(c.IssuedDate),
		UserId:                        []byte(c.Username),
	}
}

func statementFromProto(pb *StatementData) *Statement {
	return &Statement{
		Effect:   pb.Effect,
		Action:   pb.Action,
		Resource: pb.Resource,
	}
}

func protoFromStatement(s *Statement) *StatementData {
	return &StatementData{
		Effect:   s.Effect,
		Action:   s.Action,
		Resource: s.Resource,
	}
}

func statementsFromProto(pb []*StatementData) *Statements {
	statements := make(Statements, len(pb))
	for i := range pb {
		statements[i] = *statementFromProto(pb[i])
	}
	return &statements
}

func protoFromStatements(s *Statements) []*StatementData {
	statements := make([]*StatementData, len(*s))
	x := *s
	for i := range *s {
		statements[i] = protoFromStatement(&x[i])
	}
	return statements
}

func ConvertUsersList(users []*DBUser) []*User {
	kvUsers := make([]*User, 0, len(users))
	for _, u := range users {
		kvUsers = append(kvUsers, &u.User)
	}
	return kvUsers
}

func ConvertUsersDataList(users []proto.Message) []*User {
	kvUsers := make([]*User, 0, len(users))
	for _, u := range users {
		a := u.(*UserData)
		kvUsers = append(kvUsers, UserFromProto(a))
	}
	return kvUsers
}

func ConvertCredList(creds []*DBCredential, username string) []*Credential {
	res := make([]*Credential, 0, len(creds))
	for _, c := range creds {
		res = append(res, &Credential{
			Username:       username,
			BaseCredential: c.BaseCredential,
		})
	}
	return res
}

func ConvertGroupList(groups []*DBGroup) []*Group {
	res := make([]*Group, 0, len(groups))
	for _, g := range groups {
		res = append(res, &g.Group)
	}
	return res
}

func ConvertGroupDataList(group []proto.Message) []*Group {
	res := make([]*Group, 0, len(group))
	for _, g := range group {
		a := g.(*GroupData)
		res = append(res, GroupFromProto(a))
	}
	return res
}

func ConvertPolicyDataList(policies []proto.Message) []*Policy {
	res := make([]*Policy, 0, len(policies))
	for _, p := range policies {
		a := p.(*PolicyData)
		res = append(res, PolicyFromProto(a))
	}
	return res
}

func ConvertCredDataList(s crypt.SecretStore, creds []proto.Message) ([]*Credential, error) {
	res := make([]*Credential, 0, len(creds))
	for _, c := range creds {
		credentialData := c.(*CredentialData)
		m, err := CredentialFromProto(s, credentialData)
		if err != nil {
			return nil, fmt.Errorf("credentials for %s: %w", credentialData.AccessKeyId, err)
		}
		m.SecretAccessKey = ""
		res = append(res, m)
	}
	return res, nil
}

func DecryptSecret(s crypt.SecretStore, value []byte) (string, error) {
	decrypted, err := s.Decrypt(value)
	if err != nil {
		return "", err
	}
	return string(decrypted), nil
}

func EncryptSecret(s crypt.SecretStore, secretAccessKey string) ([]byte, error) {
	encrypted, err := s.Encrypt([]byte(secretAccessKey))
	if err != nil {
		return nil, err
	}
	return encrypted, nil
}

func CreateID() string {
	return uuid.New().String()
}
