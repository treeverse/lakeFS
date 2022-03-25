package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	StatementEffectAllow = "allow"
	StatementEffectDeny  = "deny"
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

type User struct {
	ID        int       `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	Username  string    `db:"display_name" json:"display_name"`
	// FriendlyName, if set, is a shorter name for the user than
	// Username.  Unlike Username it does not identify the user (it
	// might not be unique); use it in the user's GUI rather than in
	// backend code.
	FriendlyName      *string `db:"friendly_name" json:"friendly_name"`
	Email             *string `db:"email" json:"email"`
	EncryptedPassword []byte  `db:"encrypted_password" json:"encrypted_password"`
	Source            string
}

// hashPassword generates a hashed password from a plaintext string
func hashPassword(password string) ([]byte, error) {
	pw, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	return pw, nil
}

func (u *User) UpdatePassword(password string) error {
	pw, err := hashPassword(password)
	if err != nil {
		return err
	}
	u.EncryptedPassword = pw
	return nil
}

// Authenticate a user from a password Returns nil on success, or an error on failure.
func (u *User) Authenticate(password string) error {
	return bcrypt.CompareHashAndPassword(u.EncryptedPassword, []byte(password))
}

// SuperuserConfiguration requests a particular configuration for a superuser.
type SuperuserConfiguration struct {
	User
	AccessKeyID     string
	SecretAccessKey string
}

type Group struct {
	ID          int       `db:"id"`
	CreatedAt   time.Time `db:"created_at"`
	DisplayName string    `db:"display_name" json:"display_name"`
}

type Policy struct {
	ID          int        `db:"id"`
	CreatedAt   time.Time  `db:"created_at"`
	DisplayName string     `db:"display_name" json:"display_name"`
	Statement   Statements `db:"statement"`
}

type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource string   `json:"Resource"`
}

type Statements []Statement

var (
	ErrInvalidStatementSrcFormat = errors.New("invalid statements src format")
)

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

type Credential struct {
	AccessKeyID                   string    `db:"access_key_id"`
	SecretAccessKey               string    `db:"-" json:"-"`
	SecretAccessKeyEncryptedBytes []byte    `db:"secret_access_key" json:"-"`
	IssuedDate                    time.Time `db:"issued_date"`
	UserID                        int       `db:"user_id"`
}

// For JSON serialization:
type CredentialKeys struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}
