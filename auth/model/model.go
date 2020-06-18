package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

const (
	StatementEffectAllow = "Allow"
	StatementEffectDeny  = "Deny"
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

type Policy struct {
	Id          int        `db:"id"`
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
		return errors.New("invalid statements src format")
	}
	return json.Unmarshal(data, s)
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
