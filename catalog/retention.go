package catalog

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

// Avoid rounding by keeping whole hours (not Durations)
type TimePeriodHours int

type Expiration struct {
	All         *TimePeriodHours `json:",omitempty"`
	Uncommitted *TimePeriodHours `json:",omitempty"`
	Noncurrent  *TimePeriodHours `json:",omitempty"`
}

type Rule struct {
	Enabled      bool
	FilterPrefix string `json:",omitempty"`
	Expiration   Expiration
}

type Rules []Rule

type Policy struct {
	Rules       Rules
	Description string
}

type PolicyWithCreationTime struct {
	Policy
	CreatedAt time.Time `db:"created_at"`
}

// RulesHolder is a dummy struct for helping pg serialization: it has
// poor support for passing an array-valued parameter.
type RulesHolder struct {
	Rules Rules
}

func (a *RulesHolder) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *Rules) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return ErrByteSliceTypeAssertion
	}
	return json.Unmarshal(b, a)
}

type StringIterator interface {
	Next() bool
	Err() error
	Read() (string, error)
	Close()
}
