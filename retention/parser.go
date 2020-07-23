package retention

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/models"
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
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, a)
}

func ParseTimePeriod(model models.TimePeriod) (TimePeriodHours, error) {
	ret := TimePeriodHours(24 * (model.Days + 7*model.Weeks))
	if ret < TimePeriodHours(2) {
		return TimePeriodHours(0), fmt.Errorf("Minimal allowable expiration is 2 hours")
	}
	return ret, nil
}

func RenderTimePeriod(timePeriod TimePeriodHours) *models.TimePeriod {
	// Time periods are used for deletion, so safest to round them UP
	totalDays := (timePeriod + 23) / 24
	return &models.TimePeriod{Weeks: int32(totalDays / 7), Days: int32(totalDays % 7)}
}

func ParseExpiration(model models.RetentionPolicyRuleExpiration) (*Expiration, error) {
	ret := Expiration{}
	if model.All != nil {
		hours, err := ParseTimePeriod(*model.All)
		if err != nil {
			return nil, fmt.Errorf("expiration all: %s", err)
		}
		ret.All = &hours
	}
	if model.Noncurrent != nil {
		hours, err := ParseTimePeriod(*model.Noncurrent)
		if err != nil {
			return nil, fmt.Errorf("expiration noncurrent: %s", err)
		}
		ret.Noncurrent = &hours
	}
	if model.Uncommitted != nil {
		hours, err := ParseTimePeriod(*model.Uncommitted)
		if err != nil {
			return nil, fmt.Errorf("expiration uncommitted: %s", err)
		}
		ret.Uncommitted = &hours
	}
	if ret.All == nil && ret.Noncurrent == nil && ret.Uncommitted == nil {
		return nil, fmt.Errorf("expiration must specify at least one field")
	}
	return &ret, nil
}

func RenderExpiration(expiration *Expiration) *models.RetentionPolicyRuleExpiration {
	ret := models.RetentionPolicyRuleExpiration{}
	if expiration.All != nil {
		ret.All = RenderTimePeriod(*expiration.All)
	}
	if expiration.Noncurrent != nil {
		ret.Noncurrent = RenderTimePeriod(*expiration.Noncurrent)
	}
	if expiration.Uncommitted != nil {
		ret.Uncommitted = RenderTimePeriod(*expiration.Uncommitted)
	}
	return &ret
}

func ParseRule(model models.RetentionPolicyRule) (*Rule, error) {
	rule := Rule{}
	rule.Enabled = *model.Status == "enabled"
	if model.Filter == nil {
		rule.FilterPrefix = ""
	} else {
		rule.FilterPrefix = model.Filter.Prefix
	}
	if model.Expiration == nil {
		return nil, fmt.Errorf("Missing required expiration field")
	}
	expiration, err := ParseExpiration(*model.Expiration)
	if err != nil {
		return nil, err
	}
	rule.Expiration = *expiration
	return &rule, nil
}

var (
	Enabled  = "enabled"
	Disabled = "disabled"
)

func RenderRule(rule *Rule) *models.RetentionPolicyRule {
	ret := models.RetentionPolicyRule{}
	if rule.Enabled {
		ret.Status = &Enabled
	} else {
		ret.Status = &Disabled
	}
	if rule.FilterPrefix != "" {
		ret.Filter = &models.RetentionPolicyRuleFilter{Prefix: rule.FilterPrefix}
	}
	ret.Expiration = RenderExpiration(&rule.Expiration)
	return &ret
}

func ParsePolicy(model models.RetentionPolicy) (*Policy, error) {
	rules := make([]Rule, 0, len(model.Rules))

	for index, modelRule := range model.Rules {
		rule, err := ParseRule(*modelRule)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %s", index, err)
		}
		rules = append(rules, *rule)
	}

	return &Policy{Description: model.Description, Rules: rules}, nil
}

func RenderPolicy(policy *Policy) *models.RetentionPolicy {
	modelRules := make([]*models.RetentionPolicyRule, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		modelRules = append(modelRules, RenderRule(&rule))
	}
	return &models.RetentionPolicy{Description: policy.Description, Rules: modelRules}
}

// PolicyWithCreationDate never converted in, only out
func RenderPolicyWithCreationDate(policy *PolicyWithCreationTime) *models.RetentionPolicyWithCreationDate {
	serializableCreationDate := strfmt.DateTime(policy.CreatedAt)
	return &models.RetentionPolicyWithCreationDate{
		RetentionPolicy: *RenderPolicy(&policy.Policy),
		CreationDate:    &serializableCreationDate,
	}
}
