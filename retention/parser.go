package retention

import (
	"fmt"

	"github.com/treeverse/lakefs/api/gen/models"
)

// Avoid rounding by keeping whole hours (not Durations)
type TimePeriodHours int

type Expiration struct {
	All         *TimePeriodHours
	Uncommitted *TimePeriodHours
	Noncurrent  *TimePeriodHours
}

type Rule struct {
	Enabled      bool
	FilterPrefix string
	Expiration   Expiration
}

type Policy struct {
	Rules []Rule
}

type Service interface {
	GetPolicy(repositoryId string) (*models.RetentionPolicy, error)
	UpdatePolicy(repositoryId string, modelPolicy *models.RetentionPolicy) error
}

func ParseTimePeriod(model models.TimePeriod) (TimePeriodHours, error) {
	ret := TimePeriodHours(24 * (model.Days + 7*model.Weeks))
	if ret < TimePeriodHours(2) {
		return TimePeriodHours(0), fmt.Errorf("Minimal allowable expiration is 2 hours")
	}
	return ret, nil
}

func UnparseTimePeriod(timePeriod TimePeriodHours) *models.TimePeriod {
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

func UnparseExpiration(expiration *Expiration) *models.RetentionPolicyRuleExpiration {
	ret := models.RetentionPolicyRuleExpiration{}
	if expiration.All != nil {
		ret.All = UnparseTimePeriod(*expiration.All)
	}
	if expiration.Noncurrent != nil {
		ret.Noncurrent = UnparseTimePeriod(*expiration.Noncurrent)
	}
	if expiration.Uncommitted != nil {
		ret.Uncommitted = UnparseTimePeriod(*expiration.Uncommitted)
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

func UnparseRule(rule *Rule) *models.RetentionPolicyRule {
	ret := models.RetentionPolicyRule{}
	if rule.Enabled {
		ret.Status = &Enabled
	} else {
		ret.Status = &Disabled
	}
	if rule.FilterPrefix != "" {
		ret.Filter = &models.RetentionPolicyRuleFilter{Prefix: rule.FilterPrefix}
	}
	ret.Expiration = UnparseExpiration(&rule.Expiration)
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

	return &Policy{Rules: rules}, nil
}

func UnparsePolicy(policy *Policy) models.RetentionPolicy {
	modelRules := make([]*models.RetentionPolicyRule, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		modelRules = append(modelRules, UnparseRule(&rule))
	}
	return models.RetentionPolicy{Rules: modelRules}
}
