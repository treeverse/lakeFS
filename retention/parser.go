package retention

import (
	"errors"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/catalog"
)

const (
	hoursInADay             = 24
	daysInAWeek             = 7
	hoursMinAllowExpiration = 2
)

var (
	ErrExpirationNotSet               = errors.New("expiration must specify at least one field")
	ErrExpirationMinimalAllowable     = errors.New("minimal allowable expiration")
	ErrExpirationMissingRequiredField = errors.New("missing required expiration field")
)

func ParseTimePeriod(model models.TimePeriod) (catalog.TimePeriodHours, error) {
	ret := catalog.TimePeriodHours(hoursInADay * (model.Days + 7*model.Weeks))
	if ret < catalog.TimePeriodHours(hoursMinAllowExpiration) {
		return catalog.TimePeriodHours(0), fmt.Errorf("%w is %d hours", ErrExpirationMinimalAllowable, hoursMinAllowExpiration)
	}
	return ret, nil
}

func RenderTimePeriod(timePeriod catalog.TimePeriodHours) *models.TimePeriod {
	// Time periods are used for deletion, so safest to round them UP
	totalDays := (timePeriod + hoursInADay - 1) / hoursInADay
	return &models.TimePeriod{Weeks: int32(totalDays / daysInAWeek), Days: int32(totalDays % daysInAWeek)}
}

func ParseExpiration(model models.RetentionPolicyRuleExpiration) (*catalog.Expiration, error) {
	ret := catalog.Expiration{}
	if model.All != nil {
		hours, err := ParseTimePeriod(*model.All)
		if err != nil {
			return nil, fmt.Errorf("expiration all: %w", err)
		}
		ret.All = &hours
	}
	if model.Noncurrent != nil {
		hours, err := ParseTimePeriod(*model.Noncurrent)
		if err != nil {
			return nil, fmt.Errorf("expiration noncurrent: %w", err)
		}
		ret.Noncurrent = &hours
	}
	if model.Uncommitted != nil {
		hours, err := ParseTimePeriod(*model.Uncommitted)
		if err != nil {
			return nil, fmt.Errorf("expiration uncommitted: %w", err)
		}
		ret.Uncommitted = &hours
	}
	if ret.All == nil && ret.Noncurrent == nil && ret.Uncommitted == nil {
		return nil, ErrExpirationNotSet
	}
	return &ret, nil
}

func RenderExpiration(expiration *catalog.Expiration) *models.RetentionPolicyRuleExpiration {
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

func ParseRule(model models.RetentionPolicyRule) (*catalog.Rule, error) {
	rule := catalog.Rule{}
	rule.Enabled = *model.Status == "enabled"
	if model.Filter == nil {
		rule.FilterPrefix = ""
	} else {
		rule.FilterPrefix = model.Filter.Prefix
	}
	if model.Expiration == nil {
		return nil, ErrExpirationMissingRequiredField
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

func RenderRule(rule *catalog.Rule) *models.RetentionPolicyRule {
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

func ParsePolicy(model models.RetentionPolicy) (*catalog.Policy, error) {
	rules := make([]catalog.Rule, 0, len(model.Rules))

	for index, modelRule := range model.Rules {
		rule, err := ParseRule(*modelRule)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", index, err)
		}
		rules = append(rules, *rule)
	}

	return &catalog.Policy{Description: model.Description, Rules: rules}, nil
}

func RenderPolicy(policy *catalog.Policy) *models.RetentionPolicy {
	modelRules := make([]*models.RetentionPolicyRule, 0, len(policy.Rules))
	for i := range policy.Rules {
		modelRules = append(modelRules, RenderRule(&policy.Rules[i]))
	}
	return &models.RetentionPolicy{Description: policy.Description, Rules: modelRules}
}

// PolicyWithCreationDate never converted in, only out
func RenderPolicyWithCreationDate(policy *catalog.PolicyWithCreationTime) *models.RetentionPolicyWithCreationDate {
	serializableCreationDate := strfmt.DateTime(policy.CreatedAt)
	return &models.RetentionPolicyWithCreationDate{
		RetentionPolicy: *RenderPolicy(&policy.Policy),
		CreationDate:    &serializableCreationDate,
	}
}
