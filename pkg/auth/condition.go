package auth

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

const (
	OperatorNameIpAddress = "IpAddress"
)

// ConditionContext holds contextual information for condition evaluation
// Fields is a map of field names to their string values (e.g., {"SourceIp": "203.0.113.5", "VpcId": "vpc-123"})
type ConditionContext struct {
	Fields map[string]string
}

// ConditionOperator defines the interface for different condition operators
type ConditionOperator interface {
	// Evaluate checks if the condition fields and values match the context
	// fields is a map of field names to arrays of values (e.g., {"SourceIp": ["10.0.0.0/8", "192.168.1.0/24"]})
	Evaluate(fields map[string][]string, conditionCtx *ConditionContext) (bool, error)
	// Validate checks if the condition fields and values are valid
	Validate(fields map[string][]string) error
}

// IpAddressOperator handles IP address matching with CIDR notation support
// Dynamically checks all field names that contain IP addresses in the condition
type IpAddressOperator struct{}

// Validate implements ConditionOperator.
func (op *IpAddressOperator) Validate(fields map[string][]string) error {
	// Validate operator-specific constraints
	for field, values := range fields {
		// Field name can't be empty
		if field == "" {
			return fmt.Errorf("missing field name")
		}
		// Validate IP/CIDR format
		for _, value := range values {
			if _, _, err := net.ParseCIDR(value); err != nil {
				if net.ParseIP(value) == nil {
					return fmt.Errorf("invalid IP/CIDR format in %s for '%s': %s",
						OperatorNameIpAddress, field, value)
				}
			}
		}
	}
	return nil
}

// Evaluate checks if the client IP matches any of the IP fields in the condition
// It iterates over all field names and checks them against context
func (op *IpAddressOperator) Evaluate(fields map[string][]string, conditionCtx *ConditionContext) (bool, error) {
	// If no fields specified in condition, the condition passes
	if len(fields) == 0 {
		return true, nil
	}
	if conditionCtx == nil {
		return false, fmt.Errorf("invalid condition context")
	}

	// Check each field in the condition against context values (AND logic between fields)
	for fieldName, conditionValues := range fields {
		// No field or empty value for the field - condition fails
		contextValue, hasField := conditionCtx.Fields[fieldName]
		if !hasField {
			return false, nil
		}
		if contextValue == "" {
			return false, nil
		}

		// Parse the context value as an IP address
		contextIP := net.ParseIP(contextValue)
		if contextIP == nil {
			return false, fmt.Errorf("invalid IP format in field %s: %s", fieldName, contextValue)
		}

		// Check if context IP matches any of the condition values for this field (OR logic within field)
		fieldMatched := false
		for _, value := range conditionValues {
			value = strings.TrimSpace(value)

			// First try to parse as CIDR
			if _, ipNet, err := net.ParseCIDR(value); err == nil {
				if ipNet.Contains(contextIP) {
					fieldMatched = true
					break
				}
				continue
			}

			// Try to parse as a single IP address
			if singleIP := net.ParseIP(value); singleIP != nil {
				if contextIP.Equal(singleIP) {
					fieldMatched = true
					break
				}
				continue
			}

			// Invalid format
			return false, fmt.Errorf("invalid IP/CIDR format in field %s: %s", fieldName, value)
		}

		// If this field didn't match any values, entire condition fails (AND logic between fields)
		if !fieldMatched {
			return false, nil
		}
	}

	// All fields matched
	return true, nil
}

// OperatorFactory returns the appropriate operator for a given operator name
func OperatorFactory(operatorName string) (ConditionOperator, error) {
	switch operatorName {
	case OperatorNameIpAddress:
		return &IpAddressOperator{}, nil
	default:
		return nil, fmt.Errorf("unsupported condition operator: %s", operatorName)
	}
}

// EvaluateConditions checks if all conditions in the map are satisfied
// conditions is a map where keys are operator names and values are maps of field names to value arrays
// AWS IAM format: {"IpAddress": {"SourceIp": ["203.0.113.0/24", "198.51.100.25/32"]}}
// Returns true only if all conditions pass (AND logic)
func EvaluateConditions(conditions map[string]map[string][]string, conditionCtx *ConditionContext) (bool, error) {
	if len(conditions) == 0 {
		// No conditions mean always pass
		return true, nil
	}

	// Validate context
	if conditionCtx == nil {
		return false, fmt.Errorf("invalid condition context: context is nil")
	}

	// All conditions must pass (AND logic)
	for operatorName, fields := range conditions {
		operator, err := OperatorFactory(operatorName)
		if err != nil {
			return false, err
		}

		passed, err := operator.Evaluate(fields, conditionCtx)
		if err != nil {
			return false, err
		}

		if !passed {
			// One condition failed, entire statement fails
			return false, nil
		}
	}

	// All conditions passed
	return true, nil
}

// ExtractClientIP extracts the client IP from the given headers
// Checks X-Forwarded-For header first (for proxy/load balancer scenarios)
func ExtractClientIP(headers http.Header, remoteAddr string) string {
	// Try X-Forwarded-For header first
	xForwardedFor := headers.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		// X-Forwarded-For can contain multiple IPs, use the first one
		firstIP, _, _ := strings.Cut(xForwardedFor, ",")
		if firstIP != "" {
			return strings.TrimSpace(firstIP)
		}
	}

	// Try X-Real-IP header as fallback
	xRealIP := headers.Get("X-Real-IP")
	if xRealIP != "" {
		return strings.TrimSpace(xRealIP)
	}

	// Fall back to remote address, strip port if present
	if remoteAddr != "" {
		if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
			return host
		}
		return remoteAddr
	}

	return ""
}

// NewConditionContext creates a ConditionContext with the client IP in the SourceIp field
// This is the standard way to enrich context with client IP for IpAddress conditions
func NewConditionContext(clientIP string) *ConditionContext {
	return &ConditionContext{
		Fields: map[string]string{
			"SourceIp": clientIP,
		},
	}
}

// NewConditionContextWithFields creates a ConditionContext with custom field values
// This allows flexibility for future condition operators that may need different fields
func NewConditionContextWithFields(fields map[string]string) *ConditionContext {
	return &ConditionContext{
		Fields: fields,
	}
}
