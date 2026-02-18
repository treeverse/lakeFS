package auth

import (
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
)

// HasActionOnAnyResource checks if a user has a specific action on ANY resource.
// This is used for list-type operations where we want to verify the user has
// some permission before filtering results, rather than requiring wildcard access.
// Returns true if the user has at least one allow statement for the action.
func HasActionOnAnyResource(policies []*model.Policy, action string) bool {
	for _, policy := range policies {
		for _, stmt := range policy.Statement {
			if stmt.Effect != model.StatementEffectAllow {
				continue
			}
			for _, stmtAction := range stmt.Action {
				if wildcard.Match(stmtAction, action) {
					return true
				}
			}
		}
	}
	return false
}

// CheckPermission checks if a user has a specific action permission on a resource.
// Returns true if allowed, false if denied or not permitted.
// This evaluates policies similar to CheckPermissions but optimized for filtering.
func CheckPermission(resourceArn, username string, policies []*model.Policy, action string) bool {
	// Track if we found any allow statement
	hasAllow := false

	for _, policy := range policies {
		for _, stmt := range policy.Statement {
			// Skip statements with conditions for filtering
			// Conditions typically require request context (IP, time, etc.)
			if len(stmt.Condition) > 0 {
				continue
			}

			// Parse resources (handles both single string and JSON array)
			resources, err := ParsePolicyResourceAsList(stmt.Resource)
			if err != nil {
				continue // Skip invalid resources
			}

			for _, resource := range resources {
				// Interpolate ${user} placeholder
				resource = interpolateUser(resource, username)

				// Check if resource matches the target ARN
				if !ArnMatch(resource, resourceArn) {
					continue
				}

				// Check if action matches
				for _, stmtAction := range stmt.Action {
					if !wildcard.Match(stmtAction, action) {
						continue
					}

					// Found a matching statement
					if stmt.Effect == model.StatementEffectDeny {
						// Deny takes precedence - immediately return false
						return false
					}
					// Allow statement found
					hasAllow = true
				}
			}
		}
	}

	return hasAllow
}
