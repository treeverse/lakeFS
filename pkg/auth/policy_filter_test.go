package auth_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

func TestHasActionOnAnyResource(t *testing.T) {
	tests := []struct {
		name     string
		policies []*model.Policy
		action   string
		want     bool
	}{
		{
			name:     "no policies",
			policies: nil,
			action:   "fs:ListRepositories",
			want:     false,
		},
		{
			name: "wildcard action allows",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: "arn:lakefs:fs:::repository/some-repo",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name: "exact action match",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/specific-repo",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name: "different action no match",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ReadRepository"},
					Resource: "*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name: "deny statements ignored",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectDeny,
					Action:   []string{"fs:ListRepositories"},
					Resource: "*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name: "multiple policies one allows",
			policies: []*model.Policy{
				{Statement: model.Statements{{Effect: model.StatementEffectAllow, Action: []string{"fs:ReadRepository"}, Resource: "*"}}},
				{Statement: model.Statements{{Effect: model.StatementEffectAllow, Action: []string{"fs:ListRepositories"}, Resource: "arn:lakefs:fs:::repository/analytics-*"}}},
			},
			action: "fs:ListRepositories",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := auth.HasActionOnAnyResource(tt.policies, tt.action)
			if got != tt.want {
				t.Errorf("HasActionOnAnyResource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckPermission(t *testing.T) {
	tests := []struct {
		name        string
		resourceArn string
		username    string
		policies    []*model.Policy
		action      string
		want        bool
	}{
		{
			name:        "no policies - no access",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies:    nil,
			action:      "fs:ListRepositories",
			want:        false,
		},
		{
			name:        "empty policies - no access",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies:    []*model.Policy{},
			action:      "fs:ListRepositories",
			want:        false,
		},
		{
			name:        "allow all with wildcard resource",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: "*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "allow all with repository wildcard ARN",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "allow specific repository - match",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/repo1",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "allow specific repository - no match",
			resourceArn: "arn:lakefs:fs:::repository/other-repo",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/repo1",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name:        "deny takes precedence over allow",
			resourceArn: "arn:lakefs:fs:::repository/secret-repo",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Effect:   model.StatementEffectAllow,
						Action:   []string{"fs:*"},
						Resource: "*",
					},
					{
						Effect:   model.StatementEffectDeny,
						Action:   []string{"fs:ListRepositories"},
						Resource: "arn:lakefs:fs:::repository/secret-*",
					},
				},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name:        "deny specific repo - allow others",
			resourceArn: "arn:lakefs:fs:::repository/public-repo",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Effect:   model.StatementEffectAllow,
						Action:   []string{"fs:*"},
						Resource: "*",
					},
					{
						Effect:   model.StatementEffectDeny,
						Action:   []string{"fs:ListRepositories"},
						Resource: "arn:lakefs:fs:::repository/secret-*",
					},
				},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "wildcard repository pattern - match",
			resourceArn: "arn:lakefs:fs:::repository/analytics-prod",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/analytics-*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "wildcard repository pattern - no match",
			resourceArn: "arn:lakefs:fs:::repository/other-repo",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/analytics-*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name:        "user interpolation - match",
			resourceArn: "arn:lakefs:fs:::repository/user1-private",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/${user}-*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "user interpolation - no match different user",
			resourceArn: "arn:lakefs:fs:::repository/user2-private",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/${user}-*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name:        "multiple policies - allow from second policy",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{
				{
					Statement: model.Statements{{
						Effect:   model.StatementEffectAllow,
						Action:   []string{"fs:ReadRepository"},
						Resource: "arn:lakefs:fs:::repository/*",
					}},
				},
				{
					Statement: model.Statements{{
						Effect:   model.StatementEffectAllow,
						Action:   []string{"fs:ListRepositories"},
						Resource: "arn:lakefs:fs:::repository/*",
					}},
				},
			},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "action wildcard match",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:List*"},
					Resource: "arn:lakefs:fs:::repository/*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "action mismatch",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ReadRepository"},
					Resource: "arn:lakefs:fs:::repository/*",
				}},
			}},
			action: "fs:ListRepositories",
			want:   false,
		},
		{
			name:        "multiple resources in statement - array format",
			resourceArn: "arn:lakefs:fs:::repository/staging-data",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: `["arn:lakefs:fs:::repository/production-data", "arn:lakefs:fs:::repository/staging-data"]`,
				}},
			}},
			action: "fs:ListRepositories",
			want:   true,
		},
		{
			name:        "statements with conditions are skipped",
			resourceArn: "arn:lakefs:fs:::repository/repo1",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:    model.StatementEffectAllow,
					Action:    []string{"fs:ListRepositories"},
					Resource:  "arn:lakefs:fs:::repository/*",
					Condition: map[string]map[string][]string{"IpAddress": {"aws:SourceIp": {"192.168.1.0/24"}}},
				}},
			}},
			action: "fs:ListRepositories",
			want:   false, // Skipped because of condition
		},
		{
			name:        "non-repository resource - branch ARN",
			resourceArn: "arn:lakefs:fs:::repository/repo1/branch/main",
			username:    "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ReadBranch"},
					Resource: "arn:lakefs:fs:::repository/repo1/branch/*",
				}},
			}},
			action: "fs:ReadBranch",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := auth.CheckPermission(tt.resourceArn, tt.username, tt.policies, tt.action)
			if got != tt.want {
				t.Errorf("CheckPermission() = %v, want %v", got, tt.want)
			}
		})
	}
}
