package auth

import (
	"testing"
)

func TestIpAddressOperators_Evaluate(t *testing.T) {
	tests := []struct {
		name                 string
		fields               map[string][]string
		contextMap           map[string]string
		shouldErr            bool
		expectedIpAddress    bool // Expected result for IpAddress (negate=false)
		expectedNotIpAddress bool // Expected result for NotIpAddress (negate=true)
	}{
		{
			name:                 "Single IP match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: match = allow
			expectedNotIpAddress: false, // NotIpAddress: match = deny
		},
		{
			name:                 "Single IP no match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: no match = deny
			expectedNotIpAddress: true,  // NotIpAddress: no match = allow
		},
		{
			name:                 "CIDR block match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "10.255.255.255"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "CIDR block no match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "11.0.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: true,
		},
		{
			name:                 "Multiple values OR logic - first matches",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: any match = allow
			expectedNotIpAddress: false, // NotIpAddress: any match = deny
		},
		{
			name:                 "Multiple values OR logic - second matches",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: any match = allow
			expectedNotIpAddress: false, // NotIpAddress: any match = deny
		},
		{
			name:                 "Multiple values OR logic - none match",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: no match = deny
			expectedNotIpAddress: true,  // NotIpAddress: no match = allow
		},
		{
			name:                 "Invalid IP format in condition",
			fields:               map[string][]string{"SourceIp": {"invalid.ip"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            true,
			expectedIpAddress:    false, // Error cases same for both
			expectedNotIpAddress: false,
		},
		{
			name:                 "Missing field in context",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"OtherField": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Empty field value in context",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": ""},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Dynamic field - custom IP field",
			fields:               map[string][]string{"ClientIp": {"203.0.113.0/24"}},
			contextMap:           map[string]string{"ClientIp": "203.0.113.5"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Dynamic field - alternate naming",
			fields:               map[string][]string{"SourceIPAddress": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIPAddress": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Multiple fields AND logic - both match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3", "ClientIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: ALL fields match = allow
			expectedNotIpAddress: false, // NotIpAddress: ALL fields match = deny
		},
		{
			name:                 "Multiple fields AND logic - first matches, second missing",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Multiple fields AND logic - first matches, second no match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3", "ClientIp": "172.16.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Multiple fields AND logic - both no match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1", "ClientIp": "10.0.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: NO fields match = deny
			expectedNotIpAddress: true,  // NotIpAddress: NO fields match = allow
		},
		{
			name:                 "Multiple fields AND logic - first no match, second matches",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1", "ClientIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Empty values list for field",
			fields:               map[string][]string{"SourceIp": {}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &ConditionContext{Fields: tt.contextMap}

			// Test IpAddress operator
			t.Run("IpAddress", func(t *testing.T) {
				op := &IpAddressOperator{negate: false}
				result, err := op.Evaluate(tt.fields, ctx)

				if tt.shouldErr && err == nil {
					t.Errorf("Expected error but got none")
				}
				if !tt.shouldErr && err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !tt.shouldErr && result != tt.expectedIpAddress {
					t.Errorf("Expected %v but got %v", tt.expectedIpAddress, result)
				}
			})

			// Test NotIpAddress operator
			t.Run("NotIpAddress", func(t *testing.T) {
				op := &IpAddressOperator{negate: true}
				result, err := op.Evaluate(tt.fields, ctx)

				if tt.shouldErr && err == nil {
					t.Errorf("Expected error but got none")
				}
				if !tt.shouldErr && err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !tt.shouldErr && result != tt.expectedNotIpAddress {
					t.Errorf("Expected %v but got %v", tt.expectedNotIpAddress, result)
				}
			})
		})
	}
}

func TestEvaluateConditions(t *testing.T) {
	tests := []struct {
		name       string
		conditions map[string]map[string][]string
		contextMap map[string]string
		shouldErr  bool
		expected   bool
	}{
		{
			name:       "No conditions - always pass",
			conditions: map[string]map[string][]string{},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Single IP condition - match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Single IP condition - no match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "CIDR condition - match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"10.0.0.0/8"}}},
			contextMap: map[string]string{"SourceIp": "10.5.6.7"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Unsupported operator",
			conditions: map[string]map[string][]string{"UnsupportedOp": {"SomeField": {"value"}}},
			contextMap: map[string]string{"SomeField": "192.168.1.1"},
			shouldErr:  true,
			expected:   false,
		},
		{
			name:       "Dynamic field - custom field name",
			conditions: map[string]map[string][]string{"IpAddress": {"ClientIp": {"203.0.113.0/24"}}},
			contextMap: map[string]string{"ClientIp": "203.0.113.5"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "NotIpAddress condition - no match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "NotIpAddress condition - match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "NotIpAddress CIDR condition - no match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"10.0.0.0/8"}}},
			contextMap: map[string]string{"SourceIp": "11.0.0.1"},
			shouldErr:  false,
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &ConditionContext{
				Fields: tt.contextMap,
			}

			result, err := EvaluateConditions(tt.conditions, ctx)

			if tt.shouldErr && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.shouldErr && result != tt.expected {
				t.Errorf("Expected %v but got %v", tt.expected, result)
			}
		})
	}
}

func TestNewConditionContext(t *testing.T) {
	clientIP := "203.0.113.5"
	ctx := NewConditionContext(clientIP)

	if ctx.Fields["SourceIp"] != clientIP {
		t.Errorf("Expected SourceIp to be %s, got %s", clientIP, ctx.Fields["SourceIp"])
	}
}

func TestNewConditionContextWithFields(t *testing.T) {
	fields := map[string]string{
		"SourceIp":      "10.1.2.3",
		"ClientIp":      "192.168.1.1",
		"DestinationIp": "172.16.0.1",
	}
	ctx := NewConditionContextWithFields(fields)

	for key, expectedValue := range fields {
		if ctx.Fields[key] != expectedValue {
			t.Errorf("Expected %s to be %s, got %s", key, expectedValue, ctx.Fields[key])
		}
	}
}
