package mvcc

import (
	"testing"
)

func TestValidate(t *testing.T) {
	type args struct {
		validators ValidateFields
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "empty",
			args:    args{},
			wantErr: false,
		},
		{
			name: "all pass",
			args: args{
				validators: ValidateFields{
					{Name: "v1", IsValid: func() bool { return true }},
					{Name: "v2", IsValid: func() bool { return true }},
					{Name: "v3", IsValid: func() bool { return true }},
				},
			},
			wantErr: false,
		},
		{
			name: "all fail",
			args: args{
				validators: ValidateFields{
					{Name: "v1", IsValid: func() bool { return false }},
					{Name: "v2", IsValid: func() bool { return false }},
					{Name: "v3", IsValid: func() bool { return false }},
				},
			},
			wantErr: true,
		},
		{
			name: "one of each",
			args: args{
				validators: ValidateFields{
					{Name: "v1", IsValid: func() bool { return true }},
					{Name: "v2", IsValid: func() bool { return false }},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.args.validators); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsValidBranchName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "simple", input: "branch", want: true},
		{name: "empty", input: "", want: false},
		{name: "short", input: "a", want: true},
		{name: "space", input: "got space", want: false},
		{name: "special", input: "/branch", want: false},
		{name: "dash", input: "a-branch", want: true},
		{name: "leading-dash", input: "-branch", want: false},
		{name: "underscores", input: "__", want: true},
		{name: "backslash", input: "a\\branch", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidBranchName(tt.input)
			if got != tt.want {
				t.Errorf("IsValidBranchName() got = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestIsNonEmptyString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "simple", input: "data", want: true},
		{name: "empty", input: "", want: false},
		{name: "space", input: "s p a c e", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsNonEmptyString(tt.input)
			if got != tt.want {
				t.Errorf("IsNonEmptyString() got = %t, want %t", got, tt.want)
			}
		})
	}
}
