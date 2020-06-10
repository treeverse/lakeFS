package catalog

import (
	"regexp"
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
					"v1": func() error { return nil },
					"v2": func() error { return nil },
					"v3": func() error { return nil },
				},
			},
			wantErr: false,
		},
		{
			name: "all fail",
			args: args{
				validators: ValidateFields{
					"v1": func() error { return ErrInvalidValue },
					"v2": func() error { return ErrInvalidValue },
					"v3": func() error { return ErrInvalidValue },
				},
			},
			wantErr: true,
		},
		{
			name: "one of each",
			args: args{
				validators: ValidateFields{
					"v1": func() error { return nil },
					"v2": func() error { return ErrInvalidValue },
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

func TestValidateBranchName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple", input: "branch", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "short", input: "a", wantErr: true},
		{name: "space", input: "got space", wantErr: true},
		{name: "special", input: "/branch", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := ValidateBranchName(tt.input)
			err := validator()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBranchName() err = %s, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple", input: "bucket1", wantErr: false},
		{name: "special1", input: "1.2.3.4", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "short", input: "a", wantErr: true},
		{name: "space", input: "got space", wantErr: true},
		{name: "special2", input: "1/2/3/4", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := ValidateBucketName(tt.input)
			err := validator()
			if (err != nil) != tt.wantErr {
				t.Errorf("TestValidateBucketName() err = %s, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNonEmptyString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple", input: "data", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "space", input: "s p a c e", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := ValidateNonEmptyString(tt.input)
			err := validator()
			if (err != nil) != tt.wantErr {
				t.Errorf("TestValidateNonEmptyString() err = %s, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestValidateRegexp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		re      *regexp.Regexp
		wantErr bool
	}{
		{name: "match", input: "data", re: regexp.MustCompile("^d+"), wantErr: false},
		{name: "no match", input: "data", re: regexp.MustCompile("^D+"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := ValidateRegexp(tt.input, tt.re)
			err := validator()
			if (err != nil) != tt.wantErr {
				t.Errorf("TestValidateRegexp() err = %s, wantErr %t", err, tt.wantErr)
			}
		})
	}
}
