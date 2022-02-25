package dbtutil

import (
	"errors"
	"os/exec"
	"reflect"
	"regexp"
	"testing"
)

type DummyCommandExecutor struct {
	err    error
	output string
}

func (dce *DummyCommandExecutor) ExecuteCommand(*exec.Cmd) ([]byte, error) {
	return []byte(dce.output), dce.err
}

var schemaRegex = regexp.MustCompile(`schema: (.+)`)

// 3. Submatch is nil (output is not in the regex)
// 4. Submatch has less than 2 parts (meaning that it's not of the format "schema: nameOfSchema")

func TestDbtDebug(t *testing.T) {
	type args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    CommandExecutor
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
		validateMissingSchemaInDebugError bool
	}{
		{
			name: "Happy Flow",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    &DummyCommandExecutor{output: "schema: success", err: nil},
			},
			want:    "success",
			wantErr: false,
		},
		{
			name: "Command failed with error and output string",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: nil,
				executor:    &DummyCommandExecutor{ output: "some error output", err: errors.New("BOOM") },
			},
			want:    "some error output",
			wantErr: true,
		},
		{
			name: "Submatch is nil (output is not in the regex)",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    &DummyCommandExecutor{ output: "no schema definition", err: nil },
			},
			want: "",
			wantErr: true,
			validateMissingSchemaInDebugError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtDebug(tt.args.projectRoot, tt.args.schemaRegex, tt.args.executor)
			if (err != nil) != tt.wantErr {
				t.Errorf("DbtDebug() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (err != nil) && tt.validateMissingSchemaInDebugError && err.()
			if got != tt.want {
				t.Errorf("DbtDebug() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDbtLsToJson(t *testing.T) {
	type args struct {
		projectRoot  string
		resourceType string
		selectValues []string
		executor     CommandExecutor
	}
	tests := []struct {
		name    string
		args    args
		want    []DbtResource
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtLsToJson(tt.args.projectRoot, tt.args.resourceType, tt.args.selectValues, tt.args.executor)
			if (err != nil) != tt.wantErr {
				t.Errorf("DbtLsToJson() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DbtLsToJson() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDbtRun(t *testing.T) {
	type args struct {
		projectRoot            string
		schema                 string
		schemaEnvVarIdentifier string
		selectValues           []string
		executor               CommandExecutor
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtRun(tt.args.projectRoot, tt.args.schema, tt.args.schemaEnvVarIdentifier, tt.args.selectValues, tt.args.executor)
			if (err != nil) != tt.wantErr {
				t.Errorf("DbtRun() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DbtRun() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMissingSchemaIdentifierError_Error(t *testing.T) {
	type fields struct {
		schemaIdentifier       string
		generateSchemaFileName string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mie := &MissingSchemaIdentifierError{
				schemaIdentifier:       tt.fields.schemaIdentifier,
				generateSchemaFileName: tt.fields.generateSchemaFileName,
			}
			if got := mie.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMissingSchemaInDebugError_Error(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mie := &MissingSchemaInDebugError{}
			if got := mie.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateGenerateSchemaMacro(t *testing.T) {
	type args struct {
		projectRoot            string
		macrosDirName          string
		generateSchemaFileName string
		schemaIdentifier       string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateGenerateSchemaMacro(tt.args.projectRoot, tt.args.macrosDirName, tt.args.generateSchemaFileName, tt.args.schemaIdentifier); (err != nil) != tt.wantErr {
				t.Errorf("ValidateGenerateSchemaMacro() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
