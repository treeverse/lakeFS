package dbtutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

type DummyCommandExecutor struct {
	err       error
	output    string
	envReturn bool
}

func (dce DummyCommandExecutor) ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	sb := strings.Builder{}
	sb.WriteString(dce.output)
	if len(cmd.Env) > 0 && dce.envReturn {
		sb.WriteString(fmt.Sprintf(" %s", cmd.Env[len(cmd.Env)-1]))
	}
	return []byte(sb.String()), dce.err
}

var schemaRegex = regexp.MustCompile(`schema: (.+)`)

func TestDbtDebug(t *testing.T) {
	type args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    CommandExecutor
	}
	tests := []struct {
		name                              string
		args                              args
		want                              string
		wantErr                           bool
		validateMissingSchemaInDebugError bool
	}{
		{
			name: "Happy Flow",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "schema: success", err: nil},
			},
			want:    "success",
			wantErr: false,
		},
		{
			name: "Command failed with error and output string",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: nil,
				executor:    DummyCommandExecutor{output: "some error output", err: errors.New("BOOM")},
			},
			want:    "some error output",
			wantErr: true,
		},
		{
			name: "Submatch is nil (output is not in the regex)",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "no schema definition", err: nil},
			},
			want:                              "",
			wantErr:                           true,
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
			if (err != nil) && tt.validateMissingSchemaInDebugError && !errors.Is(err, MissingSchemaInDebugError{}) {
				t.Errorf("DbtDebug() error = %v, wantErr %v, validateMissingSchemaInDebugError %v", err, tt.wantErr, tt.validateMissingSchemaInDebugError)
				return
			}
			if got != tt.want {
				t.Errorf("DbtDebug() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDbtLsToJson(t *testing.T) {
	DBTResources := make([]DBTResource, 0)
	DBTResources = append(DBTResources, DBTResource{Schema: "schema1", Alias: "alias1"}, DBTResource{Schema: "schema2", Alias: "alias2"})
	type args struct {
		projectRoot  string
		resourceType string
		selectValues []string
		executor     CommandExecutor
	}
	tests := []struct {
		name    string
		args    args
		want    []DBTResource
		wantErr bool
	}{
		{
			name: "Happy Flow",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: nil, output: generateJsonStringFromResources(DBTResources...)},
			},
			want:    DBTResources,
			wantErr: false,
		},
		{
			name: "command returns an error",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: errors.New("BOOM"), output: ""},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "command returns invalid json",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: nil, output: "invalid json"},
			},
			want:    nil,
			wantErr: true,
		},
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
	schemaKey := "theschema"
	schemaValue := "myschema"
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
		{
			name: "happy flow",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: true,
				},
			},
			want:    fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			wantErr: false,
		},
		{
			name: "schemaEnvVarIdentifier empty",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: "",
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			want:    "hello",
			wantErr: false,
		},
		{
			name: "schema value empty",
			args: args{
				projectRoot:            "nevermind",
				schema:                 "",
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			want:    "hello",
			wantErr: false,
		},
		{
			name: "command returns an error",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       errors.New("BOOM"),
					output:    "hello",
					envReturn: true,
				},
			},
			want:    fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			wantErr: true,
		},
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

func generateJsonStringFromResources(resources ...DBTResource) string {
	sb := strings.Builder{}
	for _, resource := range resources {
		j, _ := json.Marshal(resource)
		sb.WriteString(string(j))
		sb.WriteString("\n")
	}
	return sb.String()
}
