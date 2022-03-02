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

type dummyCommandExecutorError struct{}

func (dcee dummyCommandExecutorError) Error() string {
	return "BOOM"
}

func (dce DummyCommandExecutor) ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	sb := strings.Builder{}
	sb.WriteString(dce.output)
	if len(cmd.Env) > 0 && dce.envReturn {
		sb.WriteString(fmt.Sprintf(" %s", cmd.Env[len(cmd.Env)-1]))
	}
	return []byte(sb.String()), dce.err
}

func TestDbtDebug(t *testing.T) {
	type args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    CommandExecutor
	}
	var schemaRegex = regexp.MustCompile(`schema: (.+)`)

	type dbtDebugTestCase struct {
		Name       string
		Args       args
		SchemaName string
		WantErr    bool
	}
	tests := []dbtDebugTestCase{
		{
			Name: "Sunny day Flow",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "schema: success", err: nil},
			},
			SchemaName: "success",
			WantErr:    false,
		},
		{
			Name: "Command failed with error and output string",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: nil,
				executor:    DummyCommandExecutor{output: "some error output", err: fmt.Errorf("BOOM")},
			},
			SchemaName: "some error output",
			WantErr:    true,
		},
		{
			Name: "Submatch is nil (output is not in the regex)",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "no schema definition", err: nil},
			},
			SchemaName: "",
			WantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			schema, err := DbtDebug(tt.Args.projectRoot, tt.Args.schemaRegex, tt.Args.executor)
			if err != nil && tt.WantErr == false {
				t.Errorf("DbtDebug() error = %v, but no error was expected", err)
				return
			}
			if schema != tt.SchemaName {
				t.Errorf("DbtDebug() schema = %v, schemaName %v", schema, tt.SchemaName)
			}
		})
	}
}

func TestDbtLsToJson(t *testing.T) {
	DBTResources := make([]DBTResource, 0)
	DBTResources = append(DBTResources, DBTResource{Schema: "schema1", Alias: "alias1"}, DBTResource{Schema: "schema2", Alias: "alias2"})
	type args struct {
		ProjectRoot  string
		ResourceType string
		SelectValues []string
		Executor     CommandExecutor
	}
	type dbtLsToJsonTestCase struct {
		Name         string
		Args         args
		DbtResources []DBTResource
		ErrType      error
	}
	tests := []dbtLsToJsonTestCase{
		{
			Name: "Sunny day Flow",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: nil, output: toJSON(DBTResources...)},
			},
			DbtResources: DBTResources,
			ErrType:      nil,
		},
		{
			Name: "command returns an error",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: dummyCommandExecutorError{}, output: ""},
			},
			DbtResources: nil,
			ErrType:      dummyCommandExecutorError{},
		},
		{
			Name: "command returns invalid json",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: nil, output: "invalid json"},
			},
			DbtResources: nil,
			ErrType:      &json.SyntaxError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dbtResources, err := DbtLsToJSON(tt.Args.ProjectRoot, tt.Args.ResourceType, tt.Args.SelectValues, tt.Args.Executor)
			if (err != nil) && !errors.As(err, &tt.ErrType) {
				t.Errorf("DbtLsToJSON() error = %v, expected ErrType: %v", err, tt.ErrType)
				return
			}
			if !reflect.DeepEqual(dbtResources, tt.DbtResources) {
				t.Errorf("DbtLsToJSON() dbtResources = %v, schemaName %v", dbtResources, tt.DbtResources)
			}
		})
	}
}

func TestDbtRun(t *testing.T) {
	schemaKey := "theschema"
	schemaValue := "myschema"
	type args struct {
		ProjectRoot            string
		Schema                 string
		SchemaEnvVarIdentifier string
		SelectValues           []string
		Executor               CommandExecutor
	}
	type dbtRunTestCase struct {
		Name      string
		Args      args
		RunOutput string
		Err       error
	}
	tests := []dbtRunTestCase{
		{
			Name: "Sunny day flow",
			Args: args{
				ProjectRoot:            "nevermind",
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: true,
				},
			},
			RunOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			Err:       nil,
		},
		{
			Name: "schemaEnvVarIdentifier empty",
			Args: args{
				ProjectRoot:            "nevermind",
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: "",
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			RunOutput: "hello",
			Err:       nil,
		},
		{
			Name: "schema value empty",
			Args: args{
				ProjectRoot:            "nevermind",
				Schema:                 "",
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			RunOutput: "hello",
			Err:       nil,
		},
		{
			Name: "command returns an error",
			Args: args{
				ProjectRoot:            "nevermind",
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       dummyCommandExecutorError{},
					output:    "hello",
					envReturn: true,
				},
			},
			RunOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			Err:       dummyCommandExecutorError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			runOutput, err := DbtRun(tt.Args.ProjectRoot, tt.Args.Schema, tt.Args.SchemaEnvVarIdentifier, tt.Args.SelectValues, tt.Args.Executor)
			if (err != nil) && !errors.Is(err, tt.Err) {
				t.Errorf("DbtRun() error = %v, ErrType %v", err, tt.Err)
				return
			}
			if runOutput != tt.RunOutput {
				t.Errorf("DbtRun() runOutput = %v, schemaName %v", runOutput, tt.RunOutput)
			}
		})
	}
}

func toJSON(resources ...DBTResource) string {
	sb := strings.Builder{}
	for _, resource := range resources {
		j, _ := json.Marshal(resource)
		sb.WriteString(string(j))
		sb.WriteString("\n")
	}
	return sb.String()
}
