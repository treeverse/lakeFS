package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
)

var schemaRegex = regexp.MustCompile(`schema: (.+)`)

const (
	lakectlIdentifier    = "LAKEFS_SCHEMA"
	macrosDirName        = "macros"
	generateSchemaName   = "generate_schema_name.sql"
	continueOnSchemaFlag = "continue-on-schema-exists"
)

type model struct {
	Schema string `json:"schema"`
	Alias  string `json:"alias"`
}

var dbtCmd = &cobra.Command{
	Use:   "dbt",
	Short: "Integration with dbt commands",
}

var dbtCreateBranchSchema = &cobra.Command{
	Use:     "create-branch-schema",
	Short:   "Creates a new schema dedicated for branch and clones all dbt models to new schema",
	Example: "lakectl dbt create-branch-schema --branch <branch-name>",
	Run: func(cmd *cobra.Command, args []string) {
		clientType, _ := cmd.Flags().GetString("from-client-type")
		branchName, _ := cmd.Flags().GetString("branch")
		toSchema, _ := cmd.Flags().GetString("to-schema")
		projectRoot, _ := cmd.Flags().GetString("project-root")
		skipViews, _ := cmd.Flags().GetBool("skip-views")
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-location")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		continueOnSchemaExists, _ := cmd.Flags().GetBool(continueOnSchemaFlag)

		// in order to generate views in new schema dbt should contain a macro script allowing lakectl to dynamically set the schema
		// this could be skipped by using the skip-views flag
		if !skipViews {
			validateGenerateSchemaMacro(projectRoot)
		}

		schema := dbtDebug(projectRoot)
		copySchemaWithDbtTables(cmd.Context(), continueOnError, continueOnSchemaExists, clientType, schema, toSchema, branchName, projectRoot, dbfsLocation)

		if skipViews {
			fmt.Println("skipping views")
		} else {
			createViews(projectRoot, toSchema)
		}

		fmt.Printf("create-branch-schema done! \n your new schema is %s\n you could now edit your dbt profile and run dbt on branch %s\n", toSchema, branchName)
	},
}

func validateGenerateSchemaMacro(projectRoot string) {
	p := path.Join(projectRoot, macrosDirName, generateSchemaName)
	data, err := ioutil.ReadFile(p)
	if err != nil {
		DieErr(err)
	}

	if !strings.Contains(string(data), lakectlIdentifier) {
		// TODO(Guys): add link to docs
		DieFmt("generate_schema_name does not contain lakectl addition. handle lakefs support to %s or use skip-views flag", generateSchemaName)
	}
}

// copySchemaWithDbtTables create a copy of fromSchema and copies all dbt models materialized as table or incremental
func copySchemaWithDbtTables(ctx context.Context, continueOnError, continueOnSchemaExists bool, clientType, fromSchema, toSchema, branchName, projectRoot, dbfsLocation string) {
	client, clientDeferFunc := getMetastoreClient(clientType, "")
	defer clientDeferFunc()
	if len(toSchema) == 0 {
		toSchema = branchName
	}

	err := metastore.CopyDB(ctx, client, client, fromSchema, toSchema, branchName, dbfsLocation)
	switch {
	case err == nil:
		fmt.Printf("schema %s created\n", toSchema)
	case !errors.Is(err, mserrors.ErrSchemaExists):
		DieErr(err)
	case continueOnSchemaExists:
		fmt.Printf("schema %s already exists, proceeding with existing schema\n", toSchema)
	default:
		DieFmt("schema %s exists, change schema or use %s flag", toSchema, continueOnSchemaFlag)
	}

	models := getDbtTables(projectRoot)
	if err := CopyModels(ctx, models, continueOnError, fromSchema, toSchema, branchName, dbfsLocation, client); err != nil {
		DieErr(err)
	}
}

func createViews(projectRoot, toSchema string) {
	dbtCmd := exec.Command("dbt", "run", "--select", "config.materialized:view")
	dbtCmd.Env = append(dbtCmd.Env, lakectlIdentifier+"="+toSchema)
	dbtCmd.Dir = projectRoot
	if err := dbtCmd.Run(); err != nil {
		DieFmt("failed creating views with err: %v", err)
	}
	fmt.Println("views created")
}

func CopyModels(ctx context.Context, models []model, continueOnError bool, fromSchema, toSchema, branchName, dbfsLocation string, client metastore.Client) error {
	for _, m := range models {
		if fromSchema != m.Schema {
			fmt.Printf("skipping %s.%s, not in schema: %s", m.Schema, m.Alias, fromSchema)
			continue
		}
		err := metastore.CopyOrMerge(ctx, client, client, m.Schema, m.Alias, toSchema, m.Alias, branchName, "", nil, false, dbfsLocation)
		if err != nil {
			if !continueOnError {
				return err
			}
			fmt.Printf("copy %s.%s -> %s.%s  failed: %s\n", m.Schema, m.Alias, toSchema, m.Alias, err)
		} else {
			fmt.Printf("copied %s.%s -> %s.%s\n", m.Schema, m.Alias, toSchema, m.Alias)
		}
	}
	return nil
}

func getDbtTables(projectRoot string) []model {
	dbtCmd := exec.Command("dbt", "ls", "--resource-type", "model", "--select", "config.materialized:table config.materialized:incremental", "--output", "json", "--output-keys", "alias schema")
	dbtCmd.Dir = projectRoot
	output, err := dbtCmd.Output()
	if err != nil {
		fmt.Println(string(output))
		DieErr(err)
	}
	models := make([]model, 0)
	scan := bufio.NewScanner(bytes.NewReader(output))
	for scan.Scan() {
		line := scan.Bytes()
		var m model
		err = json.Unmarshal(line, &m)
		if err != nil {
			DieErr(err)
		}
		models = append(models, m)
	}
	return models
}

// dbtDebug validates dbt connection using dbt debug, return the schema configured by the target environment (configured in the dbt profiles file)
func dbtDebug(projectRoot string) string {
	dbtCmd := exec.Command("dbt", "debug")
	dbtCmd.Dir = projectRoot
	output, err := dbtCmd.Output()
	if err != nil {
		fmt.Println(string(output))
		DieErr(err)
	}
	submatch := schemaRegex.FindSubmatch(output)
	if submatch == nil || len(submatch) < 2 {
		DieFmt("Failed extracting schema from dbt debug message")
	}
	schema := submatch[1]
	fmt.Printf("dbt debug succeeded with schema %s\n", schema)
	return string(schema)
}

var dbtGenerateSchemaMacro = &cobra.Command{
	Use:     "generate-schema-macro",
	Short:   "generates the a macro allowing lakectl to run dbt on dynamic schemas",
	Example: "lakectl dbt generate-schema-macro",
	Run: func(cmd *cobra.Command, args []string) {
		projectRoot, _ := cmd.Flags().GetString("project-root")

		if !pathExists(path.Join(projectRoot, macrosDirName)) {
			DieFmt("The project-root should contain the macro directory")
		}

		macroPath := path.Join(projectRoot, macrosDirName, generateSchemaName)
		if pathExists(macroPath) {
			DieFmt("%s already exists, add lakeFS schema generation manually", macroPath)
		}
		const generateSchemaData = `
generate_schema_name.sql 

{# This macro was generated by lakeFS in order to allow lakectl run dbt on dynamic schemas #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{  env_var('LAKEFS_SCHEMA', target.schema) }}

{%- endmacro %}
`
		//nolint:gosec
		err := ioutil.WriteFile(macroPath, []byte(generateSchemaData), 0644)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("macro created in path %s\n", macroPath)
	},
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		DieErr(err)
	}
	return true
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(dbtCmd)
	dbtCmd.AddCommand(dbtCreateBranchSchema)
	dbtCreateBranchSchema.Flags().String("branch", "", "requested branch")
	_ = dbtCreateBranchSchema.MarkFlagRequired("branch")
	dbtCreateBranchSchema.Flags().String("to-schema", "", "destination schema name [default is branch]")
	dbtCreateBranchSchema.Flags().String("project-root", ".", "location of dbt project")
	dbtCreateBranchSchema.Flags().String("dbfs-location", "", "")
	dbtCreateBranchSchema.Flags().Bool("skip-views", false, "")
	dbtCreateBranchSchema.Flags().Bool("continue-on-error", false, "prevent command from failing when a single table fails")
	dbtCreateBranchSchema.Flags().Bool(continueOnSchemaFlag, false, "allow running on existing schema")

	dbtCmd.AddCommand(dbtGenerateSchemaMacro)
	dbtGenerateSchemaMacro.Flags().String("project-root", ".", "location of dbt project")
}
