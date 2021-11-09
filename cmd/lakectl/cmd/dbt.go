package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	lakectlIdentifier  = "LAKEFS_SCHEMA"
	macrosDirName      = "macros"
	generateSchemaName = "generate_schema_name.sql"
)

var dbtCmd = &cobra.Command{
	Use:   "dbt",
	Short: "Integration with dbt commands",
}

var dbtCreateBranchSchema = &cobra.Command{
	Use:     "create-branch-schema",
	Short:   "Creates a new schema dedicated for branch and clones all dbt models to new schema",
	Example: "lakectl dbt create-branch-schema <branch-name>",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		clientType, _ := cmd.Flags().GetString("from-client-type")
		toSchema, _ := cmd.Flags().GetString("to-schema")
		projectRoot, _ := cmd.Flags().GetString("project-root")
		skipViews, _ := cmd.Flags().GetBool("skip-views")
		dbfsLocation, _ := cmd.Flags().GetString("dbfsLocation")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		continueOnSchemaExists, _ := cmd.Flags().GetBool("continue-on-schema-exists")
		branchName := args[0]

		if projectRoot == "" {
			projectRoot = MustString(os.Getwd())
		}

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
	file, err := os.Open(p)
	defer func() {
		if err = file.Close(); err != nil {
			DieErr(err)
		}
	}()
	if err != nil {
		if os.IsNotExist(err) {
			DieFmt("missing macro generate_schema_name.sql macro, run 'lakectl dbt generate-macro' or add manually")
		}
		DieErr(fmt.Errorf("failed to read generate schema macro: %w", err))
	}
	if !containsLakectlIdentifier(file) {
		// TODO(Guys): add link to docs
		DieFmt("generate_schema_name does not contain lakectl addition. handle lakefs support to generate_schema_name.sql or use skip-views flag")
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
	case err != nil && !errors.Is(err, mserrors.ErrSchemaExists):
		DieErr(err)
	case errors.Is(err, mserrors.ErrSchemaExists) && continueOnSchemaExists:
		fmt.Printf("schema %s already exists, proceeding with existing schema\n", toSchema)
	case errors.Is(err, mserrors.ErrSchemaExists):
		DieFmt("schema %s exists, change schema or use --continue-on-schema-exists flag\n", toSchema)
	default:
		fmt.Printf("created schema %s\n", toSchema)
	}

	if err := CopyDbtTables(ctx, client, continueOnError, projectRoot, fromSchema, toSchema, branchName, dbfsLocation); err != nil {
		DieErr(err)
	}
}

func createViews(projectRoot, toSchema string) {
	os.Setenv(lakectlIdentifier, toSchema)
	defer os.Unsetenv(lakectlIdentifier)
	dbtCmd := exec.Command("dbt", "run", "--select", "config.materialized:view")
	dbtCmd.Dir = projectRoot
	if err := dbtCmd.Run(); err != nil {
		DieFmt("failed creating views with err: %v\n", err)
	}
	fmt.Println("views created")
}

type model struct {
	Schema string `json:"schema"`
	Alias  string `json:"alias"`
}

// CopyDbtTables copies dbt tables (models materialized as tables or incremental) to given schema
func CopyDbtTables(ctx context.Context, client metastore.Client, continueOnError bool, projectRoot, fromSchema, toSchema, branchName, dbfsLocation string) error {
	dbtCmd := exec.Command("dbt", "ls", "--resource-type", "model", "--select", "config.materialized:table config.materialized:incremental", "--output", "json", "--output-keys", "alias schema")
	dbtCmd.Dir = projectRoot
	stdout, err := dbtCmd.StdoutPipe()
	if err != nil {
		return err
	}
	err = dbtCmd.Start()
	if err != nil {
		return err
	}
	scan := bufio.NewScanner(stdout)
	for scan.Scan() {
		line := scan.Bytes()
		var m model
		err = json.Unmarshal(line, &m)
		if err != nil {
			return err
		}

		if fromSchema != m.Schema {
			fmt.Printf("skipping %s.%s, not in schema: %s", m.Schema, m.Alias, fromSchema)
			continue
		}
		err = metastore.CopyOrMerge(ctx, client, client, m.Schema, m.Alias, toSchema, m.Alias, branchName, "", nil, false, dbfsLocation)
		if err != nil {
			if !continueOnError {
				return err
			}
			fmt.Println(err)
		}
		fmt.Printf("coppied %s.%s -> %s.%s\n", m.Schema, m.Alias, toSchema, m.Alias)
	}
	return nil
}

// dbtDebug validates dbt connection using dbt debug, return the schema configured by the target environment (configured in the dbt profiles file)
func dbtDebug(projectRoot string) string {
	dbtCmd := exec.Command("dbt", "debug")
	dbtCmd.Dir = projectRoot
	output, err := dbtCmd.Output()
	if err != nil {
		DieErr(err)
	}
	schema := schemaRegex.FindSubmatch(output)[1]
	fmt.Printf("dbt debug succeeded with schema %s\n", schema)
	return string(schema)
}

func containsLakectlIdentifier(file io.Reader) bool {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), lakectlIdentifier) {
			return true
		}
	}
	return false
}

var dbtGenerateSchemaMacro = &cobra.Command{
	Use:     "generate-macro",
	Short:   "generates the a macro allowing lakectl to run dbt on dynamic schemas",
	Example: "lakectl dbt generate-macro",
	Run: func(cmd *cobra.Command, args []string) {
		projectRoot, _ := cmd.Flags().GetString("project-root")
		if projectRoot == "" {
			projectRoot = MustString(os.Getwd())
		}

		if !pathExists(path.Join(projectRoot, macrosDirName)) {
			DieFmt("The project-root should contain the macro directory\n")
		}

		macroPath := path.Join(projectRoot, macrosDirName, generateSchemaName)
		if pathExists(macroPath) {
			DieFmt("%v already exists, add lakeFS schema generation manually \n", macroPath)
		}
		// write to file
		file, err := os.Create(macroPath)
		if err != nil {
			DieErr(err)
		}
		defer func() {
			err := file.Close()
			if err != nil {
				DieErr(err)
			}
		}()
		const generateSchemaData = `
generate_schema_name.sql 

{# This macro was generated by lakeFS in order to allow lakectl run dbt on dynamic schemas #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{  env_var('LAKEFS_SCHEMA', target.schema) }}

{%- endmacro %}
`
		_, err = file.WriteString(generateSchemaData)
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
	dbtCreateBranchSchema.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	dbtCreateBranchSchema.Flags().String("to-schema", "", "destination schema name [default is from-branch]")
	dbtCreateBranchSchema.Flags().String("project-root", "", "location of dbt project [default is current working directory]")
	dbtCreateBranchSchema.Flags().String("dbfs-location", "", "")
	dbtCreateBranchSchema.Flags().Bool("skip-views", false, "")
	dbtCreateBranchSchema.Flags().Bool("continue-on-error", false, "prevent command from failing when a single table fails")
	dbtCreateBranchSchema.Flags().Bool("continue-on-schema-exists", false, "allow running on existing schema")

	dbtCmd.AddCommand(dbtGenerateSchemaMacro)
	dbtGenerateSchemaMacro.Flags().String("project-root", "", "location of dbt project [default is current working directory]")
}
