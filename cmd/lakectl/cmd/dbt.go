package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
)

var (
	schemaRegex           = regexp.MustCompile(`schema: (.+)`)
	materializedSelection = []string{"config.materialized:table", "config.materialized:incremental"}
)

const (
	schemaEnvVarName     = "LAKEFS_SCHEMA"
	macrosDirName        = "macros"
	generateSchemaName   = "generate_schema_name.sql"
	continueOnSchemaFlag = "continue-on-schema-exists"
	createBranchFlag     = "create-branch"
	resourceType         = "model"
)

func ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	return cmd.Output()
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
		shouldCreateBranch, _ := cmd.Flags().GetBool(createBranchFlag)
		toSchema, _ := cmd.Flags().GetString("to-schema")
		projectRoot, _ := cmd.Flags().GetString("project-root")
		skipViews, _ := cmd.Flags().GetBool("skip-views")
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-location")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		continueOnSchemaExists, _ := cmd.Flags().GetBool(continueOnSchemaFlag)

		if strings.TrimSpace(toSchema) == "" {
			toSchema = branchName
		}

		metastoreClient, closeClient := getMetastoreClient(clientType, "")
		defer closeClient()

		// in order to generate views in new schema dbt should contain a macro script allowing lakectl to dynamically set the schema
		// this could be skipped by using the skip-views flag
		if !skipViews {
			err := ValidateGenerateSchemaMacro(projectRoot, macrosDirName, generateSchemaName, schemaEnvVarName)
			if err != nil {
				utils.DieErr(err)
			}
		}

		schema := retrieveSchema(projectRoot)
		ctx := cmd.Context()

		if shouldCreateBranch {
			handleBranchCreation(ctx, schema, branchName, metastoreClient)
		}

		copySchemaWithDbtTables(ctx, continueOnError, continueOnSchemaExists, schema, toSchema, branchName, dbfsLocation, projectRoot, metastoreClient)

		if skipViews {
			fmt.Println("skipping views")
		} else {
			createViews(projectRoot, toSchema)
		}

		fmt.Printf("create-branch-schema done!\nyour new schema is %s\nyou can now edit your dbt profile and run dbt on branch %s\n", toSchema, branchName)
	},
}

func retrieveSchema(projectRoot string) string {
	schemaOutput, err := DBTDebug(projectRoot, schemaRegex, ExecuteCommand)
	if err != nil {
		fmt.Println(schemaOutput)
		utils.DieErr(err)
	}
	fmt.Println("dbt debug succeeded with schemaOutput:", schemaOutput)
	return schemaOutput
}

func handleBranchCreation(ctx context.Context, schema, branchName string, metastoreClient metastore.Client) {
	sourceRepo, sourceBranch, err := ExtractRepoAndBranchFromDBName(ctx, schema, metastoreClient)
	if err != nil {
		utils.DieFmt(`Given schema has no source branch associated with it %s`, err)
	}
	CreateBranch(ctx, sourceRepo, sourceBranch, branchName)
}

// copySchemaWithDbtTables create a copy of fromSchema and copies all dbt models materialized as table or incremental
func copySchemaWithDbtTables(ctx context.Context, continueOnError, continueOnSchemaExists bool, fromSchema, toSchema, branchName, dbfsLocation, projectRoot string, client metastore.Client) {
	err := metastore.CopyDB(ctx, client, client, fromSchema, toSchema, branchName, dbfsLocation)
	switch {
	case err == nil:
		fmt.Printf("schema %s created\n", toSchema)
	case !errors.Is(err, mserrors.ErrSchemaExists):
		utils.DieErr(err)
	case continueOnSchemaExists:
		fmt.Printf("schema %s already exists, proceeding with existing schema\n", toSchema)
	default:
		utils.DieFmt("Schema %s exists, change schema or use %s flag", toSchema, continueOnSchemaFlag)
	}

	models := getDBTTables(projectRoot)
	if err := copyModels(ctx, models, continueOnError, fromSchema, toSchema, branchName, dbfsLocation, client); err != nil {
		utils.DieErr(err)
	}
}

func createViews(projectRoot, toSchema string) {
	output, err := DBTRun(projectRoot, toSchema, schemaEnvVarName, []string{"config.materialized:view"}, ExecuteCommand)
	if err != nil {
		utils.DieFmt("Failed creating views with err: %v\nOutput:\n%s", err, output)
	}
	fmt.Println("views created")
}

func copyModels(ctx context.Context, models []DBTResource, continueOnError bool, fromSchema, toSchema, branchName, dbfsLocation string, client metastore.Client) error {
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

func getDBTTables(projectRoot string) []DBTResource {
	resources, err := DBTLsToJSON(projectRoot, resourceType, materializedSelection, ExecuteCommand)
	if err != nil {
		utils.DieErr(err)
	}
	return resources
}

var dbtGenerateSchemaMacro = &cobra.Command{
	Use:     "generate-schema-macro",
	Short:   "generates the a macro allowing lakectl to run dbt on dynamic schemas",
	Example: "lakectl dbt generate-schema-macro",
	Run: func(cmd *cobra.Command, args []string) {
		projectRoot, _ := cmd.Flags().GetString("project-root")

		if !pathExists(path.Join(projectRoot, macrosDirName)) {
			utils.DieFmt("The project-root should contain the macro directory")
		}

		macroPath := path.Join(projectRoot, macrosDirName, generateSchemaName)
		if pathExists(macroPath) {
			utils.DieFmt("%s already exists, add lakeFS schema generation manually", macroPath)
		}
		const generateSchemaData = `
generate_schema_name.sql 

{# This macro was generated by lakeFS in order to allow lakectl run dbt on dynamic schemas #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{  env_var('LAKEFS_SCHEMA', target.schema) }}

{%- endmacro %}
`
		//nolint:gosec
		err := os.WriteFile(macroPath, []byte(generateSchemaData), 0o644) //nolint: gomnd
		if err != nil {
			utils.DieErr(err)
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
		utils.DieErr(err)
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
	dbtCreateBranchSchema.Flags().Bool(createBranchFlag, false, "create a new branch for the schema")

	dbtCmd.AddCommand(dbtGenerateSchemaMacro)
	dbtGenerateSchemaMacro.Flags().String("project-root", ".", "location of dbt project")
}
