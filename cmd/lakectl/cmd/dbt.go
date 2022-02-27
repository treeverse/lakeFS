package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/dbtutil"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
)

type DbtCommandClient interface {
	// Run performs 'dbt run' over the given schema and selected materializations
	Run(schema string, selectValues []string) (string, error)
	// Debug validates dbt connection using dbt debug, return the schema configured by the target environment (configured in the dbt profiles file)
	Debug() string
	// Ls performs 'dbt ls' over the given schema and selected materializations
	Ls(resourceType string, selectStatement []string) []dbtutil.DBTResource
	// ValidateGenerateSchemaMacro validates that the "generate_schema_name.sql" file was created and used correctly (according to implementation)
	ValidateGenerateSchemaMacro()
}

type DbtClient struct {
	projectDir string
}

var schemaRegex = regexp.MustCompile(`schema: (.+)`)

const (
	lakectlIdentifier      = "LAKEFS_SCHEMA"
	macrosDirName          = "macros"
	generateSchemaName     = "generate_schema_name.sql"
	continueOnSchemaFlag   = "continue-on-schema-exists"
	createLakefsBranchFlag = "create-branch"
)

var (
	noSourceBranchError = errors.New(`given schema has no source lakefs branch associated with it`)
)

func (d DbtClient) ValidateGenerateSchemaMacro() {
	err := dbtutil.ValidateGenerateSchemaMacro(d.projectDir, macrosDirName, generateSchemaName, lakectlIdentifier)
	if err != nil {
		switch e := err.(type) {
		case *dbtutil.MissingSchemaIdentifierError:
			DieFmt(e.Error())
		default:
			DieErr(e)
		}
	}
}

func (d DbtClient) Run(schema string, selectValues []string) (string, error) {
	return dbtutil.DbtRun(d.projectDir, schema, lakectlIdentifier, selectValues, d)
}

func (d DbtClient) Debug() string {
	schemaOutput, err := dbtutil.DbtDebug(d.projectDir, schemaRegex, d)
	if err != nil {
		fmt.Println(schemaOutput)
		switch e := err.(type) {
		case *dbtutil.MissingSchemaInDebugError:
			DieFmt(e.Error())
		default:
			DieErr(e)
		}
	}
	fmt.Printf("dbt debug succeeded with schemaOutput %s\n", schemaOutput)
	return schemaOutput
}

func (d DbtClient) Ls(resourceType string, selectStatement []string) []dbtutil.DBTResource {
	resources, err := dbtutil.DbtLsToJson(d.projectDir, resourceType, selectStatement, d)
	if err != nil {
		DieErr(err)
	}
	return resources
}

func (d DbtClient) ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
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
		shouldCreateBranch, _ := cmd.Flags().GetBool(createLakefsBranchFlag)
		toSchema, _ := cmd.Flags().GetString("to-schema")
		projectRoot, _ := cmd.Flags().GetString("project-root")
		skipViews, _ := cmd.Flags().GetBool("skip-views")
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-location")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		continueOnSchemaExists, _ := cmd.Flags().GetBool(continueOnSchemaFlag)

		if len(toSchema) == 0 {
			toSchema = branchName
		}

		dbtClient := DbtClient{projectDir: projectRoot}

		// in order to generate views in new schema dbt should contain a macro script allowing lakectl to dynamically set the schema
		// this could be skipped by using the skip-views flag
		if !skipViews {
			dbtClient.ValidateGenerateSchemaMacro()
		}

		schema := dbtClient.Debug()
		ctx := cmd.Context()

		if shouldCreateBranch {
			handleBranchCreation(ctx, clientType, schema, branchName)
		}

		copySchemaWithDbtTables(ctx, continueOnError, continueOnSchemaExists, clientType, schema, toSchema, branchName, dbfsLocation, dbtClient)

		if skipViews {
			fmt.Println("skipping views")
		} else {
			createViews(dbtClient, toSchema)
		}

		fmt.Printf("create-branch-schema done! \nyour new schema is %s\nyou can now edit your dbt profile and run dbt on branch %s\n", toSchema, branchName)
	},
}

func handleBranchCreation(ctx context.Context, clientType, schema, branchName string) {
	sourceRepo, sourceBranch, err := ExtractRepoAndBranchFromDBName(ctx, clientType, schema)
	if err != nil {
		DieErr(noSourceBranchError)
	}
	sourceBranchUri := GenerateLakeFSBranchURIFromRepoAndBranchName(sourceRepo, sourceBranch)
	destinationBranchUri := GenerateLakeFSBranchURIFromRepoAndBranchName(sourceRepo, branchName)
	CreateBranch(ctx, sourceBranchUri, destinationBranchUri)
}

// copySchemaWithDbtTables create a copy of fromSchema and copies all dbt models materialized as table or incremental
func copySchemaWithDbtTables(ctx context.Context, continueOnError, continueOnSchemaExists bool, clientType, fromSchema, toSchema, branchName, dbfsLocation string, dbtClient DbtCommandClient) {
	client, clientDeferFunc := getMetastoreClient(clientType, "")
	defer clientDeferFunc()

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

	models := getDbtTables(dbtClient)
	if err := copyModels(ctx, models, continueOnError, fromSchema, toSchema, branchName, dbfsLocation, client); err != nil {
		DieErr(err)
	}
}

func createViews(dbtClient DbtCommandClient, toSchema string) {
	output, err := dbtClient.Run(toSchema, []string{"config.materialized:view"})
	if err != nil {
		DieFmt("failed creating views with err: %v\nOutput:\n%s", err, output)
	}
	fmt.Println("views created")
}

func copyModels(ctx context.Context, models []dbtutil.DBTResource, continueOnError bool, fromSchema, toSchema, branchName, dbfsLocation string, client metastore.Client) error {
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

func getDbtTables(dbtClient DbtCommandClient) []dbtutil.DBTResource {
	materializedSelection := []string{"config.materialized:table", "config.materialized:incremental"}
	resourceType := "model"
	return dbtClient.Ls(resourceType, materializedSelection)
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
	dbtCreateBranchSchema.Flags().Bool(createLakefsBranchFlag, false, "create a new lakeFS branch for the schema")

	dbtCmd.AddCommand(dbtGenerateSchemaMacro)
	dbtGenerateSchemaMacro.Flags().String("project-root", ".", "location of dbt project")
}
