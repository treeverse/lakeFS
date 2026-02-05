package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	configureOutputFile = "config.yaml"
	defaultOutputDir    = "~/.lakefs"
	secretKeyLength     = 32 // 256-bit key
	signingKeyLength    = 32 // 256-bit key

	// Database types
	databaseTypePostgres = "postgres"
	databaseTypeLocal    = "local"
	databaseTypeDynamoDB = "dynamodb"
	databaseTypeCosmosDB = "cosmosdb"

	// Blockstore types
	blockstoreTypeLocal = "local"
	blockstoreTypeS3    = "s3"
	blockstoreTypeGS    = "gs"
	blockstoreTypeAzure = "azure"

	// Logging formats
	loggingFormatText = "text"
	loggingFormatJSON = "json"

	// Category statuses
	statusRequired    = "REQUIRED"
	statusConfigured  = "configured"
	statusOptional    = "optional"
	statusIncomplete  = "INCOMPLETE"
)

// Category identifiers
const (
	categoryDatabase   = "database"
	categoryBlockstore = "blockstore"
	categorySecurity   = "security"
	categoryListen     = "listen"
	categoryLogging    = "logging"
	categoryTLS        = "tls"
	categoryGateway    = "gateway"
	categoryStats      = "stats"
	categorySaveExit   = "save_exit"
	categoryReview     = "review"
)

var (
	configOutputPath string
	configInputPath  string

	databaseTypes   = []string{databaseTypePostgres, databaseTypeLocal, databaseTypeDynamoDB, databaseTypeCosmosDB}
	blockstoreTypes = []string{blockstoreTypeS3, blockstoreTypeGS, blockstoreTypeAzure, blockstoreTypeLocal}
	loggingFormats  = []string{loggingFormatText, loggingFormatJSON}
	loggingLevels   = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}
)

// configCategory represents a configuration category with its status
type configCategory struct {
	ID          string
	Name        string
	Description string
	Required    bool
	Configured  bool
}

// configState holds the current configuration state
type configState struct {
	viper      *viper.Viper
	categories map[string]*configCategory
}

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Interactively create or update a lakeFS configuration file",
	Long: `Create or update a lakeFS configuration file through an interactive prompt.

This command guides you through setting up the configuration for running lakeFS.
You can navigate between configuration categories and update specific sections.

Categories are marked as:
  [REQUIRED]   - Must be configured before saving
  [configured] - Already configured (can be modified)
  [optional]   - Optional settings (can be skipped)

If an existing configuration file is found, values will be loaded as defaults.`,
	Run: runConfig,
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.Flags().StringVarP(&configOutputPath, "output", "o", "", "Output path for config file (default: ~/.lakefs/config.yaml)")
	configCmd.Flags().StringVarP(&configInputPath, "config", "c", "", "Load existing configuration from this file")
}

func newConfigState() *configState {
	return &configState{
		viper: viper.New(),
		categories: map[string]*configCategory{
			categoryDatabase: {
				ID:          categoryDatabase,
				Name:        "Database Configuration",
				Description: "Configure metadata storage (PostgreSQL, Local, DynamoDB, CosmosDB)",
				Required:    true,
				Configured:  false,
			},
			categoryBlockstore: {
				ID:          categoryBlockstore,
				Name:        "Object Storage Configuration",
				Description: "Configure data storage backend (S3, GCS, Azure, Local)",
				Required:    true,
				Configured:  false,
			},
			categorySecurity: {
				ID:          categorySecurity,
				Name:        "Security Configuration",
				Description: "Configure authentication and signing secrets",
				Required:    true,
				Configured:  false,
			},
			categoryListen: {
				ID:          categoryListen,
				Name:        "Listen Address",
				Description: "Configure server binding address and port",
				Required:    false,
				Configured:  false,
			},
			categoryLogging: {
				ID:          categoryLogging,
				Name:        "Logging Configuration",
				Description: "Configure log format, level, and output",
				Required:    false,
				Configured:  false,
			},
			categoryTLS: {
				ID:          categoryTLS,
				Name:        "TLS Configuration",
				Description: "Configure HTTPS with TLS certificates",
				Required:    false,
				Configured:  false,
			},
			categoryGateway: {
				ID:          categoryGateway,
				Name:        "S3 Gateway Configuration",
				Description: "Configure S3-compatible gateway settings",
				Required:    false,
				Configured:  false,
			},
			categoryStats: {
				ID:          categoryStats,
				Name:        "Statistics Configuration",
				Description: "Configure anonymous usage statistics",
				Required:    false,
				Configured:  false,
			},
		},
	}
}

func (cs *configState) loadExistingConfig(path string) error {
	cs.viper.SetConfigFile(path)
	cs.viper.SetConfigType("yaml")

	if err := cs.viper.ReadInConfig(); err != nil {
		return err
	}

	// Update category status based on loaded config
	cs.updateCategoryStatus()
	return nil
}

func (cs *configState) updateCategoryStatus() {
	// Check database configuration
	if cs.viper.GetString("database.type") != "" {
		cs.categories[categoryDatabase].Configured = true
	}

	// Check blockstore configuration
	if cs.viper.GetString("blockstore.type") != "" {
		cs.categories[categoryBlockstore].Configured = true
	}

	// Check security configuration
	authKey := cs.viper.GetString("auth.encrypt.secret_key")
	signingKey := cs.viper.GetString("blockstore.signing.secret_key")
	if authKey != "" && signingKey != "" &&
		authKey != "THIS_MUST_BE_CHANGED_IN_PRODUCTION" &&
		signingKey != "OVERRIDE_THIS_SIGNING_SECRET_DEFAULT" {
		cs.categories[categorySecurity].Configured = true
	}

	// Check listen address
	if cs.viper.GetString("listen_address") != "" {
		cs.categories[categoryListen].Configured = true
	}

	// Check logging configuration
	if cs.viper.GetString("logging.level") != "" || cs.viper.GetString("logging.format") != "" {
		cs.categories[categoryLogging].Configured = true
	}

	// Check TLS configuration
	if cs.viper.GetBool("tls.enabled") {
		cs.categories[categoryTLS].Configured = true
	}

	// Check gateway configuration
	if cs.viper.GetString("gateways.s3.domain_name") != "" {
		cs.categories[categoryGateway].Configured = true
	}

	// Check stats configuration (consider configured if explicitly set)
	if cs.viper.IsSet("stats.enabled") {
		cs.categories[categoryStats].Configured = true
	}
}

func (cs *configState) getCategoryStatus(cat *configCategory) string {
	if cat.Configured {
		return statusConfigured
	}
	if cat.Required {
		return statusRequired
	}
	return statusOptional
}

func (cs *configState) allRequiredConfigured() bool {
	for _, cat := range cs.categories {
		if cat.Required && !cat.Configured {
			return false
		}
	}
	return true
}

func (cs *configState) getMissingRequired() []string {
	var missing []string
	for _, cat := range cs.categories {
		if cat.Required && !cat.Configured {
			missing = append(missing, cat.Name)
		}
	}
	return missing
}

func runConfig(_ *cobra.Command, _ []string) {
	printWelcomeBanner()
	fmt.Println()

	// Determine output path
	outputPath, err := resolveOutputPath()
	if err != nil {
		printMsgAndExit("failed to resolve output path: ", err)
	}

	// Initialize config state
	state := newConfigState()

	// Try to load existing configuration
	configLoaded := false
	if configInputPath != "" {
		// Load from specified input path
		if err := state.loadExistingConfig(configInputPath); err != nil {
			printNonFatalError(fmt.Sprintf("Failed to load config from %s: %v", configInputPath, err))
		} else {
			fmt.Printf("Loaded existing configuration from %s\n", configInputPath)
			configLoaded = true
		}
	} else if fileExists(outputPath) {
		// Try to load from output path
		if err := state.loadExistingConfig(outputPath); err == nil {
			fmt.Printf("Loaded existing configuration from %s\n", outputPath)
			configLoaded = true
		}
	}

	if configLoaded {
		fmt.Println("Existing values will be used as defaults.")
	}
	fmt.Println()

	// Main configuration loop
	for {
		category, exit := showMainMenu(state)
		if exit {
			break
		}

		switch category {
		case categoryDatabase:
			if err := configureDatabase(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryBlockstore:
			if err := configureBlockstore(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categorySecurity:
			if err := configureSecrets(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryListen:
			if err := configureListenAddress(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryLogging:
			if err := configureLoggingSection(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryTLS:
			if err := configureTLSSection(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryGateway:
			if err := configureS3Gateway(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryStats:
			if err := configureStatsSection(state); err != nil {
				printNonFatalError(err.Error())
			}
		case categoryReview:
			showConfigurationReview(state)
		case categorySaveExit:
			if !state.allRequiredConfigured() {
				missing := state.getMissingRequired()
				fmt.Println()
				fmt.Println("Cannot save configuration - the following required sections are not configured:")
				for _, m := range missing {
					fmt.Printf("  - %s\n", m)
				}
				fmt.Println()
				continue
			}

			// Confirm save
			if err := saveConfiguration(state, outputPath); err != nil {
				printNonFatalError(fmt.Sprintf("Failed to save configuration: %v", err))
				continue
			}

			fmt.Println()
			fmt.Printf("Configuration written to %s\n", outputPath)
			fmt.Println()
			printNextSteps(outputPath)
			return
		}

		fmt.Println()
	}
}

func showMainMenu(state *configState) (string, bool) {
	// Build menu items with status
	type menuItem struct {
		id      string
		display string
	}

	// Define category order
	categoryOrder := []string{
		categoryDatabase,
		categoryBlockstore,
		categorySecurity,
		categoryListen,
		categoryLogging,
		categoryTLS,
		categoryGateway,
		categoryStats,
	}

	items := make([]menuItem, 0, len(categoryOrder)+3)

	// Add separator for required sections
	fmt.Println("--- Required Configuration ---")
	for _, catID := range categoryOrder {
		cat := state.categories[catID]
		if !cat.Required {
			continue
		}
		status := state.getCategoryStatus(cat)
		statusDisplay := formatStatus(status)
		items = append(items, menuItem{
			id:      cat.ID,
			display: fmt.Sprintf("%s %s", statusDisplay, cat.Name),
		})
	}

	// Add separator for optional sections
	fmt.Println("--- Optional Configuration ---")
	for _, catID := range categoryOrder {
		cat := state.categories[catID]
		if cat.Required {
			continue
		}
		status := state.getCategoryStatus(cat)
		statusDisplay := formatStatus(status)
		items = append(items, menuItem{
			id:      cat.ID,
			display: fmt.Sprintf("%s %s", statusDisplay, cat.Name),
		})
	}

	// Add action items
	fmt.Println("--- Actions ---")
	items = append(items, menuItem{id: categoryReview, display: "Review current configuration"})

	saveStatus := ""
	if !state.allRequiredConfigured() {
		saveStatus = " (requires completing required sections)"
	}
	items = append(items, menuItem{id: categorySaveExit, display: fmt.Sprintf("Save and exit%s", saveStatus)})
	items = append(items, menuItem{id: "cancel", display: "Cancel without saving"})

	// Create display items for promptui
	displayItems := make([]string, len(items))
	for i, item := range items {
		displayItems[i] = item.display
	}

	prompt := promptui.Select{
		Label: "Select a category to configure",
		Items: displayItems,
		Size:  15,
	}

	idx, _, err := prompt.Run()
	if err != nil {
		if err == promptui.ErrInterrupt {
			return "", true
		}
		printMsgAndExit("prompt failed: ", err)
	}

	selectedID := items[idx].id
	if selectedID == "cancel" {
		confirm, _ := promptConfirm("Are you sure you want to exit without saving?")
		if confirm {
			fmt.Println("Configuration cancelled.")
			return "", true
		}
		return "", false
	}

	return selectedID, false
}

func formatStatus(status string) string {
	switch status {
	case statusRequired:
		return "[REQUIRED]  "
	case statusConfigured:
		return "[configured]"
	case statusOptional:
		return "[optional]  "
	case statusIncomplete:
		return "[INCOMPLETE]"
	default:
		return "[          ]"
	}
}

func showConfigurationReview(state *configState) {
	fmt.Println()
	fmt.Println("Configuration Review")
	fmt.Println("====================")
	fmt.Println()

	v := state.viper

	// Listen Address
	fmt.Println("Listen Address:")
	listenAddr := v.GetString("listen_address")
	if listenAddr == "" {
		listenAddr = config.DefaultListenAddress + " (default)"
	}
	fmt.Printf("  Address: %s\n", listenAddr)
	fmt.Println()

	// Database
	fmt.Println("Database:")
	dbType := v.GetString("database.type")
	if dbType == "" {
		fmt.Println("  NOT CONFIGURED")
	} else {
		fmt.Printf("  Type: %s\n", dbType)
		switch dbType {
		case databaseTypePostgres:
			fmt.Printf("  Connection: %s\n", maskConnectionString(v.GetString("database.postgres.connection_string")))
		case databaseTypeLocal:
			fmt.Printf("  Path: %s\n", v.GetString("database.local.path"))
		case databaseTypeDynamoDB:
			fmt.Printf("  Table: %s\n", v.GetString("database.dynamodb.table_name"))
			fmt.Printf("  Region: %s\n", v.GetString("database.dynamodb.aws_region"))
		case databaseTypeCosmosDB:
			fmt.Printf("  Endpoint: %s\n", v.GetString("database.cosmosdb.endpoint"))
			fmt.Printf("  Database: %s\n", v.GetString("database.cosmosdb.database"))
		}
	}
	fmt.Println()

	// Blockstore
	fmt.Println("Object Storage:")
	bsType := v.GetString("blockstore.type")
	if bsType == "" {
		fmt.Println("  NOT CONFIGURED")
	} else {
		fmt.Printf("  Type: %s\n", bsType)
		switch bsType {
		case blockstoreTypeS3:
			fmt.Printf("  Region: %s\n", v.GetString("blockstore.s3.region"))
			if endpoint := v.GetString("blockstore.s3.endpoint"); endpoint != "" {
				fmt.Printf("  Endpoint: %s\n", endpoint)
			}
		case blockstoreTypeGS:
			if creds := v.GetString("blockstore.gs.credentials_file"); creds != "" {
				fmt.Printf("  Credentials File: %s\n", creds)
			} else {
				fmt.Println("  Using default application credentials")
			}
		case blockstoreTypeAzure:
			fmt.Printf("  Storage Account: %s\n", v.GetString("blockstore.azure.storage_account"))
		case blockstoreTypeLocal:
			fmt.Printf("  Path: %s\n", v.GetString("blockstore.local.path"))
		}
	}
	fmt.Println()

	// Security
	fmt.Println("Security:")
	authKey := v.GetString("auth.encrypt.secret_key")
	signingKey := v.GetString("blockstore.signing.secret_key")
	if authKey == "" || signingKey == "" {
		fmt.Println("  NOT CONFIGURED")
	} else {
		fmt.Printf("  Auth Secret: %s\n", maskSecret(authKey))
		fmt.Printf("  Signing Key: %s\n", maskSecret(signingKey))
	}
	fmt.Println()

	// Optional configurations
	if v.GetString("logging.level") != "" {
		fmt.Println("Logging:")
		fmt.Printf("  Level: %s\n", v.GetString("logging.level"))
		fmt.Printf("  Format: %s\n", v.GetString("logging.format"))
		fmt.Println()
	}

	if v.GetBool("tls.enabled") {
		fmt.Println("TLS:")
		fmt.Println("  Enabled: true")
		fmt.Printf("  Cert File: %s\n", v.GetString("tls.cert_file"))
		fmt.Printf("  Key File: %s\n", v.GetString("tls.key_file"))
		fmt.Println()
	}

	if v.GetString("gateways.s3.domain_name") != "" {
		fmt.Println("S3 Gateway:")
		fmt.Printf("  Domain: %s\n", v.GetString("gateways.s3.domain_name"))
		fmt.Printf("  Region: %s\n", v.GetString("gateways.s3.region"))
		fmt.Println()
	}

	if v.IsSet("stats.enabled") {
		fmt.Println("Statistics:")
		fmt.Printf("  Enabled: %v\n", v.GetBool("stats.enabled"))
		fmt.Println()
	}

	fmt.Println("====================")
	fmt.Println()

	// Wait for user to press enter
	promptString("Press Enter to continue", "")
}

func maskConnectionString(connStr string) string {
	if connStr == "" {
		return ""
	}
	// Parse and mask password
	u, err := url.Parse(connStr)
	if err != nil {
		return "***"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

func maskSecret(secret string) string {
	if len(secret) <= 8 {
		return "***"
	}
	return secret[:4] + "..." + secret[len(secret)-4:]
}

// configureListenAddress prompts for the server listen address
func configureListenAddress(state *configState) error {
	fmt.Println()
	fmt.Println("--- Listen Address Configuration ---")
	fmt.Println("Configure the address and port lakeFS will listen on.")
	fmt.Println()

	currentValue := state.viper.GetString("listen_address")
	if currentValue == "" {
		currentValue = config.DefaultListenAddress
	}

	address, err := promptWithValidation(
		"Listen address",
		currentValue,
		validateListenAddress,
	)
	if err != nil {
		return err
	}
	state.viper.Set("listen_address", address)
	state.categories[categoryListen].Configured = true
	return nil
}

// configureDatabase handles all database type configurations
func configureDatabase(state *configState) error {
	fmt.Println()
	fmt.Println("--- Database Configuration ---")
	fmt.Println("lakeFS requires a database for metadata storage.")
	fmt.Println()
	fmt.Println("postgres:  Production-ready PostgreSQL database")
	fmt.Println("local:     Local embedded database (testing/development only)")
	fmt.Println("dynamodb:  AWS DynamoDB (for AWS deployments)")
	fmt.Println("cosmosdb:  Azure CosmosDB (for Azure deployments)")
	fmt.Println()

	currentType := state.viper.GetString("database.type")
	defaultIdx := 0
	for i, t := range databaseTypes {
		if t == currentType {
			defaultIdx = i
			break
		}
	}

	prompt := promptui.Select{
		Label:     "Database type",
		Items:     databaseTypes,
		CursorPos: defaultIdx,
	}

	_, dbType, err := prompt.Run()
	if err != nil {
		return err
	}
	state.viper.Set("database.type", dbType)

	switch dbType {
	case databaseTypePostgres:
		err = configurePostgres(state)
	case databaseTypeLocal:
		err = configureLocalDB(state)
	case databaseTypeDynamoDB:
		err = configureDynamoDB(state)
	case databaseTypeCosmosDB:
		err = configureCosmosDB(state)
	}

	if err != nil {
		return err
	}

	state.categories[categoryDatabase].Configured = true
	return nil
}

func configurePostgres(state *configState) error {
	fmt.Println()
	fmt.Println("PostgreSQL connection string format:")
	fmt.Println("  postgres://user:password@host:port/database?sslmode=disable")
	fmt.Println()

	currentValue := state.viper.GetString("database.postgres.connection_string")
	if currentValue == "" {
		currentValue = "postgres://localhost:5432/lakefs?sslmode=disable"
	}

	connStr, err := promptWithValidation(
		"PostgreSQL connection string",
		currentValue,
		validatePostgresConnectionString,
	)
	if err != nil {
		return err
	}
	state.viper.Set("database.postgres.connection_string", connStr)

	// Optional: configure connection pool
	configurePool, err := promptConfirm("Configure connection pool settings?")
	if err != nil {
		return err
	}
	if configurePool {
		currentMaxOpen := state.viper.GetInt("database.postgres.max_open_connections")
		if currentMaxOpen == 0 {
			currentMaxOpen = 25
		}
		maxOpen, err := promptInt("Max open connections", currentMaxOpen, 1, 1000)
		if err != nil {
			return err
		}
		state.viper.Set("database.postgres.max_open_connections", maxOpen)

		currentMaxIdle := state.viper.GetInt("database.postgres.max_idle_connections")
		if currentMaxIdle == 0 {
			currentMaxIdle = 25
		}
		maxIdle, err := promptInt("Max idle connections", currentMaxIdle, 1, 1000)
		if err != nil {
			return err
		}
		state.viper.Set("database.postgres.max_idle_connections", maxIdle)
	}

	return nil
}

func configureLocalDB(state *configState) error {
	fmt.Println()
	printLocalStorageWarning()

	currentValue := state.viper.GetString("database.local.path")
	if currentValue == "" {
		currentValue = "~/lakefs/metadata"
	}

	path, err := promptWithValidation(
		"Local database path",
		currentValue,
		validatePath,
	)
	if err != nil {
		return err
	}
	state.viper.Set("database.local.path", path)
	return nil
}

func configureDynamoDB(state *configState) error {
	fmt.Println()
	fmt.Println("DynamoDB configuration for AWS deployments.")
	fmt.Println()

	currentTableName := state.viper.GetString("database.dynamodb.table_name")
	if currentTableName == "" {
		currentTableName = "kvstore"
	}
	tableName, err := promptString("DynamoDB table name", currentTableName)
	if err != nil {
		return err
	}
	state.viper.Set("database.dynamodb.table_name", tableName)

	currentRegion := state.viper.GetString("database.dynamodb.aws_region")
	if currentRegion == "" {
		currentRegion = "us-east-1"
	}
	region, err := promptString("AWS region", currentRegion)
	if err != nil {
		return err
	}
	state.viper.Set("database.dynamodb.aws_region", region)

	// Optional endpoint (for local DynamoDB)
	currentEndpoint := state.viper.GetString("database.dynamodb.endpoint")
	useCustomEndpoint, err := promptConfirm("Use custom DynamoDB endpoint (e.g., for local DynamoDB)?")
	if err != nil {
		return err
	}
	if useCustomEndpoint {
		defaultEndpoint := currentEndpoint
		if defaultEndpoint == "" {
			defaultEndpoint = "http://localhost:8000"
		}
		endpoint, err := promptWithValidation("DynamoDB endpoint URL", defaultEndpoint, validateURL)
		if err != nil {
			return err
		}
		state.viper.Set("database.dynamodb.endpoint", endpoint)
	}

	// Optional: AWS credentials
	useStaticCreds, err := promptConfirm("Configure static AWS credentials (skip if using IAM roles)?")
	if err != nil {
		return err
	}
	if useStaticCreds {
		currentAccessKey := state.viper.GetString("database.dynamodb.aws_access_key_id")
		accessKeyID, err := promptString("AWS Access Key ID", currentAccessKey)
		if err != nil {
			return err
		}
		state.viper.Set("database.dynamodb.aws_access_key_id", accessKeyID)

		secretKey, err := promptPassword("AWS Secret Access Key")
		if err != nil {
			return err
		}
		state.viper.Set("database.dynamodb.aws_secret_access_key", secretKey)
	}

	return nil
}

func configureCosmosDB(state *configState) error {
	fmt.Println()
	fmt.Println("CosmosDB configuration for Azure deployments.")
	fmt.Println()

	currentEndpoint := state.viper.GetString("database.cosmosdb.endpoint")
	endpoint, err := promptWithValidation("CosmosDB endpoint URL", currentEndpoint, validateURL)
	if err != nil {
		return err
	}
	state.viper.Set("database.cosmosdb.endpoint", endpoint)

	key, err := promptPassword("CosmosDB key")
	if err != nil {
		return err
	}
	state.viper.Set("database.cosmosdb.key", key)

	currentDB := state.viper.GetString("database.cosmosdb.database")
	if currentDB == "" {
		currentDB = "lakefs"
	}
	database, err := promptString("CosmosDB database name", currentDB)
	if err != nil {
		return err
	}
	state.viper.Set("database.cosmosdb.database", database)

	currentContainer := state.viper.GetString("database.cosmosdb.container")
	if currentContainer == "" {
		currentContainer = "lakefs"
	}
	container, err := promptString("CosmosDB container name", currentContainer)
	if err != nil {
		return err
	}
	state.viper.Set("database.cosmosdb.container", container)

	return nil
}

// configureBlockstore handles all blockstore type configurations
func configureBlockstore(state *configState) error {
	fmt.Println()
	fmt.Println("--- Object Storage Configuration ---")
	fmt.Println("lakeFS stores data in an object storage backend.")
	fmt.Println()
	fmt.Println("s3:    Amazon S3 or S3-compatible storage (MinIO, Ceph, etc.)")
	fmt.Println("gs:    Google Cloud Storage")
	fmt.Println("azure: Azure Blob Storage")
	fmt.Println("local: Local filesystem (testing/development only)")
	fmt.Println()

	currentType := state.viper.GetString("blockstore.type")
	defaultIdx := 0
	for i, t := range blockstoreTypes {
		if t == currentType {
			defaultIdx = i
			break
		}
	}

	prompt := promptui.Select{
		Label:     "Object storage type",
		Items:     blockstoreTypes,
		CursorPos: defaultIdx,
	}

	_, bsType, err := prompt.Run()
	if err != nil {
		return err
	}
	state.viper.Set("blockstore.type", bsType)

	switch bsType {
	case blockstoreTypeS3:
		err = configureS3(state)
	case blockstoreTypeGS:
		err = configureGS(state)
	case blockstoreTypeAzure:
		err = configureAzure(state)
	case blockstoreTypeLocal:
		err = configureLocalBlockstore(state)
	}

	if err != nil {
		return err
	}

	state.categories[categoryBlockstore].Configured = true
	return nil
}

func configureS3(state *configState) error {
	fmt.Println()
	fmt.Println("S3 or S3-compatible storage configuration.")
	fmt.Println()

	currentRegion := state.viper.GetString("blockstore.s3.region")
	if currentRegion == "" {
		currentRegion = config.DefaultBlockstoreS3Region
	}
	region, err := promptString("S3 region", currentRegion)
	if err != nil {
		return err
	}
	state.viper.Set("blockstore.s3.region", region)

	// Custom endpoint for S3-compatible storage
	currentEndpoint := state.viper.GetString("blockstore.s3.endpoint")
	useCustomEndpoint, err := promptConfirm("Use custom S3 endpoint (for MinIO, Ceph, etc.)?")
	if err != nil {
		return err
	}
	if useCustomEndpoint {
		endpoint, err := promptWithValidation("S3 endpoint URL", currentEndpoint, validateURL)
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.s3.endpoint", endpoint)

		currentForcePathStyle := state.viper.GetBool("blockstore.s3.force_path_style")
		defaultForcePathStyle := "y"
		if !currentForcePathStyle && currentEndpoint != "" {
			defaultForcePathStyle = "n"
		}
		forcePathStyle, err := promptConfirmWithDefault("Force path-style addressing (required for most S3-compatible storage)?", defaultForcePathStyle == "y")
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.s3.force_path_style", forcePathStyle)
	}

	// AWS credentials
	useStaticCreds, err := promptConfirm("Configure static AWS credentials (skip if using IAM roles/instance profiles)?")
	if err != nil {
		return err
	}
	if useStaticCreds {
		currentAccessKey := state.viper.GetString("blockstore.s3.credentials.access_key_id")
		accessKeyID, err := promptString("AWS Access Key ID", currentAccessKey)
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.s3.credentials.access_key_id", accessKeyID)

		secretKey, err := promptPassword("AWS Secret Access Key")
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.s3.credentials.secret_access_key", secretKey)
	}

	return nil
}

func configureGS(state *configState) error {
	fmt.Println()
	fmt.Println("Google Cloud Storage configuration.")
	fmt.Println()

	currentCredsFile := state.viper.GetString("blockstore.gs.credentials_file")
	useCredsFile, err := promptConfirm("Use credentials file (skip if using default application credentials)?")
	if err != nil {
		return err
	}
	if useCredsFile {
		credsFile, err := promptWithValidation("Path to credentials JSON file", currentCredsFile, validatePath)
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.gs.credentials_file", credsFile)
	}

	return nil
}

func configureAzure(state *configState) error {
	fmt.Println()
	fmt.Println("Azure Blob Storage configuration.")
	fmt.Println()

	currentAccount := state.viper.GetString("blockstore.azure.storage_account")
	storageAccount, err := promptString("Azure Storage Account name", currentAccount)
	if err != nil {
		return err
	}
	state.viper.Set("blockstore.azure.storage_account", storageAccount)

	useAccessKey, err := promptConfirm("Use storage access key (skip if using managed identity)?")
	if err != nil {
		return err
	}
	if useAccessKey {
		accessKey, err := promptPassword("Azure Storage Access Key")
		if err != nil {
			return err
		}
		state.viper.Set("blockstore.azure.storage_access_key", accessKey)
	}

	return nil
}

func configureLocalBlockstore(state *configState) error {
	fmt.Println()
	printLocalStorageWarning()

	currentPath := state.viper.GetString("blockstore.local.path")
	if currentPath == "" {
		currentPath = config.DefaultBlockstoreLocalPath
	}

	path, err := promptWithValidation(
		"Local storage path",
		currentPath,
		validatePath,
	)
	if err != nil {
		return err
	}
	state.viper.Set("blockstore.local.path", path)

	currentImport := state.viper.GetBool("blockstore.local.import_enabled")
	enableImport, err := promptConfirmWithDefault("Enable importing data from local paths?", currentImport)
	if err != nil {
		return err
	}
	state.viper.Set("blockstore.local.import_enabled", enableImport)

	return nil
}

// configureSecrets handles authentication and signing secrets
func configureSecrets(state *configState) error {
	fmt.Println()
	fmt.Println("--- Security Configuration ---")
	fmt.Println("lakeFS requires secret keys for authentication and signing.")
	fmt.Println()

	currentAuthSecret := state.viper.GetString("auth.encrypt.secret_key")
	hasValidAuthSecret := currentAuthSecret != "" && currentAuthSecret != "THIS_MUST_BE_CHANGED_IN_PRODUCTION"

	if hasValidAuthSecret {
		fmt.Println("Authentication secret key is already configured.")
		changeAuth, err := promptConfirm("Do you want to change the authentication secret key?")
		if err != nil {
			return err
		}
		if changeAuth {
			hasValidAuthSecret = false
		}
	}

	if !hasValidAuthSecret {
		generateAuthSecret, err := promptConfirm("Generate a random authentication secret key?")
		if err != nil {
			return err
		}

		var authSecret string
		if generateAuthSecret {
			authSecret, err = generateSecureSecret(secretKeyLength)
			if err != nil {
				return fmt.Errorf("failed to generate auth secret: %w", err)
			}
			fmt.Println("  Generated authentication secret key.")
		} else {
			authSecret, err = promptPassword("Authentication secret key (min 8 characters)")
			if err != nil {
				return err
			}
			if len(authSecret) < 8 {
				return fmt.Errorf("authentication secret key must be at least 8 characters")
			}
		}
		state.viper.Set("auth.encrypt.secret_key", authSecret)
	}

	currentSigningSecret := state.viper.GetString("blockstore.signing.secret_key")
	hasValidSigningSecret := currentSigningSecret != "" && currentSigningSecret != "OVERRIDE_THIS_SIGNING_SECRET_DEFAULT"

	if hasValidSigningSecret {
		fmt.Println("Blockstore signing key is already configured.")
		changeSigning, err := promptConfirm("Do you want to change the blockstore signing key?")
		if err != nil {
			return err
		}
		if changeSigning {
			hasValidSigningSecret = false
		}
	}

	if !hasValidSigningSecret {
		generateSigningSecret, err := promptConfirm("Generate a random blockstore signing key?")
		if err != nil {
			return err
		}

		var signingSecret string
		if generateSigningSecret {
			signingSecret, err = generateSecureSecret(signingKeyLength)
			if err != nil {
				return fmt.Errorf("failed to generate signing secret: %w", err)
			}
			fmt.Println("  Generated blockstore signing key.")
		} else {
			signingSecret, err = promptPassword("Blockstore signing key (min 8 characters)")
			if err != nil {
				return err
			}
			if len(signingSecret) < 8 {
				return fmt.Errorf("signing key must be at least 8 characters")
			}
		}
		state.viper.Set("blockstore.signing.secret_key", signingSecret)
	}

	// Print notice about secrets
	printSecretGeneratedNotice()

	state.categories[categorySecurity].Configured = true
	return nil
}

// configureLoggingSection handles logging configuration
func configureLoggingSection(state *configState) error {
	fmt.Println()
	fmt.Println("--- Logging Configuration ---")
	fmt.Println()
	fmt.Println("text: Human-readable format")
	fmt.Println("json: JSON format for log aggregators")
	fmt.Println()

	currentFormat := state.viper.GetString("logging.format")
	defaultIdx := 0
	for i, f := range loggingFormats {
		if f == currentFormat {
			defaultIdx = i
			break
		}
	}

	formatPrompt := promptui.Select{
		Label:     "Log format",
		Items:     loggingFormats,
		CursorPos: defaultIdx,
	}
	_, format, err := formatPrompt.Run()
	if err != nil {
		return err
	}
	state.viper.Set("logging.format", format)

	currentLevel := state.viper.GetString("logging.level")
	if currentLevel == "" {
		currentLevel = "INFO"
	}
	defaultLevelIdx := 2 // INFO
	for i, l := range loggingLevels {
		if l == currentLevel {
			defaultLevelIdx = i
			break
		}
	}

	levelPrompt := promptui.Select{
		Label:     "Log level",
		Items:     loggingLevels,
		CursorPos: defaultLevelIdx,
	}
	_, level, err := levelPrompt.Run()
	if err != nil {
		return err
	}
	state.viper.Set("logging.level", level)

	outputToFile, err := promptConfirm("Write logs to file (in addition to stdout)?")
	if err != nil {
		return err
	}
	if outputToFile {
		currentLogPath := "/var/log/lakefs.log"
		logPath, err := promptWithValidation("Log file path", currentLogPath, validatePath)
		if err != nil {
			return err
		}
		state.viper.Set("logging.output", []string{"-", logPath})
	} else {
		state.viper.Set("logging.output", "-")
	}

	state.categories[categoryLogging].Configured = true
	return nil
}

// configureTLSSection handles TLS configuration
func configureTLSSection(state *configState) error {
	fmt.Println()
	fmt.Println("--- TLS Configuration ---")
	fmt.Println("Configure HTTPS with TLS certificates.")
	fmt.Println()

	currentEnabled := state.viper.GetBool("tls.enabled")
	enableTLS, err := promptConfirmWithDefault("Enable TLS (HTTPS)?", currentEnabled)
	if err != nil {
		return err
	}
	state.viper.Set("tls.enabled", enableTLS)

	if enableTLS {
		currentCertFile := state.viper.GetString("tls.cert_file")
		certFile, err := promptWithValidation("TLS certificate file path", currentCertFile, validatePath)
		if err != nil {
			return err
		}
		state.viper.Set("tls.cert_file", certFile)

		currentKeyFile := state.viper.GetString("tls.key_file")
		keyFile, err := promptWithValidation("TLS private key file path", currentKeyFile, validatePath)
		if err != nil {
			return err
		}
		state.viper.Set("tls.key_file", keyFile)
	}

	state.categories[categoryTLS].Configured = true
	return nil
}

// configureS3Gateway handles S3 gateway configuration
func configureS3Gateway(state *configState) error {
	fmt.Println()
	fmt.Println("--- S3 Gateway Configuration ---")
	fmt.Println("The S3 gateway allows S3-compatible access to lakeFS.")
	fmt.Println()

	currentDomain := state.viper.GetString("gateways.s3.domain_name")
	if currentDomain == "" {
		currentDomain = "s3.local.lakefs.io"
	}
	domainName, err := promptString("S3 gateway domain name", currentDomain)
	if err != nil {
		return err
	}
	state.viper.Set("gateways.s3.domain_name", domainName)

	currentRegion := state.viper.GetString("gateways.s3.region")
	if currentRegion == "" {
		currentRegion = "us-east-1"
	}
	region, err := promptString("S3 gateway region", currentRegion)
	if err != nil {
		return err
	}
	state.viper.Set("gateways.s3.region", region)

	state.categories[categoryGateway].Configured = true
	return nil
}

// configureStatsSection handles anonymous statistics configuration
func configureStatsSection(state *configState) error {
	fmt.Println()
	fmt.Println("--- Anonymous Usage Statistics ---")
	fmt.Println("lakeFS collects anonymous usage statistics to help improve the product.")
	fmt.Println("No personal or sensitive data is collected.")
	fmt.Println()

	currentEnabled := state.viper.GetBool("stats.enabled")
	if !state.viper.IsSet("stats.enabled") {
		currentEnabled = true // Default is enabled
	}

	enableStats, err := promptConfirmWithDefault("Enable anonymous usage statistics?", currentEnabled)
	if err != nil {
		return err
	}
	state.viper.Set("stats.enabled", enableStats)

	state.categories[categoryStats].Configured = true
	return nil
}

// Helper functions

func resolveOutputPath() (string, error) {
	if configOutputPath != "" {
		return homedir.Expand(configOutputPath)
	}

	dir, err := homedir.Expand(defaultOutputDir)
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, configureOutputFile), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func saveConfiguration(state *configState, outputPath string) error {
	// Ensure directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	state.viper.SetConfigType("yaml")
	state.viper.SetConfigFile(outputPath)

	return state.viper.WriteConfig()
}

func printNextSteps(outputPath string) {
	fmt.Println("Next steps:")
	fmt.Println("  1. Review the configuration file and adjust as needed")
	fmt.Println("  2. Start lakeFS with: lakefs run --config " + outputPath)
	fmt.Println("  3. Complete the setup in the web UI at http://localhost:8000/setup")
}

func generateSecureSecret(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// Prompt helpers

func promptString(label, defaultValue string) (string, error) {
	prompt := promptui.Prompt{
		Label:   label,
		Default: defaultValue,
	}
	return prompt.Run()
}

func promptPassword(label string) (string, error) {
	prompt := promptui.Prompt{
		Label: label,
		Mask:  '*',
	}
	return prompt.Run()
}

func promptConfirm(label string) (bool, error) {
	prompt := promptui.Prompt{
		Label:     label,
		IsConfirm: true,
	}
	_, err := prompt.Run()
	if err != nil {
		if err == promptui.ErrAbort {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func promptConfirmWithDefault(label string, defaultYes bool) (bool, error) {
	defaultStr := "n"
	if defaultYes {
		defaultStr = "y"
	}

	prompt := promptui.Prompt{
		Label:     label,
		IsConfirm: true,
		Default:   defaultStr,
	}
	_, err := prompt.Run()
	if err != nil {
		if err == promptui.ErrAbort {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func promptWithValidation(label, defaultValue string, validate func(string) error) (string, error) {
	prompt := promptui.Prompt{
		Label:    label,
		Default:  defaultValue,
		Validate: validate,
	}
	return prompt.Run()
}

func promptInt(label string, defaultValue, min, max int) (int, error) {
	prompt := promptui.Prompt{
		Label:   label,
		Default: strconv.Itoa(defaultValue),
		Validate: func(input string) error {
			val, err := strconv.Atoi(input)
			if err != nil {
				return fmt.Errorf("invalid number")
			}
			if val < min || val > max {
				return fmt.Errorf("value must be between %d and %d", min, max)
			}
			return nil
		},
	}
	result, err := prompt.Run()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(result)
}

// Validation functions

func validateListenAddress(input string) error {
	// Check for host:port format
	host, port, err := net.SplitHostPort(input)
	if err != nil {
		return fmt.Errorf("invalid address format (expected host:port)")
	}

	// Validate port
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("invalid port number")
	}

	// Validate host (can be empty, IP, or hostname)
	if host != "" && net.ParseIP(host) == nil {
		// Not an IP, check if it's a valid hostname pattern
		if !isValidHostname(host) {
			return fmt.Errorf("invalid hostname")
		}
	}

	return nil
}

func isValidHostname(hostname string) bool {
	if len(hostname) > 253 {
		return false
	}
	// Simple hostname validation
	validHostname := regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$`)
	return validHostname.MatchString(hostname)
}

func validateURL(input string) error {
	if input == "" {
		return fmt.Errorf("URL cannot be empty")
	}
	u, err := url.ParseRequestURI(input)
	if err != nil {
		return fmt.Errorf("invalid URL format")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL must start with http:// or https://")
	}
	return nil
}

func validatePostgresConnectionString(input string) error {
	if input == "" {
		return fmt.Errorf("connection string cannot be empty")
	}

	// Basic validation - should start with postgres:// or postgresql://
	if !strings.HasPrefix(input, "postgres://") && !strings.HasPrefix(input, "postgresql://") {
		return fmt.Errorf("connection string must start with postgres:// or postgresql://")
	}

	// Try to parse as URL
	_, err := url.Parse(input)
	if err != nil {
		return fmt.Errorf("invalid connection string format")
	}

	return nil
}

func validatePath(input string) error {
	if input == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// Expand home directory for validation
	expanded, err := homedir.Expand(input)
	if err != nil {
		return fmt.Errorf("invalid path format")
	}

	// Check if it's an absolute path or starts with ~
	if !filepath.IsAbs(expanded) && !strings.HasPrefix(input, "~") {
		return fmt.Errorf("path should be absolute or start with ~")
	}

	return nil
}

// UI formatting helpers

const welcomeBanner = `
lakeFS Configuration
====================

This wizard will guide you through creating a configuration
file for running lakeFS.

Navigate between categories using the menu.
[REQUIRED] sections must be completed before saving.

For more information, visit:
https://docs.lakefs.io/reference/configuration.html
`

const localStorageWarning = `
WARNING: Local storage is for testing/development only.
Do NOT use local storage for production workloads!
`

const secretGeneratedNotice = `
IMPORTANT: Secret keys have been configured.
Keep your configuration file secure and backed up!
`

func printWelcomeBanner() {
	fmt.Print(welcomeBanner)
}

func printLocalStorageWarning() {
	fmt.Print(localStorageWarning)
}

func printSecretGeneratedNotice() {
	fmt.Print(secretGeneratedNotice)
}
