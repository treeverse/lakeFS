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
	configureOutputFile     = "config.yaml"
	defaultOutputDir        = "~/.lakefs"
	secretKeyLength         = 32 // 256-bit key
	signingKeyLength        = 32 // 256-bit key
	modeBasic               = "basic"
	modeAdvanced            = "advanced"
	databaseTypePostgres    = "postgres"
	databaseTypeLocal       = "local"
	databaseTypeDynamoDB    = "dynamodb"
	databaseTypeCosmosDB    = "cosmosdb"
	blockstoreTypeLocal     = "local"
	blockstoreTypeS3        = "s3"
	blockstoreTypeGS        = "gs"
	blockstoreTypeAzure     = "azure"
	loggingFormatText       = "text"
	loggingFormatJSON       = "json"
)

var (
	configOutputPath string

	databaseTypes   = []string{databaseTypePostgres, databaseTypeLocal, databaseTypeDynamoDB, databaseTypeCosmosDB}
	blockstoreTypes = []string{blockstoreTypeS3, blockstoreTypeGS, blockstoreTypeAzure, blockstoreTypeLocal}
	loggingFormats  = []string{loggingFormatText, loggingFormatJSON}
	loggingLevels   = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}
)

// configureCmd represents the configure command
var configureCmd = &cobra.Command{
	Use:   "configure",
	Short: "Interactively create a lakeFS configuration file",
	Long: `Create a lakeFS configuration file through an interactive prompt.

This command guides you through setting up the essential configuration
for running lakeFS, including:
  - Database connection (PostgreSQL, Local, DynamoDB, or CosmosDB)
  - Object storage backend (S3, GCS, Azure, or Local)
  - Security secrets (auto-generated or custom)

You can choose between basic mode (essential settings only) or advanced
mode (includes logging, TLS, caching, and gateway settings).`,
	Run: runConfigure,
}

func init() {
	rootCmd.AddCommand(configureCmd)
	configureCmd.Flags().StringVarP(&configOutputPath, "output", "o", "", "Output path for config file (default: ~/.lakefs/config.yaml)")
}

func runConfigure(_ *cobra.Command, _ []string) {
	printWelcomeBanner()
	fmt.Println()

	// Determine output path
	outputPath, err := resolveOutputPath()
	if err != nil {
		printMsgAndExit("failed to resolve output path: ", err)
	}

	// Check if file exists and confirm overwrite
	if fileExists(outputPath) {
		overwrite, err := promptConfirm(fmt.Sprintf("Configuration file %s already exists. Overwrite?", outputPath))
		if err != nil {
			printMsgAndExit("prompt failed: ", err)
		}
		if !overwrite {
			fmt.Println("Configuration cancelled.")
			return
		}
	}

	// Initialize a new viper instance for writing
	configViper := viper.New()

	// Choose configuration mode
	mode, err := promptSelect("Configuration mode", []string{modeBasic, modeAdvanced},
		"basic: Essential settings only\nadvanced: Full configuration including logging, TLS, caching")
	if err != nil {
		printMsgAndExit("prompt failed: ", err)
	}
	fmt.Println()

	// Run basic configuration (always required)
	if err := configureBasic(configViper); err != nil {
		printMsgAndExit("configuration failed: ", err)
	}

	// Run advanced configuration if selected
	if mode == modeAdvanced {
		if err := configureAdvanced(configViper); err != nil {
			printMsgAndExit("configuration failed: ", err)
		}
	}

	// Write configuration file
	if err := writeConfig(configViper, outputPath); err != nil {
		printMsgAndExit("failed to write configuration: ", err)
	}

	fmt.Println()
	fmt.Printf("Configuration written to %s\n", outputPath)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Review the configuration file and adjust as needed")
	fmt.Println("  2. Start lakeFS with: lakefs run --config " + outputPath)
	fmt.Println("  3. Complete the setup in the web UI at http://localhost:8000/setup")
}

// configureBasic handles the essential configuration settings
func configureBasic(v *viper.Viper) error {
	fmt.Println("=== Basic Configuration ===")
	fmt.Println()

	// Listen address
	if err := configureListenAddress(v); err != nil {
		return err
	}

	// Database configuration
	if err := configureDatabase(v); err != nil {
		return err
	}

	// Blockstore configuration
	if err := configureBlockstore(v); err != nil {
		return err
	}

	// Security secrets
	if err := configureSecrets(v); err != nil {
		return err
	}

	return nil
}

// configureAdvanced handles optional advanced settings
func configureAdvanced(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("=== Advanced Configuration ===")
	fmt.Println()

	// Logging configuration
	configureLogging, err := promptConfirm("Configure logging settings?")
	if err != nil {
		return err
	}
	if configureLogging {
		if err := configureLoggingSection(v); err != nil {
			return err
		}
	}

	// TLS configuration
	configureTLS, err := promptConfirm("Configure TLS (HTTPS)?")
	if err != nil {
		return err
	}
	if configureTLS {
		if err := configureTLSSection(v); err != nil {
			return err
		}
	}

	// S3 Gateway configuration
	configureGateway, err := promptConfirm("Configure S3 Gateway settings?")
	if err != nil {
		return err
	}
	if configureGateway {
		if err := configureS3Gateway(v); err != nil {
			return err
		}
	}

	// Stats configuration
	configureStats, err := promptConfirm("Configure anonymous usage statistics?")
	if err != nil {
		return err
	}
	if configureStats {
		if err := configureStatsSection(v); err != nil {
			return err
		}
	}

	return nil
}

// configureListenAddress prompts for the server listen address
func configureListenAddress(v *viper.Viper) error {
	address, err := promptWithValidation(
		"Listen address",
		config.DefaultListenAddress,
		validateListenAddress,
	)
	if err != nil {
		return err
	}
	v.Set("listen_address", address)
	return nil
}

// configureDatabase handles all database type configurations
func configureDatabase(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- Database Configuration ---")
	fmt.Println("lakeFS requires a database for metadata storage.")
	fmt.Println()

	dbType, err := promptSelect("Database type", databaseTypes,
		"postgres: Production-ready PostgreSQL database\n"+
			"local: Local embedded database (for testing/development)\n"+
			"dynamodb: AWS DynamoDB (for AWS deployments)\n"+
			"cosmosdb: Azure CosmosDB (for Azure deployments)")
	if err != nil {
		return err
	}
	v.Set("database.type", dbType)

	switch dbType {
	case databaseTypePostgres:
		return configurePostgres(v)
	case databaseTypeLocal:
		return configureLocalDB(v)
	case databaseTypeDynamoDB:
		return configureDynamoDB(v)
	case databaseTypeCosmosDB:
		return configureCosmosDB(v)
	}

	return nil
}

func configurePostgres(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("PostgreSQL connection string format:")
	fmt.Println("  postgres://user:password@host:port/database?sslmode=disable")
	fmt.Println()

	connStr, err := promptWithValidation(
		"PostgreSQL connection string",
		"postgres://localhost:5432/lakefs?sslmode=disable",
		validatePostgresConnectionString,
	)
	if err != nil {
		return err
	}
	v.Set("database.postgres.connection_string", connStr)

	// Optional: configure connection pool
	configurePool, err := promptConfirm("Configure connection pool settings?")
	if err != nil {
		return err
	}
	if configurePool {
		maxOpen, err := promptInt("Max open connections", 25, 1, 1000)
		if err != nil {
			return err
		}
		v.Set("database.postgres.max_open_connections", maxOpen)

		maxIdle, err := promptInt("Max idle connections", 25, 1, 1000)
		if err != nil {
			return err
		}
		v.Set("database.postgres.max_idle_connections", maxIdle)
	}

	return nil
}

func configureLocalDB(v *viper.Viper) error {
	fmt.Println()
	printLocalStorageWarning()

	path, err := promptWithValidation(
		"Local database path",
		"~/lakefs/metadata",
		validatePath,
	)
	if err != nil {
		return err
	}
	v.Set("database.local.path", path)
	return nil
}

func configureDynamoDB(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("DynamoDB configuration for AWS deployments.")
	fmt.Println()

	tableName, err := promptString("DynamoDB table name", "kvstore")
	if err != nil {
		return err
	}
	v.Set("database.dynamodb.table_name", tableName)

	// AWS Region
	region, err := promptString("AWS region", "us-east-1")
	if err != nil {
		return err
	}
	v.Set("database.dynamodb.aws_region", region)

	// Optional endpoint (for local DynamoDB)
	useCustomEndpoint, err := promptConfirm("Use custom DynamoDB endpoint (e.g., for local DynamoDB)?")
	if err != nil {
		return err
	}
	if useCustomEndpoint {
		endpoint, err := promptWithValidation("DynamoDB endpoint URL", "http://localhost:8000", validateURL)
		if err != nil {
			return err
		}
		v.Set("database.dynamodb.endpoint", endpoint)
	}

	// Optional: AWS credentials
	useStaticCreds, err := promptConfirm("Configure static AWS credentials (skip if using IAM roles)?")
	if err != nil {
		return err
	}
	if useStaticCreds {
		accessKeyID, err := promptString("AWS Access Key ID", "")
		if err != nil {
			return err
		}
		v.Set("database.dynamodb.aws_access_key_id", accessKeyID)

		secretKey, err := promptPassword("AWS Secret Access Key")
		if err != nil {
			return err
		}
		v.Set("database.dynamodb.aws_secret_access_key", secretKey)
	}

	return nil
}

func configureCosmosDB(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("CosmosDB configuration for Azure deployments.")
	fmt.Println()

	endpoint, err := promptWithValidation("CosmosDB endpoint URL", "", validateURL)
	if err != nil {
		return err
	}
	v.Set("database.cosmosdb.endpoint", endpoint)

	key, err := promptPassword("CosmosDB key")
	if err != nil {
		return err
	}
	v.Set("database.cosmosdb.key", key)

	database, err := promptString("CosmosDB database name", "lakefs")
	if err != nil {
		return err
	}
	v.Set("database.cosmosdb.database", database)

	container, err := promptString("CosmosDB container name", "lakefs")
	if err != nil {
		return err
	}
	v.Set("database.cosmosdb.container", container)

	return nil
}

// configureBlockstore handles all blockstore type configurations
func configureBlockstore(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- Object Storage Configuration ---")
	fmt.Println("lakeFS stores data in an object storage backend.")
	fmt.Println()

	bsType, err := promptSelect("Object storage type", blockstoreTypes,
		"s3: Amazon S3 or S3-compatible storage\n"+
			"gs: Google Cloud Storage\n"+
			"azure: Azure Blob Storage\n"+
			"local: Local filesystem (for testing/development)")
	if err != nil {
		return err
	}
	v.Set("blockstore.type", bsType)

	switch bsType {
	case blockstoreTypeS3:
		return configureS3(v)
	case blockstoreTypeGS:
		return configureGS(v)
	case blockstoreTypeAzure:
		return configureAzure(v)
	case blockstoreTypeLocal:
		return configureLocalBlockstore(v)
	}

	return nil
}

func configureS3(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("S3 or S3-compatible storage configuration.")
	fmt.Println()

	region, err := promptString("S3 region", config.DefaultBlockstoreS3Region)
	if err != nil {
		return err
	}
	v.Set("blockstore.s3.region", region)

	// Custom endpoint for S3-compatible storage
	useCustomEndpoint, err := promptConfirm("Use custom S3 endpoint (for MinIO, Ceph, etc.)?")
	if err != nil {
		return err
	}
	if useCustomEndpoint {
		endpoint, err := promptWithValidation("S3 endpoint URL", "", validateURL)
		if err != nil {
			return err
		}
		v.Set("blockstore.s3.endpoint", endpoint)

		forcePathStyle, err := promptConfirm("Force path-style addressing (required for most S3-compatible storage)?")
		if err != nil {
			return err
		}
		v.Set("blockstore.s3.force_path_style", forcePathStyle)
	}

	// AWS credentials
	useStaticCreds, err := promptConfirm("Configure static AWS credentials (skip if using IAM roles/instance profiles)?")
	if err != nil {
		return err
	}
	if useStaticCreds {
		accessKeyID, err := promptString("AWS Access Key ID", "")
		if err != nil {
			return err
		}
		v.Set("blockstore.s3.credentials.access_key_id", accessKeyID)

		secretKey, err := promptPassword("AWS Secret Access Key")
		if err != nil {
			return err
		}
		v.Set("blockstore.s3.credentials.secret_access_key", secretKey)
	}

	return nil
}

func configureGS(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("Google Cloud Storage configuration.")
	fmt.Println()

	useCredsFile, err := promptConfirm("Use credentials file (skip if using default application credentials)?")
	if err != nil {
		return err
	}
	if useCredsFile {
		credsFile, err := promptWithValidation("Path to credentials JSON file", "", validatePath)
		if err != nil {
			return err
		}
		v.Set("blockstore.gs.credentials_file", credsFile)
	}

	return nil
}

func configureAzure(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("Azure Blob Storage configuration.")
	fmt.Println()

	storageAccount, err := promptString("Azure Storage Account name", "")
	if err != nil {
		return err
	}
	v.Set("blockstore.azure.storage_account", storageAccount)

	useAccessKey, err := promptConfirm("Use storage access key (skip if using managed identity)?")
	if err != nil {
		return err
	}
	if useAccessKey {
		accessKey, err := promptPassword("Azure Storage Access Key")
		if err != nil {
			return err
		}
		v.Set("blockstore.azure.storage_access_key", accessKey)
	}

	return nil
}

func configureLocalBlockstore(v *viper.Viper) error {
	fmt.Println()
	printLocalStorageWarning()

	path, err := promptWithValidation(
		"Local storage path",
		config.DefaultBlockstoreLocalPath,
		validatePath,
	)
	if err != nil {
		return err
	}
	v.Set("blockstore.local.path", path)

	enableImport, err := promptConfirm("Enable importing data from local paths?")
	if err != nil {
		return err
	}
	v.Set("blockstore.local.import_enabled", enableImport)

	return nil
}

// configureSecrets handles authentication and signing secrets
func configureSecrets(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- Security Configuration ---")
	fmt.Println("lakeFS requires secret keys for authentication and signing.")
	fmt.Println()

	// Auth encryption secret
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
	v.Set("auth.encrypt.secret_key", authSecret)

	// Blockstore signing secret
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
	v.Set("blockstore.signing.secret_key", signingSecret)

	// Print notice if secrets were generated
	if generateAuthSecret || generateSigningSecret {
		printSecretGeneratedNotice()
	}

	return nil
}

// configureLoggingSection handles logging configuration
func configureLoggingSection(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- Logging Configuration ---")

	format, err := promptSelect("Log format", loggingFormats, "text: Human-readable format\njson: JSON format for log aggregators")
	if err != nil {
		return err
	}
	v.Set("logging.format", format)

	level, err := promptSelect("Log level", loggingLevels, "")
	if err != nil {
		return err
	}
	v.Set("logging.level", level)

	outputToFile, err := promptConfirm("Write logs to file (in addition to stdout)?")
	if err != nil {
		return err
	}
	if outputToFile {
		logPath, err := promptWithValidation("Log file path", "/var/log/lakefs.log", validatePath)
		if err != nil {
			return err
		}
		v.Set("logging.output", []string{"-", logPath})
	} else {
		v.Set("logging.output", "-")
	}

	return nil
}

// configureTLSSection handles TLS configuration
func configureTLSSection(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- TLS Configuration ---")

	enableTLS, err := promptConfirm("Enable TLS (HTTPS)?")
	if err != nil {
		return err
	}
	v.Set("tls.enabled", enableTLS)

	if enableTLS {
		certFile, err := promptWithValidation("TLS certificate file path", "", validatePath)
		if err != nil {
			return err
		}
		v.Set("tls.cert_file", certFile)

		keyFile, err := promptWithValidation("TLS private key file path", "", validatePath)
		if err != nil {
			return err
		}
		v.Set("tls.key_file", keyFile)
	}

	return nil
}

// configureS3Gateway handles S3 gateway configuration
func configureS3Gateway(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- S3 Gateway Configuration ---")
	fmt.Println("The S3 gateway allows S3-compatible access to lakeFS.")
	fmt.Println()

	domainName, err := promptString("S3 gateway domain name", "s3.local.lakefs.io")
	if err != nil {
		return err
	}
	v.Set("gateways.s3.domain_name", domainName)

	region, err := promptString("S3 gateway region", "us-east-1")
	if err != nil {
		return err
	}
	v.Set("gateways.s3.region", region)

	return nil
}

// configureStatsSection handles anonymous statistics configuration
func configureStatsSection(v *viper.Viper) error {
	fmt.Println()
	fmt.Println("--- Anonymous Usage Statistics ---")
	fmt.Println("lakeFS collects anonymous usage statistics to help improve the product.")
	fmt.Println()

	enableStats, err := promptConfirm("Enable anonymous usage statistics?")
	if err != nil {
		return err
	}
	v.Set("stats.enabled", enableStats)

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

func writeConfig(v *viper.Viper, outputPath string) error {
	// Ensure directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	v.SetConfigType("yaml")
	v.SetConfigFile(outputPath)

	return v.WriteConfig()
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

func promptSelect(label string, items []string, helpText string) (string, error) {
	if helpText != "" {
		fmt.Println(helpText)
	}

	prompt := promptui.Select{
		Label: label,
		Items: items,
	}
	_, result, err := prompt.Run()
	return result, err
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
╔═══════════════════════════════════════════════════════════════╗
║                   lakeFS Configuration                        ║
╠═══════════════════════════════════════════════════════════════╣
║  This wizard will guide you through creating a configuration  ║
║  file for running lakeFS.                                     ║
║                                                               ║
║  For more information, visit:                                 ║
║  https://docs.lakefs.io/reference/configuration.html          ║
╚═══════════════════════════════════════════════════════════════╝
`

const localStorageWarning = `
┌─────────────────────────────────────────────────────────────────┐
│  WARNING: Local storage is for testing/development only.       │
│  Do NOT use local storage for production workloads!            │
└─────────────────────────────────────────────────────────────────┘
`

const secretGeneratedNotice = `
┌─────────────────────────────────────────────────────────────────┐
│  IMPORTANT: Secret keys have been generated.                   │
│  Keep your configuration file secure and backed up!            │
└─────────────────────────────────────────────────────────────────┘
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
