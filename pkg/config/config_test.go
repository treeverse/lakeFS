package config_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/block/local"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func newConfigFromFile(fn string) (*config.Config, error) {
	viper.SetConfigFile(fn)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	cfg, err := config.NewConfig("")
	if err != nil {
		return nil, err
	}
	err = cfg.Validate()
	return cfg, err
}

func TestConfig_Setup(t *testing.T) {
	// test defaults
	c, err := config.NewConfig("")
	testutil.Must(t, err)
	// Don't validate, some tested configs don't have all required fields.
	if c.ListenAddress != config.DefaultListenAddress {
		t.Fatalf("expected listen addr '%s', got '%s'", config.DefaultListenAddress, c.ListenAddress)
	}
}

func TestConfig_NewFromFile(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/valid_config.yaml")
		testutil.Must(t, err)
		if c.ListenAddress != "0.0.0.0:8005" {
			t.Fatalf("expected listen addr 0.0.0.0:8005, got %s", c.ListenAddress)
		}
		if diffs := deep.Equal([]string(c.Gateways.S3.DomainNames), []string{"s3.example.com", "gs3.example.com", "gcp.example.net"}); diffs != nil {
			t.Fatalf("expected domain name s3.example.com, diffs %s", diffs)
		}
	})

	t.Run("invalid config", func(t *testing.T) {
		_, err := newConfigFromFile("testdata/invalid_config.yaml")
		// viper errors are not
		if err == nil || !strings.HasPrefix(err.Error(), "While parsing config:") {
			t.Fatalf("expected invalid configuration file to fail, got %v", err)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		_, err := newConfigFromFile("testdata/valid_configgggggggggggggggg.yaml")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected missing configuration file to fail, got %v", err)
		}
	})
}

func pushEnv(key, value string) func() {
	oldValue := os.Getenv(key)
	_ = os.Setenv(key, value)
	return func() {
		_ = os.Setenv(key, oldValue)
	}
}

func TestConfig_EnvironmentVariables(t *testing.T) {
	const dbString = "not://a/database"
	defer pushEnv("LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING", dbString)()

	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	// read in environment variables
	viper.AutomaticEnv()

	c, err := newConfigFromFile("testdata/valid_config.yaml")
	testutil.Must(t, err)
	kvParams, err := c.DatabaseParams()
	testutil.Must(t, err)
	if kvParams.Postgres.ConnectionString != dbString {
		t.Errorf("got DB connection string %s, expected to override to %s", kvParams.Postgres.ConnectionString, dbString)
	}
}

func TestConfig_DomainNamePrefix(t *testing.T) {
	_, err := newConfigFromFile("testdata/domain_name_prefix.yaml")
	if !errors.Is(err, config.ErrBadDomainNames) {
		t.Errorf("got error %s not %s", err, config.ErrBadDomainNames)
	}
}

func TestConfig_BuildBlockAdapter(t *testing.T) {
	ctx := context.Background()
	t.Run("local block adapter", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/valid_config.yaml")
		testutil.Must(t, err)
		adapter, err := factory.BuildBlockAdapter(ctx, nil, c)
		testutil.Must(t, err)
		if _, ok := adapter.(*local.Adapter); !ok {
			t.Fatalf("expected a local block adapter, got something else instead")
		}
	})

	t.Run("s3 block adapter", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/valid_s3_adapter_config.yaml")
		testutil.Must(t, err)
		adapter, err := factory.BuildBlockAdapter(ctx, nil, c)
		testutil.Must(t, err)
		if _, ok := adapter.(*s3a.Adapter); !ok {
			t.Fatalf("expected an s3 block adapter, got something else instead")
		}
	})

	t.Run("gs block adapter", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/valid_gs_adapter_config.yaml")
		testutil.Must(t, err)
		adapter, err := factory.BuildBlockAdapter(ctx, nil, c)
		testutil.Must(t, err)
		if _, ok := adapter.(*gs.Adapter); !ok {
			t.Fatalf("expected an gs block adapter, got something else instead")
		}
	})
}

func TestConfig_JSONLogger(t *testing.T) {
	logfile := "/tmp/lakefs_json_logger_test.log"
	_ = os.Remove(logfile)
	_, err := newConfigFromFile("testdata/valid_json_logger_config.yaml")
	testutil.Must(t, err)

	logging.Default().Info("some message that I should be looking for")

	content, err := os.Open(logfile)
	if err != nil {
		t.Fatalf("unexpected error reading log file: %s", err)
	}
	defer func() {
		_ = content.Close()
	}()
	reader := bufio.NewReader(content)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("could not read line from logfile: %s", err)
	}
	m := make(map[string]string)
	err = json.Unmarshal([]byte(line), &m)
	if err != nil {
		t.Fatalf("could not parse JSON line from logfile: %s", err)
	}
	if _, ok := m["msg"]; !ok {
		t.Fatalf("expected a msg field, could not find one")
	}
}

func verifyAWSConfig(t *testing.T, c *config.Config) {
	awsConfig := c.GetAwsConfig()
	credentials, err := awsConfig.Credentials.Get()
	testutil.Must(t, err)
	if credentials.AccessKeyID != "my-key-id" {
		t.Fatalf("unexpected key id in credentials. expected %s got %s", "my-key-id", credentials.AccessKeyID)
	}
	if credentials.SecretAccessKey != "my-secret-key" {
		t.Fatalf("unexpected secret access key in credentials. expected %s got %s", "my-secret-key", credentials.SecretAccessKey)
	}
}

func TestConfig_AWSConfig(t *testing.T) {
	t.Run("use secret_access_key configuration", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/aws_credentials.yaml")
		testutil.Must(t, err)
		verifyAWSConfig(t, c)
	})
	t.Run("use alias access_secret_key configuration", func(t *testing.T) {
		c, err := newConfigFromFile("testdata/aws_credentials_with_alias.yaml")
		testutil.Must(t, err)
		verifyAWSConfig(t, c)
	})
}
