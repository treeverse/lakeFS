package config_test

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/block/local"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/testutil"
)

func newConfigFromFile(fn string) *config.Config {
	viper.SetConfigFile(fn)
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	return config.NewConfig()
}

func TestConfig_Setup(t *testing.T) {
	// test defaults
	c := config.NewConfig()
	if c.GetListenAddress() != config.DefaultListenAddr {
		t.Fatalf("expected listen addr %s, got %s", config.DefaultListenAddr, c.GetListenAddress())
	}
}

func TestNewFromFile(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		c := newConfigFromFile("testdata/valid_config.yaml")
		if c.GetListenAddress() != "0.0.0.0:8005" {
			t.Fatalf("expected listen addr 0.0.0.0:8005, got %s", c.GetListenAddress())
		}
		if c.GetS3GatewayDomainName() != "s3.example.com" {
			t.Fatalf("expected domain name s3.example.com, got %s", c.GetS3GatewayDomainName())
		}
	})

	t.Run("invalid config", func(t *testing.T) {
		var causedPanic bool
		defer func() {
			if r := recover(); r != nil {
				causedPanic = true
			}
		}()
		if causedPanic {
			t.Fatalf("did not expect panic before reading invalid file")
		}
		_ = newConfigFromFile("testdata/invalid_config.yaml")
		if !causedPanic {
			t.Fatalf("expected panic after reading invalid file")
		}
	})

	t.Run("missing config", func(t *testing.T) {
		var causedPanic bool
		defer func() {
			if r := recover(); r != nil {
				causedPanic = true
			}
		}()
		if causedPanic {
			t.Fatalf("did not expect panic before reading missing file")
		}
		_ = newConfigFromFile("testdata/valid_configgggggggggggggggg.yaml")
		if !causedPanic {
			t.Fatalf("expected panic after reading missing file")
		}
	})
}

func TestConfig_BuildBlockAdapter(t *testing.T) {
	t.Run("local block adapter", func(t *testing.T) {
		c := newConfigFromFile("testdata/valid_config.yaml")
		adapter, err := factory.BuildBlockAdapter(c, nil)
		testutil.Must(t, err)
		if _, ok := adapter.(*local.Adapter); !ok {
			t.Fatalf("expected a local block adapter, got something else instead")
		}
	})

	t.Run("s3 block adapter", func(t *testing.T) {
		newConfigFromFile("testdata/valid_s3adapter_config.yaml")
		c := config.NewConfig()
		adapter, err := factory.BuildBlockAdapter(c, nil)
		testutil.Must(t, err)
		if _, ok := adapter.(*s3a.Adapter); !ok {
			t.Fatalf("expected an s3 block adapter, got something else instead")
		}
	})

	// TODO(barak): add gs block adapter test
}

func TestConfig_JSONLogger(t *testing.T) {
	logfile := "/tmp/lakefs_json_logger_test.log"
	_ = os.Remove(logfile)
	_ = newConfigFromFile("testdata/valid_json_logger_config.yaml")

	log.Info("some message that I should be looking for")

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
