package config

import (
	"reflect"
	"time"
)

// Strings is a []string that mapstructure can deserialize from a single string or from a list
// of strings.
type Strings []string

var ourStringsType = reflect.TypeOf(Strings{})
var stringType = reflect.TypeOf("")
var stringSliceType = reflect.TypeOf([]string{})

// decodeStrings is a mapstructure.HookFuncType that decodes a single string value or a slice
// of strings into Strings.
func DecodeStrings(fromValue reflect.Value, toValue reflect.Value) (interface{}, error) {
	if toValue.Type() != ourStringsType {
		return fromValue.Interface(), nil
	}
	if fromValue.Type() == stringSliceType {
		return Strings(fromValue.Interface().([]string)), nil
	}
	if fromValue.Type() == stringType {
		return Strings{fromValue.String()}, nil
	}
	return fromValue.Interface(), nil
}

type SecureString string

func (s *SecureString) String() string {
	return string(*s)
}

// S3AuthInfo holds S3-style authentication.
type S3AuthInfo struct {
	CredentialsFile string `mapstructure:"credentials_file"`
	Profile         string
	Credentials     *struct {
		AccessKeyID SecureString `mapstructure:"access_key_id"`
		// AccessSecretKey is the old name for SecretAccessKey.
		//
		// Deprecated: use SecretAccessKey instead.
		AccessSecretKey SecureString `mapstructure:"access_secret_key"`
		SecretAccessKey SecureString `mapstructure:"secret_access_key"`
		SessionToken    SecureString `mapstructure:"session_token"`
	}
}

// Output struct of configuration, used to validate.  If you read a key using a viper accessor
// rather than accessing a field of this struct, that key will *not* be validated.  So don't
// do that.
type configuration struct {
	ListenAddress string `mapstructure:"listen_address"`

	Logging struct {
		Format string
		Level  string
		Output string
	}

	Database struct {
		ConnectionString      SecureString  `mapstructure:"connection_string"`
		MaxOpenConnections    int32         `mapstructure:"max_open_connections"`
		MaxIdleConnections    int32         `mapstructure:"max_idle_connections"`
		ConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`
	}

	Auth struct {
		Cache struct {
			Enabled bool
			Size    int
			TTL     time.Duration
			Jitter  time.Duration
		}
		Encrypt struct {
			SecretKey SecureString `mapstructure:"secret_key" validate:"required"`
		}
	}
	Blockstore struct {
		Type  string `validate:"required"`
		Local *struct {
			Path string
		}
		S3 *struct {
			S3AuthInfo            `mapstructure:",squash"`
			Region                string
			Endpoint              string
			StreamingChunkSize    int           `mapstructure:"streaming_chunk_size"`
			StreamingChunkTimeout time.Duration `mapstructure:"streaming_chunk_timeout"`
			MaxRetries            int           `mapstructure:"max_retries"`
			ForcePathStyle        bool          `mapstructure:"force_path_style"`
		}
		Azure *struct {
			TryTimeout       time.Duration `mapstructure:"try_timeout"`
			StorageAccount   string        `mapstructure:"storage_account"`
			StorageAccessKey string        `mapstructure:"storage_access_key"`
			AuthMethod       string        `mapstructure:"auth_method"`
		}
		GS *struct {
			S3Endpoint      string `mapstructure:"s3_endpoint"`
			CredentialsFile string `mapstructure:"credentials_file"`
			CredentialsJSON string `mapstructure:"credentials_json"`
		}
	}
	Committed struct {
		LocalCache struct {
			SizeBytes             int64 `mapstructure:"size_bytes"`
			Dir                   string
			MaxUploadersPerWriter int     `mapstructure:"max_uploaders_per_writer"`
			RangeProportion       float64 `mapstructure:"range_proportion"`
			MetaRangeProportion   float64 `mapstructure:"metarange_proportion"`
		} `mapstructure:"local_cache"`
		BlockStoragePrefix string `mapstructure:"block_storage_prefix"`
		Permanent          struct {
			MinRangeSizeBytes      uint64  `mapstructure:"min_range_size_bytes"`
			MaxRangeSizeBytes      uint64  `mapstructure:"max_range_size_bytes"`
			RangeRaggednessEntries float64 `mapstructure:"range_raggedness_entries"`
		}
		SSTable struct {
			Memory struct {
				CacheSizeBytes int64 `mapstructure:"cache_size_bytes"`
			}
		}
	}
	Gateways struct {
		S3 struct {
			DomainNames Strings `mapstructure:"domain_name"`
			Region      string
			FallbackURL string `mapstructure:"fallback_url"`
		}
	}
	Stats struct {
		Enabled       bool
		Address       string
		FlushInterval time.Duration `mapstructure:"flush_interval"`
	}
	Installation struct {
		FixedID string `mapstructure:"fixed_id"`
	}
}
