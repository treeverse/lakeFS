// Package flare is used for collecting, sanitizing, and packaging lakeFS configuration, log files, and environment variables
// for debugging and troubleshooting purposes.
package flare

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/octarinesec/secret-detector/pkg/scanner"
	"github.com/octarinesec/secret-detector/pkg/secrets"
	"gopkg.in/yaml.v3"
)

var defaultEnvVarPrefixes = []string{"LAKEFS_", "HTTP_", "HOSTNAME"}

const (
	DirPermissions  = 0700
	FilePremissions = 0600
	FlareUmask      = 077
)

type RedactedValueReplacer func(value string) string

// LogFormat is a log file format supported by the flare package
type LogFormat string

const (
	LogFormatJSON      LogFormat = "json"
	LogFormatPlainText LogFormat = "text"
)

var (
	ErrExtractDateFromJSONLogLine = errors.New("failed to extract date from log line")
	ErrDateNotFound               = errors.New("date not found in log line")
)

type WithTime struct {
	Time time.Time
}

type Flare struct {
	envVariables    []string
	envVarPrefixes  []string
	envVarBlacklist *regexp.Regexp
	// LogDateLayout is the layout used by time.Parse to parse dates in log lines.
	// The default value is time.RFC3339.
	// This can be changed using the WithLogDateLayout option.
	LogDateLayout string
	replacerFunc  RedactedValueReplacer
	scanner       secrets.Scanner
}

func defaultSecretReplacer(value string) string {
	hasher := sha512.New()
	hasher.Write([]byte(value))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

type Option func(*Flare)

func NewFlare(options ...Option) (*Flare, error) {
	// remove the ini transformer because it has false positives with plain text log lines
	config := scanner.NewConfigBuilderFrom(scanner.NewConfigWithDefaults()).RemoveTransformers("ini").Build()
	// set zero threshold for entropy-based detection in the keyword based detector
	// example: LAKEFS_AUTH_ENCRYPT_SECRET_KEY=123asdasd will be detected by the {secret, key} keywords regardless of the value
	// The value here is the threshold of the result of calculating the Shannon entropy of the string
	// This can be a value between 0 and log2(len(string))
	// A value of 0 means that we will detect the secret even if all characters are the same
	// Essentially, this means that we redact secrets based on keywords detected in the key with no requirements on the value
	config.DetectorConfigs["keyword"] = []string{"0"}
	s, err := scanner.NewScannerFromConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to init secrets scanner: %w", err)
	}
	flare := &Flare{
		envVariables:    os.Environ(),
		envVarPrefixes:  defaultEnvVarPrefixes,
		envVarBlacklist: nil,
		replacerFunc:    defaultSecretReplacer,
		LogDateLayout:   time.RFC3339,
		scanner:         s,
	}

	for _, opt := range options {
		opt(flare)
	}

	return flare, nil
}

// WithEnvVarPrefixes replaces the default list of environment variable prefixes that flare processes.
// The default list is "LAKEFS_", "HTTP_", "HOSTNAME".
func WithEnvVarPrefixes(prefixes []string) Option {
	return func(f *Flare) {
		f.envVarPrefixes = prefixes
	}
}

// WithAdditionalEnvVarPrefix adds additional environment prefixes to the default list that flare processes.
// The default list is "LAKEFS_", "HTTP_", "HOSTNAME".
func WithAdditionalEnvVarPrefix(envVar string) Option {
	return func(f *Flare) {
		f.envVarPrefixes = append(f.envVarPrefixes, envVar)
	}
}

// WithEnvVarBlacklist adds a list of environment variable keys that will be explicitly redacted.
func WithEnvVarBlacklist(blacklist []string) Option {
	return func(f *Flare) {
		if len(blacklist) != 0 {
			f.envVarBlacklist = regexp.MustCompile(fmt.Sprintf(`^(?:%s)=`, strings.Join(blacklist, "|")))
		}
	}
}

// WithSecretReplacerFunc replaces the default secret replacement func with a function that takes the raw value and returns the redacted value
// The default secret replacement func replaces the secret with a SHA512 hash of the secret value.
// This allows comparison of values without exposing the secret values.
func WithSecretReplacerFunc(fn RedactedValueReplacer) Option {
	return func(f *Flare) {
		f.replacerFunc = fn
	}
}

// WithEnv replaces the process environment variables vars.
func WithEnv(vars []string) Option {
	return func(f *Flare) {
		f.envVariables = vars
	}
}

// ProcessConfig takes a config struct, marshals it to YAML and writes it out to outputPath
func (f *Flare) ProcessConfig(cfg interface{}, outputPath, fileName string, getWriterFunc GetFileWriterFunc) (retErr error) {
	yamlCfg, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	configOutPath := filepath.Join(outputPath, fileName)
	w, err := getWriterFunc(configOutPath)
	if err != nil {
		return fmt.Errorf("%s: %w", configOutPath, err)
	}
	defer func() {
		e := w.Close()
		if retErr == nil {
			retErr = e
		}
	}()
	_, err = w.Write(yamlCfg)
	return err
}

// ProcessEnvVars iterates over all defined env vars, filters them according to the defined prefixes,
// redacts secrets, and writes them out to file.
func (f *Flare) ProcessEnvVars(outPath, fileName string, getWriterFunc GetFileWriterFunc) (retErr error) {
	outputFilePath := filepath.Join(outPath, fileName)
	w, err := getWriterFunc(outputFilePath)
	if err != nil {
		return fmt.Errorf("%s: %w", outputFilePath, err)
	}
	defer func() {
		e := w.Close()
		if retErr == nil {
			retErr = e
		}
	}()

	err = f.processEnvVars(w)
	if err != nil {
		return fmt.Errorf("%s: %w", outputFilePath, err)
	}
	return nil
}

// SetBaselinePermissions sets the Umask for all files created by flare for posix operating systems
// on Windows this is a noop as permissions are set according to the parent directory
func SetBaselinePermissions(mask int) {
	setBaselinePermissions(mask)
}

func (f *Flare) processEnvVars(w io.Writer) error {
	for _, e := range f.envVariables {
		for _, p := range f.envVarPrefixes {
			if strings.HasPrefix(e, p) {
				var re string
				var err error
				if f.inEnvVarBlacklist(e) {
					kv := strings.Split(e, "=")
					redactedVal := f.replacerFunc(kv[1])
					re = strings.Join([]string{kv[0], redactedVal}, "=")
				} else {
					re, err = f.redactSecrets(e)
					if err != nil {
						return err
					}
				}
				if _, err := w.Write([]byte(fmt.Sprintf("%s\n", re))); err != nil {
					return fmt.Errorf("failed to write to output: %w", err)
				}
			}
		}
	}

	return nil
}

func (f *Flare) inEnvVarBlacklist(ev string) bool {
	if f.envVarBlacklist == nil {
		return false
	}
	return f.envVarBlacklist.Match([]byte(ev))
}

func (f *Flare) redactSecrets(line string) (string, error) {
	return redactSecrets(line, f.replacerFunc, f.scanner)
}

func redactSecrets(line string, replacerFunc RedactedValueReplacer, scanner secrets.Scanner) (string, error) {
	detectedSecrets, err := scanner.Scan(line)
	if err != nil {
		return "", err
	}

	for _, secret := range detectedSecrets {
		line = strings.Replace(line, secret.Value, replacerFunc(secret.Value), 1)
	}
	return line, nil
}
