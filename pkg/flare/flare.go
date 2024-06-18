// Package flare is used for collecting, sanitizing, and packaging lakeFS configuration, log files, and environment variables
// for debugging and troubleshooting purposes.
package flare

import (
	"archive/zip"
	"bufio"
	"crypto/sha512"
	"encoding/json"
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

	secretScanner        secrets.Scanner
	secretScannerInitErr error

	plainTextLogDateRegex = regexp.MustCompile(`^[-_A-Z]+\s+(?P<date>\[\d{4}(?:.\d{2}){2}(?:\s|T)(?:\d{2}.){2}\d{2}[\+-]?\d{2}:\d{2}\])`)
	expectedSubmatches    = 2
)

type WithTime struct {
	Time time.Time
}

type Flare struct {
	envVarPrefixes []string
	logFormat      LogFormat
	// LogDateLayout is the layout used by time.Parse to parse dates in log lines.
	// The default value is time.RFC3339.
	// This can be changed using the WithLogDateLayout option.
	LogDateLayout string
	replacerFunc  RedactedValueReplacer
}

func defaultSecretReplacer(value string) string {
	sha_512 := sha512.New()
	sha_512.Write([]byte(value))
	return string(sha_512.Sum(nil))
}

type Option func(*Flare)

func NewFlare(logFormat LogFormat, options ...Option) (*Flare, error) {
	if secretScannerInitErr != nil {
		return nil, fmt.Errorf("failed to init secrets scanner: %w", secretScannerInitErr)
	}
	flare := &Flare{
		envVarPrefixes: defaultEnvVarPrefixes,
		replacerFunc:   defaultSecretReplacer,
		LogDateLayout:  time.RFC3339,
		logFormat:      logFormat,
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

// WithSecretReplacerFunc replaces the default secret replacement func with a function that takes the raw value and returns the redacted value
// The default secret replacement func replaces the secret with a SHA512 hash of the secret value.
// This allows comparison of values without exposing the secret values.
func WithSecretReplacerFunc(fn RedactedValueReplacer) Option {
	return func(f *Flare) {
		f.replacerFunc = fn
	}
}

// WithLogDateLayout sets an alternate date layout for parsing dates in log lines.
func WithLogDateLayout(l string) Option {
	return func(f *Flare) {
		f.LogDateLayout = l
	}
}

// LogLineDateExtractor represents a function that extracts the date from a log line in a specific format.
type LogLineDateExtractor func(line, logDateLayout string) (time.Time, error)

var dateExtractors = map[LogFormat]LogLineDateExtractor{
	LogFormatJSON:      extractDateFromJSONLine,
	LogFormatPlainText: extractDateFromPlainTextLine,
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

func (f *Flare) processEnvVars(w io.Writer) error {
	for _, e := range os.Environ() {
		for _, p := range f.envVarPrefixes {
			if strings.HasPrefix(e, p) {
				re, err := f.redactSecrets(e)
				if err != nil {
					return err
				}
				if _, err := w.Write([]byte(fmt.Sprintf("%s\n", re))); err != nil {
					return fmt.Errorf("failed to write to output: %w", err)
				}
			}
		}
	}

	return nil
}

// ZipFolder zips the files created during the flare process into a single file
// for sharing and writes to outputPath
func (f *Flare) ZipFolder(inputPath, outputPath string) (retErr error) {
	ex, err := os.Executable()
	if err != nil {
		return err
	}

	fl, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer fl.Close()

	w := zip.NewWriter(fl)
	defer func() {
		e := w.Close()
		if retErr == nil {
			retErr = e
		}
	}()

	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		exPath := filepath.Dir(ex)
		relPath, err := filepath.Rel(exPath, path)
		if err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		defer file.Close()

		f, err := w.Create(relPath)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}

		_, err = io.Copy(f, file)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}

		return nil
	}
	err = filepath.Walk(inputPath, walker)
	return fmt.Errorf("%s: %w", outputPath, err)
}

// SupportedLogFormat checks that the string provided is one of the supported LogFormats
func SupportedLogFormat(format string) bool {
	v := LogFormat(format)
	return v == LogFormatJSON || v == LogFormatPlainText
}

// ProcessLogFiles processes log files in inputPath.
// Log files have a size limit and when rolling over to a new file, the current file is renamed with the cutoff time.Time before the extension.
// Processed files are read line-by-line. Each line is filtered according to the date range,
// sanitized from secrets, and is written to the file in outputPath.
func (f *Flare) ProcessLogFiles(inputPath, outputPath string, startDate, endDate *time.Time, getWriterFunc GetFileWriterFunc) (retErr error) {
	logDir, logFile := filepath.Split(inputPath)
	filename, ext := getFilenameAndExt(logFile)

	files, err := os.ReadDir(logDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasPrefix(file.Name(), filename) || filepath.Ext(file.Name()) != ext {
			continue
		}
		err = f.processLogFile(fmt.Sprintf("%s/%s", logDir, file.Name()), outputPath, startDate, endDate, getWriterFunc)
		if err != nil {
			return err
		}
	}

	return nil
}

func getFilenameAndExt(in string) (string, string) {
	return strings.TrimPrefix(filepath.Base(in), filepath.Ext(in)), filepath.Ext(in)
}

func extractDateFromPlainTextLine(line, logDateLayout string) (time.Time, error) {
	matches := plainTextLogDateRegex.FindStringSubmatch(line)
	if len(matches) != expectedSubmatches {
		return time.Time{}, ErrDateNotFound
	}

	strDate := matches[1][1 : len(matches[1])-1]
	date, err := time.Parse(logDateLayout, strDate)
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}

func extractDateFromJSONLine(line, logDateLayout string) (time.Time, error) {
	var t WithTime
	err := json.Unmarshal([]byte(line), &t)
	if err != nil {
		return time.Time{}, err
	}

	if t.Time.IsZero() {
		return time.Time{}, ErrExtractDateFromJSONLogLine
	}

	return t.Time, nil
}

func (f *Flare) handleLogLine(line string, start, end *time.Time) (string, error) {
	lineDate, err := dateExtractors[f.logFormat](line, f.LogDateLayout)
	if err != nil {
		return "", err
	}
	if !((start == nil || lineDate.Equal(*start) || lineDate.After(*start)) &&
		(end == nil || lineDate.Equal(*end) || lineDate.Before(*end))) {
		return "", nil
	}

	redactedLine, err := f.redactSecrets(line)
	if err != nil {
		return "", err
	}
	return redactedLine, nil
}

func (f *Flare) processLogFile(inputFileName, outputPath string, startDate, endDate *time.Time, getWriterFunc GetFileWriterFunc) (retErr error) {
	skippedLines := 0
	sourceFile, err := os.Open(inputFileName)
	if err != nil {
		return fmt.Errorf("failed to open input log file %s: %w", inputFileName, err)
	}
	defer sourceFile.Close()

	outputFileName := filepath.Join(outputPath, filepath.Base(inputFileName))
	w, err := getWriterFunc(outputFileName)
	if err != nil {
		return fmt.Errorf("%s: %w", outputFileName, err)
	}
	defer func() {
		e := w.Close()
		if retErr == nil {
			retErr = e
		}
	}()

	r := bufio.NewReader(sourceFile)

	for {
		line, err := r.ReadString('\n')
		if line == "" && err == io.EOF {
			break
		}
		pLine, err := f.handleLogLine(line, startDate, endDate)
		if err != nil {
			skippedLines += 1
			fmt.Fprintf(os.Stderr, "failed to process log line: %s\n", err)
		}

		if pLine != "" {
			if _, err := w.Write([]byte(pLine)); err != nil {
				return fmt.Errorf("failed to write to output file %s: %w", outputFileName, err)
			}
		}
	}

	fmt.Printf("Skipped %d log lines\n", skippedLines)
	return nil
}

func (f *Flare) redactSecrets(line string) (string, error) {
	return redactSecrets(line, f.replacerFunc)
}

func redactSecrets(line string, replacerFunc RedactedValueReplacer) (string, error) {
	detectedSecrets, err := secretScanner.Scan(line)
	if err != nil {
		return "", err
	}

	for _, secret := range detectedSecrets {
		line = strings.Replace(line, secret.Value, replacerFunc(secret.Value), 1)
	}
	return line, nil
}

//nolint:gochecknoinits
func init() {
	// remove the ini transformer because it has false positives with plain text log lines
	config := scanner.NewConfigBuilderFrom(scanner.NewConfigWithDefaults()).RemoveTransformers("ini").Build()
	// set zero threshold for entropy-based detection in the keyword based detector
	// example: LAKEFS_AUTH_ENCRYPT_SECRET_KEY=123asdasd will be detected by the {secret, key} keywords regardless of the value
	config.DetectorConfigs["keyword"] = []string{"0"}
	s, err := scanner.NewScannerFromConfig(config)
	if err != nil {
		secretScannerInitErr = err
	}
	secretScanner = s
}
