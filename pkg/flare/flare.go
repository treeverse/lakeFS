// Package flare is used for collecting, sanitizing, and packaging lakeFS configuration, log files, and environment variables
// for debugging and troubleshooting purposes.
package flare

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/octarinesec/secret-detector/pkg/scanner"
	"github.com/octarinesec/secret-detector/pkg/secrets"
	"gopkg.in/yaml.v3"
)

// defaults
const (
	defaultLogDateLayout          = "2006-01-02T15:04:05-07:00"
	defaultSecretReplacementValue = "<REDACTED>"
)

var defaultEnvVarPrefixs = []string{"LAKEFS_", "HTTP_", "HOSTNAME"}

const (
	DirPermissions  = 0700
	FilePremissions = 0600
)

// LogFormat is a log file format supported by the flare package
type LogFormat string

const (
	LogFormatJSON      LogFormat = "json"
	LogFormatPlainText LogFormat = "text"
)

var (
	ErrExtractDateFromJSONLogLine = errors.New("failed to extract date from log line")
	ErrDateNotFound               = errors.New("date not found in log line")
	ErrNoWriterProvided           = errors.New("writer was not provided for processing environment variables")

	secretScanner        secrets.Scanner
	secretScannerInitErr error

	plainTextLogDateRegex = regexp.MustCompile(`\[\d{4}(.\d{2}){2}(\s|T)(\d{2}.){2}\d{2}[\+-]?\d{2}:\d{2}\]`)
)

type Flare struct {
	envVarPrefixes         []string
	secretReplacementValue string
	logFormat              LogFormat
	// LogDateLayout is the layout used by time.Parse to parse dates in log lines.
	// The default value is "2006-01-02T15:04:05-07:00".
	// This can be changed using the WithLogDateLayout option.
	LogDateLayout string
}

type Option func(*Flare)

func NewFlare(logFormat LogFormat, options ...Option) (*Flare, error) {
	if secretScannerInitErr != nil {
		return nil, fmt.Errorf("failed to init secrets scanner: %w", secretScannerInitErr)
	}
	flare := &Flare{
		envVarPrefixes:         defaultEnvVarPrefixs,
		secretReplacementValue: defaultSecretReplacementValue,
		LogDateLayout:          defaultLogDateLayout,
		logFormat:              logFormat,
	}

	for _, opt := range options {
		opt(flare)
	}

	return flare, nil
}

// WithEnvVarPrefixes is used to replace the default list of environment variable prefixes that flare processes.
// The default list is "LAKEFS_", "FLUFFY_", "HTTP_", "HOSTNAME".
func WithEnvVarPrefixes(prefixes []string) Option {
	return func(f *Flare) {
		f.envVarPrefixes = prefixes
	}
}

// WithAdditionalEnvVarPrefix is used to add additional environment prefixes to the default list that flare processes.
// The default list is "LAKEFS_", "FLUFFY_", "HTTP_", "HOSTNAME".
func WithAdditionalEnvVarPrefix(envVar string) Option {
	return func(f *Flare) {
		f.envVarPrefixes = append(f.envVarPrefixes, envVar)
	}
}

// WithSecretReplacementValue is used to replace the default secret replacement value of "<REDACTED>".
func WithSecretReplacementValue(v string) Option {
	return func(f *Flare) {
		f.secretReplacementValue = v
	}
}

// WithLogDateLayout is used to set an alternate date layout for parsing dates in log lines.
func WithLogDateLayout(l string) Option {
	return func(f *Flare) {
		f.LogDateLayout = l
	}
}

// Marshalable is used to limit the types passable as a template parameter to MaskConfig
type Marshalable[T any] interface {
	Marshal() ([]byte, error)
}

// LogLineDateExtractor represents a function that extracts the date from a log line in a specific format.
type LogLineDateExtractor func(line, logDateLayout string) (time.Time, error)

var dateExpractors = map[LogFormat]LogLineDateExtractor{
	LogFormatJSON:      extractDateFromJSONLine,
	LogFormatPlainText: extractDateFromPlainTextLine,
}

// ProcessConfig takes a config struct, marshals it to YAML and writes it out to outputPath
func (f *Flare) ProcessConfig(cfg interface{}, outputPath, fileName string) (retErr error) {
	yamlCfg, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	configOutPath := outputPath + fileName
	outputFile, err := os.Create(configOutPath)
	if err != nil {
		return err
	}
	defer func() {
		if e := outputFile.Close(); retErr == nil {
			retErr = e
		}
	}()

	err = os.WriteFile(configOutPath, yamlCfg, FilePremissions)
	return err
}

// ProcessEnvVars iterates over all defined env vars, filters them according to the defined prefixes,
// redacts secrets, and writes them out to file.
func (f *Flare) ProcessEnvVars(outPath, fileName string) (retErr error) {
	outputFile, err := os.OpenFile(outPath+fileName, os.O_WRONLY|os.O_CREATE, FilePremissions)
	if err != nil {
		return err
	}
	defer func() {
		if e := outputFile.Close(); retErr == nil {
			retErr = e
		}
	}()
	bw := bufio.NewWriter(outputFile)
	defer func() {
		if e := bw.Flush(); retErr == nil {
			retErr = e
		}
	}()

	err = f.processEnvVars(bw)
	return err
}

func (f *Flare) processEnvVars(bw *bufio.Writer) error {
	if bw == nil {
		return ErrNoWriterProvided
	}

	for _, e := range os.Environ() {
		for _, p := range f.envVarPrefixes {
			if strings.HasPrefix(e, p) {
				re, err := f.redactSecrets(e)
				if err != nil {
					return err
				}
				if _, err := bw.WriteString(fmt.Sprintf("%s\n", re)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// ZipFolder zips the files created during the flare process into a single file
// for sharing and writes to outputPath
func (f *Flare) ZipFolder(inputPath, outputPath string) error {
	fl, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer fl.Close()

	w := zip.NewWriter(fl)
	defer w.Close()

	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		ex, err := os.Executable()
		if err != nil {
			return err
		}
		exPath := filepath.Dir(ex)
		relPath, err := filepath.Rel(exPath, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		f, err := w.Create(relPath)
		if err != nil {
			return err
		}

		_, err = io.Copy(f, file)
		if err != nil {
			return err
		}

		return nil
	}
	err = filepath.Walk(inputPath, walker)
	return err
}

// HasLogFileOutput checks the outputs property under the logging property of the config
// to make sure logs are configured to be written to file.
func (f *Flare) HasLogFileOutput(outputs []string) bool {
	return slices.ContainsFunc(outputs, func(e string) bool {
		return e != "" && e != "-" && e != "="
	})
}

// SupportedLogFormat checks that the string provided is one of the supported LogFormats
func SupportedLogFormat(format string) bool {
	v := LogFormat(format)
	return v == LogFormatJSON || v == LogFormatPlainText
}

// ProcessLogFiles processes log files in inputPath. If the inputPath is a file, it processes the file.
// If inputPath is a directory, all files in the directory will be individually processed.
// Processed files are read line-by-line. Each line is filtered according to the date range,
// sanitized from secrets, and is written to the file in outputPath.
func (f *Flare) ProcessLogFiles(inputPath, outputPath string, startDate, endDate *time.Time) (retErr error) {
	isDir, err := isDirectory(inputPath)
	if err != nil {
		return err
	}

	if !isDir {
		return f.processLogFile(inputPath, outputPath, startDate, endDate)
	} else {
		files, err := os.ReadDir(inputPath)
		if err != nil {
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}
			err = f.processLogFile(file.Name(), outputPath, startDate, endDate)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func extractDateFromPlainTextLine(line, logDateLayout string) (time.Time, error) {
	idxs := plainTextLogDateRegex.FindIndex([]byte(line))
	if idxs == nil {
		return time.Time{}, ErrDateNotFound
	}

	strDate := line[idxs[0]+1 : idxs[1]-1]
	date, err := time.Parse(logDateLayout, strDate)
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}

func extractDateFromJSONLine(line, logDateLayout string) (time.Time, error) {
	logPropsMap := make(map[string](interface{}))
	err := json.Unmarshal([]byte(line), &logPropsMap)
	if err != nil {
		return time.Time{}, err
	}
	lineDateStr, ok := logPropsMap["time"].(string)
	if !ok {
		return time.Time{}, ErrExtractDateFromJSONLogLine
	}
	lineDate, err := time.Parse(logDateLayout, lineDateStr)
	if err != nil {
		return time.Time{}, err
	}
	return lineDate, nil
}

func (f *Flare) handleLogLine(line string, start, end *time.Time) (string, error) {
	lineDate, err := dateExpractors[f.logFormat](line, f.LogDateLayout)
	if err != nil {
		return "", err
	}
	if (start == nil || lineDate.Equal(*start) || lineDate.After(*start)) &&
		(end == nil || lineDate.Equal(*end) || lineDate.Before(*end)) {
		redactedLine, err := f.redactSecrets(line)
		if err != nil {
			return "", err
		}
		return redactedLine, nil
	}
	return "", nil
}

func (f *Flare) processLogFile(inputFileName, outputPath string, startDate, endDate *time.Time) (retErr error) {
	skippedLines := 0
	sourceFile, err := os.Open(inputFileName)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	outputFileName := fmt.Sprintf("%s%s", outputPath, inputFileName)
	outputFile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, FilePremissions)
	if err != nil {
		return err
	}
	defer func() {
		if e := outputFile.Close(); retErr == nil {
			retErr = e
		}
	}()

	bw := bufio.NewWriter(outputFile)
	defer func() {
		if e := bw.Flush(); retErr == nil {
			retErr = e
		}
	}()

	first := true
	scanner := bufio.NewScanner(sourceFile)

	for scanner.Scan() {
		line := scanner.Text()
		pLine, err := f.handleLogLine(line, startDate, endDate)
		if err != nil {
			skippedLines += 1
			fmt.Println("failed to process log line: ", err)
		}

		if pLine != "" {
			b := strings.Builder{}
			if !first {
				b.WriteString("\n")
			}
			b.WriteString(line)
			if _, err := bw.WriteString(b.String()); err != nil {
				return err
			}
		}

		first = false
	}

	fmt.Printf("Skipped %d log lines\n", skippedLines)
	return nil
}

func isDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	return fileInfo.IsDir(), err
}

func (f *Flare) redactSecrets(line string) (string, error) {
	return redactSecrets(line, f.secretReplacementValue)
}

func redactSecrets(line string, secretReplacementValue string) (string, error) {
	detectedSecrets, err := secretScanner.Scan(line)
	if err != nil {
		return "", err
	}

	for _, secret := range detectedSecrets {
		line = strings.Replace(line, secret.Value, secretReplacementValue, 1)
	}
	return line, nil
}

//nolint:gochecknoinits
func init() {
	// Registering our custom IP detector with the secrets scanner
	err := secrets.GetDetectorFactory().Register(DetectorName, NewIPDetector)
	if err != nil {
		secretScannerInitErr = err
	}
	// Adding our custom IP detector
	// and removing the ini transformer because it has false positives with plain text log lines
	config := scanner.NewConfigBuilderFrom(scanner.NewConfigWithDefaults()).AppendDetectors("ip").RemoveTransformers("ini").Build()
	s, err := scanner.NewScannerFromConfig(config)
	if err != nil {
		secretScannerInitErr = err
	}
	secretScanner = s
}
