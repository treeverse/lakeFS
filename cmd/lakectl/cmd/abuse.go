package cmd

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"
)

const (
	abuseDefaultAmount      = 1000000
	abuseDefaultParallelism = 100
)

var abuseCmd = &cobra.Command{
	Use:    "abuse <sub command>",
	Short:  "Abuse a running lakeFS instance. See sub commands for more info.",
	Hidden: true,
}

func readLines(path string) (lines []string, err error) {
	reader := OpenByPath(path)
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("%w, and while closing %s", err, closeErr)
			}
		}
	}()
	scanner := bufio.NewScanner(reader)
	lines = make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(abuseCmd)
}
