package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

var bisectRunCmd = &cobra.Command{
	Use:   "run <command>",
	Short: "Bisecting based on command status code",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var state Bisect
		err := state.Load()
		if os.IsNotExist(err) {
			Die(`You need to start by "bisect start"`, 1)
		}
		ctx := cmd.Context()
		for len(state.Commits) > 1 {
			h := len(state.Commits) >> 1
			commit := state.Commits[h]

			fmt.Printf("== Commit [%s] %s\n", commit.Id, commit.Message)
			c := bisectRunCommand(ctx, commit, args[0], args[1:])
			var bisectSelect BisectSelect
			if err := c.Run(); err != nil {
				bisectSelect = BisectSelectBad
				fmt.Printf("-- BAD [%s] %s\n", commit.Id, commit.Message)
			} else {
				bisectSelect = BisectSelectGood
				fmt.Printf("-- GOOD [%s] %s\n", commit.Id, commit.Message)
			}
			if err := state.SaveSelect(bisectSelect); err != nil {
				DieErr(err)
			}
		}
		state.PrintStatus()
	},
}

func bisectRunCommand(ctx context.Context, commit *api.Commit, name string, args []string) *exec.Cmd {
	// prepare args
	replacer := strings.NewReplacer(
		":BISECT_ID:", commit.Id,
		":BISECT_MESSAGE:", commit.Message,
		":BISECT_METARANGE_ID:", commit.MetaRangeId,
		":BISECT_COMMITTER:", commit.Committer,
	)
	cmdArgs := make([]string, 0, len(args))
	for _, arg := range args {
		cmdArgs = append(cmdArgs, replacer.Replace(arg))
	}

	// new command
	runCommand := exec.CommandContext(ctx, name, cmdArgs...)

	// prepare env - add BISECT_* based on commit
	runCommand.Env = runCommand.Environ()
	runCommand.Env = append(runCommand.Env, "BISECT_ID="+commit.Id)
	runCommand.Env = append(runCommand.Env, "BISECT_MESSAGE="+commit.Message)
	runCommand.Env = append(runCommand.Env, "BISECT_METARANGE_ID="+commit.MetaRangeId)
	runCommand.Env = append(runCommand.Env, "BISECT_COMMITTER="+commit.Committer)

	// set output to the process output
	runCommand.Stdout = os.Stdout
	runCommand.Stderr = os.Stderr
	return runCommand
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectRunCmd)
}
