package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const bisectCommitTemplate = `{{ range $val := . }}
ID:            {{ $val.Id|yellow }}{{if $val.Committer }}
Author:        {{ $val.Committer }}{{end}}
Date:          {{ $val.CreationDate|date }}
Meta Range ID: {{ $val.MetaRangeId }}
{{ if gt ($val.Parents|len) 1 -}}
Merge:         {{ $val.Parents|join ", "|bold }}
{{ end }}
	{{ $val.Message }}
{{ if $val.Metadata.AdditionalProperties }}
Metadata:
	{{ range $key, $value := $val.Metadata.AdditionalProperties }}
	{{ $key | printf "%-18s" }} = {{ $value }}
	{{ end -}}
{{ end -}}
{{ end }}`

type BisectSelect int

const (
	BisectSelectGood BisectSelect = iota
	BisectSelectBad
)

var ErrCommitNotFound = errors.New("commit not found")

// bisectCmd represents the bisect command
var bisectCmd = &cobra.Command{
	Use:    "bisect",
	Short:  "Binary search to find the commit that introduced a bug",
	Hidden: true,
}

type Bisect struct {
	Created    time.Time     `json:"created"`
	Repository string        `json:"repository"`
	BadCommit  string        `json:"badCommit,omitempty"`
	GoodCommit string        `json:"goodCommit,omitempty"`
	Commits    []*api.Commit `json:"commits,omitempty"`
}

const bisectStartCmdArgs = 2

// bisectStartCmd represents the start command
var bisectStartCmd = &cobra.Command{
	Use:   "start <bad ref> <good ref>",
	Short: "Start a bisect session",
	Args:  cobra.ExactArgs(bisectStartCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		badURI := MustParseRefURI("bad", args[0])
		goodURI := MustParseRefURI("good", args[1])
		if goodURI.Repository != badURI.Repository {
			Die("Two references doesn't use the same repository", 1)
		}
		repository := goodURI.Repository

		// resolve repository and references
		client := getClient()
		ctx := cmd.Context()
		// check repository exists
		repoResponse, err := client.GetRepositoryWithResponse(ctx, repository)
		DieOnErrorOrUnexpectedStatusCode(repoResponse, err, http.StatusOK)
		if repoResponse.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		state := &Bisect{
			Created:    time.Now().UTC(),
			Repository: repoResponse.JSON200.Id,
			BadCommit:  resolveCommitOrDie(ctx, client, badURI.Repository, badURI.Ref),
			GoodCommit: resolveCommitOrDie(ctx, client, goodURI.Repository, goodURI.Ref),
		}
		// resolve commits
		if err := state.Update(ctx, client); err != nil {
			DieErr(err)
		}
		if err := state.Save(); err != nil {
			DieErr(err)
		}
		state.PrintStatus()
	},
}

func resolveCommitOrDie(ctx context.Context, client api.ClientWithResponsesInterface, repository, ref string) string {
	response, err := client.GetCommitWithResponse(ctx, repository, ref)
	DieOnErrorOrUnexpectedStatusCode(response, err, http.StatusOK)
	return response.JSON200.Id
}

func (b *Bisect) PrintStatus() {
	fmt.Println("Repository:", b.Repository)
	commitCount := len(b.Commits)
	switch {
	case b.BadCommit == "" && b.GoodCommit == "":
		fmt.Println("Missing both good and bad commits")
	case b.GoodCommit == "":
		fmt.Printf("Bad commit lakefs://%s/%s, waiting for good commit\n", b.Repository, b.BadCommit)
	case b.BadCommit == "":
		fmt.Printf("Good commit lakefs://%s/%s, waiting for bad commit\n", b.Repository, b.GoodCommit)
	case commitCount == 0:
		fmt.Println("No commits found")
	case commitCount == 1:
		commit := b.Commits[0]
		fmt.Printf("Found commit lakefs://%s/%s %s\n", b.Repository, commit.Id, commit.Message)
	default:
		steps := math.Log2(float64(commitCount))
		h := (commitCount >> 1) - 1
		fmt.Printf("Bisecting: %d commits left to test after this (roughly %d steps)\n", h, int(steps))
		commit := b.Commits[h]
		fmt.Printf("Current commit lakefs://%s/%s %s\n", b.Repository, commit.Id, commit.Message)
	}
}

// bisectResetCmd represents the reset command
var bisectResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Clean up the bisection state",
	Run: func(cmd *cobra.Command, args []string) {
		err := BisectRemove()
		if os.IsNotExist(err) {
			Die("No active bisect session", 1)
		}
		if err != nil {
			DieErr(err)
		}
		fmt.Println("Cleared bisect session")
	},
}

// bisectBadCmd represents the bad command
var bisectBadCmd = &cobra.Command{
	Use:     "bad",
	Aliases: []string{"new"},
	Short:   "Set 'bad' commit that is known to contain the bug",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		bisectCmdSelect(BisectSelectBad)
	},
}

func bisectCmdSelect(bisectSelect BisectSelect) {
	var state Bisect
	err := state.Load()
	if os.IsNotExist(err) {
		Die(`You need to start by "bisect start"`, 1)
	}
	if err := state.SaveSelect(bisectSelect); err != nil {
		DieErr(err)
	}
	state.PrintStatus()
}

// bisectGoodCmd represents the good command
var bisectGoodCmd = &cobra.Command{
	Use:     "good",
	Aliases: []string{"old"},
	Short:   "Set current commit as 'good' commit that is known to be before the bug was introduced",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		bisectCmdSelect(BisectSelectGood)
	},
}

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

// bisectLogCmd represents the log command
var bisectLogCmd = &cobra.Command{
	Use:   "log",
	Short: "Print out the current bisect state",
	Run: func(cmd *cobra.Command, args []string) {
		var state Bisect
		err := state.Load()
		if os.IsNotExist(err) {
			Die(`You need to start by "bisect start"`, 1)
		}
		state.PrintStatus()
	},
}

// bisectViewCmd represents the log command
var bisectViewCmd = &cobra.Command{
	Use:   "view",
	Short: "Current bisect commits",
	Run: func(cmd *cobra.Command, args []string) {
		var state Bisect
		err := state.Load()
		if os.IsNotExist(err) {
			Die(`You need to start by "bisect start"`, 1)
		}
		if len(state.Commits) == 0 {
			state.PrintStatus()
			return
		}
		Write(bisectCommitTemplate, state.Commits)
	},
}

const bisectFilePath = "~/.lakectl.bisect.json"

func BisectRemove() error {
	expand, err := homedir.Expand(bisectFilePath)
	if err != nil {
		return err
	}
	return os.Remove(expand)
}

func (b *Bisect) Load() error {
	expand, err := homedir.Expand(bisectFilePath)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(expand)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, b)
}

func (b *Bisect) Save() error {
	expand, err := homedir.Expand(bisectFilePath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	const fileMode = 0o644
	return os.WriteFile(expand, data, fileMode)
}

func (b *Bisect) Update(ctx context.Context, client api.ClientWithResponsesInterface) error {
	if b.GoodCommit == "" || b.BadCommit == "" {
		b.Commits = nil
		return nil
	}

	// scan commit log from bad to good (not included) and save them into state
	b.Commits = nil
	const amountPerCall = 500
	params := &api.LogCommitsParams{
		Amount: api.PaginationAmountPtr(amountPerCall),
	}
	var commits []*api.Commit
	for {
		logResponse, err := client.LogCommitsWithResponse(ctx, b.Repository, b.BadCommit, params)
		DieOnErrorOrUnexpectedStatusCode(logResponse, err, http.StatusOK)
		if logResponse.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		results := logResponse.JSON200.Results
		for i := range results {
			if results[i].Id == b.GoodCommit {
				b.Commits = commits
				return nil
			}
			commits = append(commits, &results[i])
		}
		if !logResponse.JSON200.Pagination.HasMore {
			break
		}
		params.After = api.PaginationAfterPtr(logResponse.JSON200.Pagination.NextOffset)
	}
	return fmt.Errorf("good %w", ErrCommitNotFound)
}

func (b *Bisect) SaveSelect(sel BisectSelect) error {
	if len(b.Commits) <= 1 {
		return nil
	}
	h := len(b.Commits) >> 1
	switch sel {
	case BisectSelectGood:
		b.Commits = b.Commits[:h]
	case BisectSelectBad:
		b.Commits = b.Commits[h:]
	default:
		DieFmt("Unknown bisect select - %d", sel)
	}
	return b.Save()
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(bisectCmd)

	bisectCmd.AddCommand(bisectStartCmd)
	bisectCmd.AddCommand(bisectResetCmd)
	bisectCmd.AddCommand(bisectBadCmd)
	bisectCmd.AddCommand(bisectGoodCmd)
	bisectCmd.AddCommand(bisectRunCmd)
	bisectCmd.AddCommand(bisectViewCmd)
	bisectCmd.AddCommand(bisectLogCmd)
}
