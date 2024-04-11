package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
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
	Created    time.Time        `json:"created"`
	Repository string           `json:"repository"`
	BadCommit  string           `json:"badCommit,omitempty"`
	GoodCommit string           `json:"goodCommit,omitempty"`
	Commits    []*apigen.Commit `json:"commits,omitempty"`
}

const bisectStartCmdArgs = 2

func resolveCommitOrDie(ctx context.Context, client apigen.ClientWithResponsesInterface, repository, ref string) string {
	response, err := client.GetCommitWithResponse(ctx, repository, ref)
	DieOnErrorOrUnexpectedStatusCode(response, err, http.StatusOK)
	if response.JSON200 == nil {
		Die("Bad response from server", 1)
	}
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

func runBisectSelect(bisectSelect BisectSelect) {
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

func (b *Bisect) Update(ctx context.Context, client apigen.ClientWithResponsesInterface) error {
	if b.GoodCommit == "" || b.BadCommit == "" {
		b.Commits = nil
		return nil
	}

	// scan commit log from bad to good (not included) and save them into state
	b.Commits = nil
	const amountPerCall = 500
	params := &apigen.LogCommitsParams{
		Amount: apiutil.Ptr(amountPerCall),
	}
	var commits []*apigen.Commit
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
		params.After = &logResponse.JSON200.Pagination.NextOffset
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
}
