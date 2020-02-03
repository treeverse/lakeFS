package cmd

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/xerrors"

	"github.com/jedib0t/go-pretty/table"

	"github.com/treeverse/lakefs/api/service"

	"github.com/spf13/cobra"
)

// branchCmd represents the branch command
var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "create and manage branches within a repository",
	Long:  `Create delete and list branches within a lakeFS repository`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("branch called")
	},
}

var branchListCmd = &cobra.Command{
	Use:   "list",
	Short: "list branches in a repository",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		repo, _ := cmd.Flags().GetString("repo")
		response, err := client.ListBranches(context.Background(), &service.ListBranchesRequest{
			RepoId: repo,
		})
		if err != nil {
			return err
		}

		// generate table
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Branch Name", "Commit ID"})
		for _, branch := range response.GetBranches() {
			t.AppendRow([]interface{}{branch.GetName(), branch.GetCommitId()})
		}
		t.Render()

		// done
		return nil
	},
}

var branchCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new branch in a repository",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		repo, _ := cmd.Flags().GetString("repo")
		sourceBranchName, _ := cmd.Flags().GetString("source")
		branchName, _ := cmd.Flags().GetString("branch")
		sourceBranch, err := client.GetBranch(context.Background(), &service.GetBranchRequest{
			RepoId:     repo,
			BranchName: sourceBranchName,
		})
		if err != nil {
			return xerrors.Errorf("could not get source branch: %w", err)
		}
		fmt.Printf("got source branch '%s', using commit '%s'\n",
			sourceBranchName, sourceBranch.GetBranch().GetCommitId())
		_, err = client.CreateBranch(context.Background(), &service.CreateBranchRequest{
			RepoId:     repo,
			BranchName: branchName,
			CommitId:   sourceBranch.GetBranch().GetCommitId(),
		})
		return err
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		repo, _ := cmd.Flags().GetString("repo")
		branchName, _ := cmd.Flags().GetString("branch")
		_, err = client.DeleteBranch(context.Background(), &service.DeleteBranchRequest{
			RepoId:     repo,
			BranchName: branchName,
		})
		return err
	},
}

var branchShowCmd = &cobra.Command{
	Use:   "show",
	Short: "show branch metadata",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	rootCmd.AddCommand(branchCmd)
	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchDeleteCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchShowCmd)

	// for the "branch" command
	branchCmd.PersistentFlags().StringP("repo", "r", "", "repository name")
	_ = cobra.MarkFlagRequired(branchCmd.PersistentFlags(), "repo")

	branchCreateCmd.Flags().StringP("branch", "b", "", "branch name")
	branchCreateCmd.Flags().StringP("source", "s", "", "source branch name")
	_ = cobra.MarkFlagRequired(branchCreateCmd.Flags(), "branch")
	_ = cobra.MarkFlagRequired(branchCreateCmd.Flags(), "from")

	branchDeleteCmd.Flags().StringP("branch", "b", "", "branch name")
	branchDeleteCmd.Flags().BoolP("sure", "y", false, "do not ask for confirmation")
	_ = cobra.MarkFlagRequired(branchDeleteCmd.Flags(), "branch")

	branchShowCmd.Flags().StringP("branch", "b", "", "branch name")
	_ = cobra.MarkFlagRequired(branchShowCmd.Flags(), "branch")
}
