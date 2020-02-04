package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/treeverse/lakefs/uri"

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
}

var branchListCmd = &cobra.Command{
	Use:     "list [repository uri]",
	Short:   "list branches in a repository",
	Example: "lakectl branch list lakefs://myrepo",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument")
		}
		u, err := uri.Parse(args[0])
		if err != nil {
			return err
		}
		if !u.IsRepository() {
			return fmt.Errorf("expected a repository URI")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		u := uri.Must(uri.Parse(args[0]))
		response, err := client.ListBranches(context.Background(), &service.ListBranchesRequest{
			RepoId: u.Repository,
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
	Use:   "create [branch uri]",
	Short: "create a new branch in a repository",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument")
		}
		u, err := uri.Parse(args[0])
		if err != nil {
			return err
		}
		if !u.IsRefspec() {
			return fmt.Errorf("expected a branch URI")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		u := uri.Must(uri.Parse(args[0]))

		sourceRawUri, _ := cmd.Flags().GetString("source")
		suri, err := uri.Parse(sourceRawUri)
		if err != nil {
			return fmt.Errorf("failed to parse source URI: %s", err)
		}
		if !strings.EqualFold(suri.Repository, u.Repository) {
			return fmt.Errorf("source branch must be in the same repository")
		}

		sourceBranch, err := client.GetBranch(context.Background(), &service.GetBranchRequest{
			RepoId:     u.Repository,
			BranchName: suri.Refspec,
		})
		if err != nil {
			return xerrors.Errorf("could not get source branch: %w", err)
		}
		fmt.Printf("got source branch '%s', using commit '%s'\n",
			suri.Refspec, sourceBranch.GetBranch().GetCommitId())
		_, err = client.CreateBranch(context.Background(), &service.CreateBranchRequest{
			RepoId:     u.Repository,
			BranchName: u.Refspec,
			CommitId:   sourceBranch.GetBranch().GetCommitId(),
		})
		return err
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:   "delete [branch uri]",
	Short: "delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument")
		}
		u, err := uri.Parse(args[0])
		if err != nil {
			return err
		}
		if !u.IsRefspec() {
			return fmt.Errorf("expected a branch URI")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		sure, err := cmd.Flags().GetBool("sure")
		if err != nil || !sure {
			return fmt.Errorf("please confirm by passing the --sure | -y flag")
		}
		client, err := getClient()
		if err != nil {
			return err
		}
		u := uri.Must(uri.Parse(args[0]))
		_, err = client.DeleteBranch(context.Background(), &service.DeleteBranchRequest{
			RepoId:     u.Repository,
			BranchName: u.Refspec,
		})
		return err
	},
}

var branchShowCmd = &cobra.Command{
	Use:   "show [branch uri]",
	Short: "show branch metadata",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument")
		}
		u, err := uri.Parse(args[0])
		if err != nil {
			return err
		}
		if !u.IsRefspec() {
			return fmt.Errorf("expected a branch URI")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		u := uri.Must(uri.Parse(args[0]))
		resp, err := client.GetBranch(context.Background(), &service.GetBranchRequest{
			RepoId:     u.Repository,
			BranchName: u.Refspec,
		})
		if err != nil {
			return err
		}
		fmt.Printf("%s\t%s\n", resp.Branch.GetName(), resp.Branch.GetCommitId())
		return nil
	},
}

func init() {
	rootCmd.AddCommand(branchCmd)
	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchDeleteCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchShowCmd)

	branchCreateCmd.Flags().StringP("source", "s", "", "source branch name")
	_ = branchCreateCmd.MarkFlagRequired("source")

	branchDeleteCmd.Flags().BoolP("sure", "y", false, "do not ask for confirmation")
}
