package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/index"
)

// treeCmd represents the tree command
var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "Dump the entire filesystem tree for the given repository and branch to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		mdb := cfg.ConnectMetadataDatabase()
		meta := index.NewDBIndex(mdb)
		return meta.Tree(repo, branch)
	},
}

func init() {
	rootCmd.AddCommand(treeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// treeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// treeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	treeCmd.Flags().StringP("repo", "r", "", "repository to list")
	treeCmd.Flags().StringP("branch", "b", "", "branch to list")
	_ = treeCmd.MarkFlagRequired("repo")
	_ = treeCmd.MarkFlagRequired("branch")
}
