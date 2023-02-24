package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	LocalSyncConcurrency = 25
	gitCommitKeyName     = "git.commit.id"
)

func mustGitConf(path string) *local.Repository {
	gitRepo, err := local.FindRepository(path)
	if errors.Is(err, local.ErrNotInRepository) {
		// not a git repo
		DieFmt("please run this command in the context of a local Git repository")
	} else if err != nil {
		DieErr(err)
	}
	return gitRepo
}

func mustLocalDeps(ctx context.Context, path string) (*local.Repository, *local.Manifest, *local.APIWrapper) {
	gitRepo := mustGitConf(path)
	manifest, err := local.LoadManifest(gitRepo.Root())
	if err != nil {
		DieErr(err)
	}
	wrapper := local.NewAPIWrapper(ctx, getClient())
	return gitRepo, manifest, wrapper
}

var localDiffChangeTypes = []string{"modified", "added", "removed"}

var localDiffColors = map[string]text.Color{
	"added":    text.FgGreen,
	"removed":  text.FgRed,
	"modified": text.FgYellow,
}

func printLocalDiff(d *local.Diff) (total int) {
	for i, diffType := range [][]string{d.Modified, d.Added, d.Removed} {
		color := localDiffColors[localDiffChangeTypes[i]]
		for _, change := range diffType {
			fmt.Print(color.Sprintf("\t%s %s\n", localDiffChangeTypes[i], change))
			total += 1
		}
	}
	if total == 0 {
		fmt.Print("\n\tNo local changes\n")
	}
	return
}

func syncDirectory(gitRepo *local.Repository, manifest *local.Manifest, client *local.APIWrapper, maxParallelism int, update bool) error {
	source, err := manifest.RemoteURI()
	DieIfErr(err)
	currentStableRef := manifest.Head
	if update || currentStableRef == "" {
		currentStableRef, err = client.Dereference(source)
		DieIfErr(err)
		manifest.Head = currentStableRef
		DieIfErr(manifest.Save())
	}
	for targetDirectory, s := range manifest.Sources {
		// sync the thing!
		stableSource := &uri.URI{
			Repository: source.Repository,
			Ref:        currentStableRef,
			Path:       swag.String(s.RemotePath),
		}
		fullPath, err := gitRepo.RelativeToRoot(targetDirectory)
		DieIfErr(err)
		DieIfErr(client.SyncDirectory(stableSource, fullPath, maxParallelism))
	}
	return nil
}

// localCmd is for integration with local execution engines!
var localCmd = &cobra.Command{
	Short: "commands used to sync and reproduce data from lakeFS locally",
}

var localAddCmd = &cobra.Command{
	Use:     "add <directory> [<remote path>]",
	Short:   "add a local directory to a lakeFS branch under the specified uri",
	Example: "add training_data datasets/training/",
	Args:    cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		targetDirectory := args[0]
		remotePath := targetDirectory
		if len(args) > 1 {
			remotePath = args[1]
		}

		gitRepo, manifest, client := mustLocalDeps(cmd.Context(), targetDirectory)

		pathInRepository, err := gitRepo.RelativeToRoot(targetDirectory)
		DieIfErr(err)
		fullPath, err := gitRepo.RelativeToRoot(pathInRepository)
		DieIfErr(err)
		if manifest.HasSource(pathInRepository) {
			DieFmt("directory already tracked in lakeFS. See data.yaml`.")
		}
		locationExists, err := local.DirectoryExists(fullPath)
		DieIfErr(err)
		if !locationExists {
			DieIfErr(os.MkdirAll(fullPath, local.DefaultDirectoryMask))
		}

		// make sure it *doesn't* exist on the server
		remoteUri, err := manifest.RemoteURI()
		DieIfErr(err)
		remoteUri = &uri.URI{
			Repository: remoteUri.Repository,
			Ref:        remoteUri.Ref,
			Path:       swag.String(remotePath),
		}
		remotePathExists, err := client.PathExists(remoteUri)
		DieIfErr(err)
		if remotePathExists {
			DieFmt("lakeFS directory already exists at '%s' - perhaps clone it instead?", remoteUri.String())
		}

		// init an empty index
		err = local.InitEmptyIndex(fullPath)
		DieIfErr(err)

		// add it to data.yaml
		manifest.SetSource(pathInRepository, remotePath)
		DieIfErr(manifest.Save())

		// add it to git ignore
		err = gitRepo.AddIgnorePattern(pathInRepository)
		DieIfErr(err)
	},
}

// localCloneCmd clones a lakeFS directory locally (committed only)
// and updates the `data.yaml` file that describes local cloned paths
var localCloneCmd = &cobra.Command{
	Use:     "clone <remote path> [<target directory>]",
	Short:   "clone a lakeFS directory locally (committed data only)",
	Example: "clone datasets/my/dataset/ my_input/",
	Args:    cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		// parse args
		remotePath := args[0]
		targetDirectory := remotePath
		if len(args) > 1 {
			targetDirectory = args[1]
		}
		gitRepo, manifest, client := mustLocalDeps(cmd.Context(), targetDirectory)
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)

		var fullPath string
		var pathInRepository string
		pathInRepository, err = gitRepo.RelativeToRoot(targetDirectory)
		DieIfErr(err)
		fullPath, err = gitRepo.RelativeToRoot(pathInRepository)
		DieIfErr(err)
		if manifest.HasSource(pathInRepository) {
			DieFmt("directory already cloned. You can try running `pull`.")
		}
		locationExists, err := local.DirectoryExists(fullPath)
		DieIfErr(err)
		if locationExists {
			DieFmt("directory already exists. Try a different location?")
		}
		isClean, err := local.IsDataClean(gitRepo, manifest)
		DieIfErr(err)
		if !isClean {
			DieFmt("cannot pull latest data while data directories have uncommitted changes. See `git-data status`")
		}
		// add it to data.yaml
		manifest.SetSource(pathInRepository, remotePath)
		DieIfErr(manifest.Save())
		// add it to git ignore
		err = gitRepo.AddIgnorePattern(pathInRepository)
		DieIfErr(err)
		// run the actual sync
		DieIfErr(syncDirectory(gitRepo, manifest, client, maxParallelism, false))
	},
}

var localStatusCmd = &cobra.Command{
	Use:   "status [<target directory>]",
	Short: "show local changes to data pulled from lakeFS",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			// directory passed
			gitRepo, manifest, _ := mustLocalDeps(cmd.Context(), args[0])
			fullPath, err := gitRepo.RelativeToRoot(args[0])
			DieIfErr(err)
			if !manifest.HasSource(fullPath) {
				DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
			}
			fmt.Printf("Directory: '%s':\n\n", fullPath)
			diffResults, err := local.DiffPath(fullPath)
			DieIfErr(err)
			printLocalDiff(diffResults)
			return
		}

		gitRepo, manifest, _ := mustLocalDeps(cmd.Context(), ".")
		for pathInRepository := range manifest.Sources {
			fmt.Printf("Directory: '%s':\n\n", pathInRepository)
			fullPath, err := gitRepo.RelativeToRoot(pathInRepository)
			DieIfErr(err)
			diffResults, err := local.DiffPath(fullPath)
			DieIfErr(err)
			printLocalDiff(diffResults)
			fmt.Print("\n\n")
		}
	},
}

var localCommitCmd = &cobra.Command{
	Use:   "commit",
	Short: "upload & commit changes to data files to the remote lakeFS repository",
	Run: func(cmd *cobra.Command, args []string) {
		gitRepo, manifest, client := mustLocalDeps(cmd.Context(), ".")
		// parse commit arguments from command line
		kvPairs, err := getKV(cmd, "meta")
		DieIfErr(err)
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		message, err := cmd.Flags().GetString("message")
		DieIfErr(err)

		remoteUri, err := manifest.RemoteURI()
		DieIfErr(err)

		// make sure we don't have any dirty writes on the lakeFS branch
		hasUncommitted, err := client.HasUncommittedChanges(remoteUri)
		DieIfErr(err)
		if hasUncommitted {
			DieFmt("your lakeFS branch already has uncommitted changes. Please commit/revert those first!")
		}

		// make sure our current ref is also the latest
		latestCommitId, err := client.Dereference(remoteUri)
		DieIfErr(err)
		if latestCommitId != manifest.Head {
			DieFmt("local copy of lakeFS branch '%s' is not up to date with server. "+
				"Please run `pull` first.", remoteUri.Ref)
		}

		// upload changes
		for directory, src := range manifest.Sources {
			// let's go!
			fullPath, err := gitRepo.RelativeToRoot(directory)
			DieIfErr(err)
			uploadUri, err := manifest.RemoteURIForPath(src.RemotePath, true)
			DieIfErr(client.UploadDirectoryChanges(uploadUri, fullPath, maxParallelism))
		}

		currentCommitId, err := gitRepo.CurrentCommitId()
		if err != nil && errors.Is(err, local.ErrNoCommitFound) {
			currentCommitId = ""
		} else if err != nil {
			DieErr(err)
		}
		remoteUrls, err := gitRepo.GetRemoteURLs()
		DieIfErr(err)
		if currentCommitId != "" {
			kvPairs[gitCommitKeyName] = currentCommitId
		}
		for remoteName, remoteUrl := range remoteUrls {
			kvPairs[fmt.Sprintf("git.remotes.%s.url", remoteName)] = remoteUrl
		}
		commitId, err := client.Commit(remoteUri, message, kvPairs)
		DieIfErr(err)

		// ensure all directories are in sync
		for directory, src := range manifest.Sources {
			// let's go!
			fullPath, err := gitRepo.RelativeToRoot(directory)
			DieIfErr(err)
			updatedSource := &uri.URI{
				Repository: remoteUri.Repository,
				Ref:        commitId,
				Path:       swag.String(src.RemotePath),
			}
			DieIfErr(client.SyncDirectory(updatedSource, fullPath, maxParallelism))
		}
		manifest.Head = commitId
		DieIfErr(manifest.Save())
	},
}

var localPullCmd = &cobra.Command{
	Use:   "pull",
	Short: "pull data files from lakeFS as described in data.yaml",
	Run: func(cmd *cobra.Command, args []string) {
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		update, err := cmd.Flags().GetBool("update")
		DieIfErr(err)
		force, err := cmd.Flags().GetBool("force")
		DieIfErr(err)

		gitRepo, manifest, client := mustLocalDeps(cmd.Context(), ".")
		isClean, err := local.IsDataClean(gitRepo, manifest)
		DieIfErr(err)

		if !isClean && !force {
			DieFmt("you have uncommitted changes to data (see `status`). " +
				"Either commit or use --force to overwrite them")
		}
		DieIfErr(syncDirectory(gitRepo, manifest, client, maxParallelism, update))
	},
}

var localCheckoutCmd = &cobra.Command{
	Use:     "checkout <lakeFS ref>",
	Example: "checkout my-branch",
	Short:   "sync this repository's data  with the specified ref",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		refName := args[0]
		gitRepo, manifest, client := mustLocalDeps(cmd.Context(), ".")
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		ref, err := manifest.RemoteURI()
		DieIfErr(err)
		ref.Ref = refName
		manifest.Remote = ref.String()
		manifest.Head = ""
		DieIfErr(manifest.Save())
		DieIfErr(syncDirectory(gitRepo, manifest, client, maxParallelism, true))
	},
}

var localInitCmd = &cobra.Command{
	Use:     "init <lakeFS ref>",
	Example: "init lakefs://example-repo/main",
	Short:   "initialize the current Git repository to track the remote lakeFS repository",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ref := MustParseRefURI("lakeFS ref", args[0])
		gitRepo := mustGitConf(".")
		manifest, err := local.CreateManifest(gitRepo.Root())
		DieIfErr(err)
		manifest.Remote = ref.String()
		DieIfErr(manifest.Save())
	},
}

func initLocalAsSubcommand(root *cobra.Command) {
	root.AddCommand(localCmd)
	initLocal("local")
}

func initLocal(name string) {
	localCmd.Use = name

	localCmd.AddCommand(localInitCmd)

	localCmd.AddCommand(localStatusCmd)

	localCmd.AddCommand(localCloneCmd)
	localCloneCmd.Flags().IntP("parallelism", "p", LocalSyncConcurrency, "maximum objects to download in parallel")

	localCmd.AddCommand(localAddCmd)

	localCmd.AddCommand(localCheckoutCmd)
	localCheckoutCmd.Flags().IntP("parallelism", "p", LocalSyncConcurrency, "maximum objects to download in parallel")

	localCmd.AddCommand(localCommitCmd)
	localCommitCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
	localCommitCmd.Flags().StringP("message", "m", "", "commit message to use for the resulting lakeFS commit")
	localCommitCmd.Flags().IntP("parallelism", "p", LocalSyncConcurrency, "maximum objects to upload in parallel")

	localCmd.AddCommand(localPullCmd)
	localPullCmd.Flags().IntP("parallelism", "p", LocalSyncConcurrency, "maximum objects to download in parallel")
	localPullCmd.Flags().BoolP("update", "u", false, "pull the latest data available on the remote (and update data.yaml)")
	localPullCmd.Flags().BoolP("force", "f", false, "force pull data from the remote. Will overwrite any local changes.")
}
