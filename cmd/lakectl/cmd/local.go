package cmd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	DownloadConcurrency = 20

	gitCommitKeyName = "git.commit.id"
	gitPathKeyName   = "git.repository.data_source_path"
)

var localCmdName = "local"

func mustGitConf(path string) *local.Conf {
	if path == "" {
		path = "."
	}
	repoCfg, err := local.Config()
	if errors.Is(err, git.ErrRepositoryNotExists) {
		// not a git repo
		DieFmt("please run this command in the context of a local Git repository")
	} else if err != nil {
		DieErr(err)
	}
	return repoCfg
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

func pull(ctx context.Context, maxParallelism int, update bool, args ...string) error {
	gitLocation := "."
	if len(args) > 0 {
		gitLocation = args[0]
	}
	repoCfg := mustGitConf(gitLocation)
	client := getClient()
	if len(args) > 0 {
		pathInRepository, err := repoCfg.RelativeToRoot(args[0])
		DieIfErr(err)
		hasSource, err := repoCfg.HasSource(pathInRepository)
		DieIfErr(err)
		if !hasSource {
			return fmt.Errorf("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
		}
		src, err := repoCfg.GetSource(pathInRepository)
		DieIfErr(err)

		source, err := src.RemoteURI()
		if err != nil {
			return fmt.Errorf("could not parse remote source for '%s': %w", pathInRepository, err)
		}

		currentStableRef := src.Head
		if update {
			currentStableRef, err = local.DereferenceBranch(ctx, client, source)
			DieIfErr(err)
			err = repoCfg.UpdateSourceVersion(pathInRepository, currentStableRef)
			DieIfErr(err)
		}
		// sync the thing!
		fullPath := path.Join(repoCfg.Root(), pathInRepository)
		return local.SyncDirectory(ctx, client, source, fullPath, maxParallelism)
	}

	// let's pull all sources in the repo
	srcConfig, err := repoCfg.GetSourcesConfig()
	DieIfErr(err)

	for targetDirectory, src := range srcConfig.Sources {
		source, err := src.RemoteURI()
		DieIfErr(err)

		// sync the thing!
		currentStableRef := src.Head
		if update {
			currentStableRef, err = local.DereferenceBranch(ctx, client, source)
			DieIfErr(err)
			DieIfErr(repoCfg.UpdateSourceVersion(targetDirectory, currentStableRef))
		}
		stableSource := &uri.URI{
			Repository: source.Repository,
			Ref:        currentStableRef,
			Path:       source.Path,
		}
		fullPath := path.Join(repoCfg.Root(), targetDirectory)
		DieIfErr(local.SyncDirectory(ctx, client, stableSource, fullPath, maxParallelism))
	}
	return nil
}

// localCmd is for integration with local execution engines!
var localCmd = &cobra.Command{
	Short: "commands used to sync and reproduce data from lakeFS locally",
}

var localAddCmd = &cobra.Command{
	Use:     "add <directory> <lakefs branch+path uri>",
	Short:   "add a local directory to a lakeFS branch under the specified uri",
	Example: "add path/to/data lakefs://example-repo/main/path/to/data/",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		targetDirectory := args[0]
		source := MustParsePathURI("path", args[1])

		repoCfg := mustGitConf(targetDirectory)

		var fullPath string
		var pathInRepository string
		pathInRepository, err := repoCfg.RelativeToRoot(targetDirectory)
		DieIfErr(err)
		fullPath = path.Join(repoCfg.Root(), pathInRepository)
		hasSource, err := repoCfg.HasSource(pathInRepository)
		DieIfErr(err)
		if hasSource {
			DieFmt("directory already tracked in lakeFS. See data.yaml`.")
		}
		locationExists, err := local.DirectoryExists(fullPath)
		DieIfErr(err)
		if !locationExists {
			DieFmt("directory '%s' doesn't exist. Create it first")
		}

		// make sure it *doesn't* exist on the server
		client := getClient()
		remotePathExists, err := local.LakeFSPathExists(cmd.Context(), client, source)
		DieIfErr(err)
		if remotePathExists {
			DieFmt("lakeFS directory already exists at '%s' - perhaps clone it instead?")
		}

		// init an empty index
		err = local.InitEmptyIndex(cmd.Context(), source, fullPath)
		DieIfErr(err)

		// add it to data.yaml
		// make sure our current ref is also the latest
		latestCommitId, err := local.DereferenceBranch(cmd.Context(), client, source)
		DieIfErr(err)
		err = repoCfg.AddSource(pathInRepository, source.String(), latestCommitId)
		DieIfErr(err)
		// add it to git ignore
		err = repoCfg.GitIgnore(pathInRepository)
		DieIfErr(err)
	},
}

// localCloneCmd clones a lakeFS directory locally (committed only).
// if the target directory is within a git repository, also add a `data.yaml` file that describes local clones of data
var localCloneCmd = &cobra.Command{
	Use:     "clone <lakeFS branch/path uri> [<target directory>]",
	Short:   "clone a lakeFS directory locally (committed only)",
	Example: "clone lakefs://example-repo/main/path/to/data/",
	Args:    cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		// parse args
		source := MustParsePathURI("path", args[0])
		var targetDirectory string
		if len(args) > 1 {
			targetDirectory = args[1]
		} else {
			targetDirectory = source.GetPath()
		}

		repoCfg := mustGitConf(targetDirectory)

		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)

		var fullPath string
		var pathInRepository string
		pathInRepository, err = repoCfg.RelativeToRoot(targetDirectory)
		DieIfErr(err)
		fullPath = path.Join(repoCfg.Root(), pathInRepository)
		hasSource, err := repoCfg.HasSource(pathInRepository)
		DieIfErr(err)
		if hasSource {
			DieFmt("directory already cloned. You can try running `pull`.")
		}

		locationExists, err := local.DirectoryExists(fullPath)
		DieIfErr(err)
		if locationExists {
			DieFmt("directory already exists. Try a different location?")
		}

		// let's try and dereference the branch
		client := getClient()
		stableRef, err := local.DereferenceBranch(cmd.Context(), client, source)
		DieIfErr(err)

		// sync the thing!
		stableSource := &uri.URI{
			Repository: source.Repository,
			Ref:        stableRef,
			Path:       source.Path,
		}
		err = local.SyncDirectory(cmd.Context(), client, stableSource, fullPath, maxParallelism)
		DieIfErr(err)
		// add it to data.yaml
		err = repoCfg.AddSource(pathInRepository, source.String(), stableRef)
		DieIfErr(err)
		// add it to git ignore
		err = repoCfg.GitIgnore(pathInRepository)
		DieIfErr(err)
	},
}

var localStatusCmd = &cobra.Command{
	Use:   "status [<target directory>]",
	Short: "show local changes to data pulled from lakeFS",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			// directory passed
			repoCfg := mustGitConf(args[0])
			fullPath, err := repoCfg.RelativeToRoot(args[0])
			hasSource, err := repoCfg.HasSource(fullPath)
			DieIfErr(err)
			if !hasSource {
				DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
			}
			fmt.Printf("Directory: '%s':\n\n", fullPath)
			diffResults, err := local.DoDiff(fullPath)
			DieIfErr(err)
			printLocalDiff(diffResults)
			return
		}

		repoCfg := mustGitConf(".")
		srcConfig, err := repoCfg.GetSourcesConfig()
		DieIfErr(err)
		for pathInRepository := range srcConfig.Sources {
			fmt.Printf("Directory: '%s':\n\n", pathInRepository)
			fullPath := path.Join(repoCfg.Root(), pathInRepository)
			diffResults, err := local.DoDiff(fullPath)
			DieIfErr(err)
			printLocalDiff(diffResults)
			fmt.Print("\n\n")
		}
	},
}

var localCommitCmd = &cobra.Command{
	Use:   "commit <target directory>",
	Short: "upload & commit changes to data files to the remote lakeFS repository",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		repoCfg := mustGitConf(args[0])
		// parse commit arguments from command line
		kvPairs, err := getKV(cmd, "meta")
		DieIfErr(err)
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		allowDirty, err := cmd.Flags().GetBool("allow-dirty")
		DieIfErr(err)
		message, err := cmd.Flags().GetString("message")
		DieIfErr(err)

		// check if code is clean?
		isClean, err := repoCfg.IsClean()
		DieIfErr(err)
		if !isClean && !allowDirty {
			DieFmt("you have uncommitted changes to your code (see `git status`). Either commit them or use --allow-dirty")
		}

		pathInRepository, err := repoCfg.RelativeToRoot(args[0])
		DieIfErr(err)
		fullPath := path.Join(repoCfg.Root(), pathInRepository)

		hasSource, err := repoCfg.HasSource(pathInRepository)
		DieIfErr(err)
		if !hasSource {
			DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", pathInRepository)
		}
		// get source branch
		src, err := repoCfg.GetSource(pathInRepository)
		DieIfErr(err)
		source, err := src.RemoteURI()
		if err != nil {
			DieFmt("could not parse remote source for '%s': %s", pathInRepository, err)
		}

		// make sure we don't have any dirty writes on the lakeFS branch
		client := getClient()
		hasUncommitted, err := local.HasUncommittedChanges(cmd.Context(), client, source)
		DieIfErr(err)
		if hasUncommitted {
			DieFmt("your lakeFS branch already has uncommitted changes. Please commit/revert those first!")
		}

		// make sure our current ref is also the latest
		latestCommitId, err := local.DereferenceBranch(cmd.Context(), client, source)
		DieIfErr(err)
		if latestCommitId != src.Head {
			DieFmt("local copy of lakeFS branch '%s' is not up to date with server. Please run `pull` first.", source.Ref)
		}

		// let's go!
		DieIfErr(local.UploadDirectoryChanges(cmd.Context(), client, source, fullPath, repoCfg.Root(), maxParallelism))

		currentCommitId, err := repoCfg.CurrentCommitId()
		if err != nil && errors.Is(err, local.ErrNoCommitFound) {
			currentCommitId = ""
		} else if err != nil {
			DieErr(err)
		}

		repo := repoCfg.Repository()
		remotes, err := repo.Remotes()
		DieIfErr(err)
		if currentCommitId != "" {
			kvPairs[gitCommitKeyName] = currentCommitId
		}
		kvPairs[gitPathKeyName] = pathInRepository
		for _, remote := range remotes {
			urls := remote.Config().URLs
			if remote.Config().IsFirstURLLocal() && len(urls) > 1 {
				urls = urls[1:]
			}
			remoteUrl := strings.Join(urls, " ; ")
			kvPairs[fmt.Sprintf("git.remotes.%s.url", remote.Config().Name)] = remoteUrl
		}
		commitId, err := local.Commit(cmd.Context(), client, source, message, kvPairs)
		DieIfErr(err)

		updatedSource := &uri.URI{
			Repository: source.Repository,
			Ref:        commitId,
			Path:       source.Path,
		}
		DieIfErr(local.SyncDirectory(cmd.Context(), client, updatedSource, fullPath, maxParallelism))
		DieIfErr(repoCfg.UpdateSourceVersion(pathInRepository, commitId))
	},
}

var localPullCmd = &cobra.Command{
	Use:   "pull [<target directory>]",
	Short: "pull data files from lakeFS as described in data.yaml",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		update, err := cmd.Flags().GetBool("update")
		DieIfErr(err)

		// make sure no local changes
		if len(args) > 0 {
			// directory passed
			repoCfg := mustGitConf(args[0])
			fullPath, err := repoCfg.RelativeToRoot(args[0])
			hasSource, err := repoCfg.HasSource(fullPath)
			DieIfErr(err)
			if !hasSource {
				DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
			}
			dirExists, err := local.DirectoryExists(fullPath)
			DieIfErr(err)
			if dirExists {
				diffResults, err := local.DoDiff(fullPath)
				DieIfErr(err)
				if !diffResults.IsClean() {
					DieFmt("Found uncommitted changes under '%s', please commit or reset first", fullPath)
				}
			}
		}

		// no directory passed
		repoCfg := mustGitConf(".")
		srcConfig, err := repoCfg.GetSourcesConfig()
		DieIfErr(err)
		for pathInRepository := range srcConfig.Sources {
			fullPath := path.Join(repoCfg.Root(), pathInRepository)
			dirExists, err := local.DirectoryExists(fullPath)
			DieIfErr(err)
			if dirExists {
				diffResults, err := local.DoDiff(fullPath)
				DieIfErr(err)
				if !diffResults.IsClean() {
					DieFmt("Found uncommitted changes under '%s', please commit or reset first", fullPath)
				}
			}
		}
		DieIfErr(pull(cmd.Context(), maxParallelism, update, args...))
	},
}

var localResetCmd = &cobra.Command{
	Use:   "reset [<target directory>]",
	Short: "overwrite local data files with files from lakeFS as described in data.yaml",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		// run pull without asking questions.
		DieIfErr(pull(cmd.Context(), maxParallelism, false, args...))
	},
}

var localCheckoutCmd = &cobra.Command{
	Use:     "checkout <target directory> <lakeFS ref>",
	Example: "checkout path/to/data lakefs://example-repo/experiment-1",
	Short:   "sync the target directory with the specified ref",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		targetDirectory := args[0]
		repoCfg := mustGitConf(targetDirectory)
		ref := MustParseRefURI("lakeFS ref", args[1])

		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)

		fullPath, err := repoCfg.RelativeToRoot(args[0])
		hasSource, err := repoCfg.HasSource(fullPath)
		DieIfErr(err)
		if !hasSource {
			DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
		}

		sources, err := repoCfg.GetSourcesConfig()
		DieIfErr(err)
		src := sources.Sources[fullPath]
		currentUri, err := src.RemoteURI()
		DieIfErr(err)
		sources.Sources[fullPath] = local.Source{
			Remote: (&uri.URI{
				Repository: ref.Repository,
				Ref:        ref.Ref,
				Path:       currentUri.Path,
			}).String(),
		}
		DieIfErr(repoCfg.SaveSourcesConfig(sources))
		DieIfErr(pull(cmd.Context(), maxParallelism, true, fullPath))
	},
}

func initLocalAsSubcommand(root *cobra.Command) {
	root.AddCommand(localCmd)
	initLocal("local")
}

func initLocal(name string) {
	localCmd.Use = name
	localCmd.AddCommand(localStatusCmd)

	localCmd.AddCommand(localCloneCmd)
	localCloneCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")

	localCmd.AddCommand(localAddCmd)

	localCmd.AddCommand(localCheckoutCmd)
	localCheckoutCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")

	localCmd.AddCommand(localCommitCmd)
	localCommitCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
	localCommitCmd.Flags().StringP("message", "m", "", "commit message to use for the resulting lakeFS commit")
	localCommitCmd.Flags().Bool("allow-dirty", false, "allow committing while the Git repository has uncommitted changes. Enabling this might hurt reproducibility.")
	localCommitCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")

	localCmd.AddCommand(localPullCmd)
	localPullCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")
	localPullCmd.Flags().BoolP("update", "u", false, "pull the latest data available on the remote (and update data.yaml)")

	localCmd.AddCommand(localResetCmd)
	localResetCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")
}
