package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/docker/docker/api/types/mount"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/go-git/go-git/v5"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	DownloadConcurrency = 5

	gitCommitKeyName  = "git.commit.id"
	gitPathKeyName    = "git.repository.path"
	gitRepoUrlKeyName = "git.repository.url"
	gitDefaultRemote  = "origin"
)

// localCmd is for integration with local execution engines!
var localCmd = &cobra.Command{
	Use:   "local",
	Short: "commands used to sync and reproduce data from lakeFS locally",
}

// cloneCmd clones a lakeFS directory locally (committed only).
// if the target directory is within a git repository, also add a `data.yaml` file
//
//	that describes local clones of data
var cloneCmd = &cobra.Command{
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

		isGit := true
		repoCfg, err := local.PathConfig(targetDirectory)
		if errors.Is(err, git.ErrRepositoryNotExists) {
			// not a git repo
			isGit = false
		} else if err != nil {
			DieErr(err)
		}

		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)

		var fullPath string
		var pathInRepository string
		if isGit {
			pathInRepository, err = repoCfg.RelativeToRoot(targetDirectory)
			DieIfErr(err)
			fullPath = path.Join(repoCfg.Root(), pathInRepository)
			hasSource, err := repoCfg.HasSource(pathInRepository)
			DieIfErr(err)
			if hasSource {
				DieFmt("directory already cloned. You can try running `pull`.")
			}
		} else {
			var err error
			fullPath, err = filepath.Abs(targetDirectory)
			DieIfErr(err)
		}

		locationExists, err := local.DirectoryExists(fullPath)
		DieIfErr(err)
		if locationExists {
			DieFmt("directory already exists. Try a different location?")
		}

		// let's try and dereference the branch
		lakeFSClient := getClient()
		stableRef, err := local.DereferenceBranch(cmd.Context(), lakeFSClient, source)
		DieIfErr(err)

		// sync the thing!
		stableSource := &uri.URI{
			Repository: source.Repository,
			Ref:        stableRef,
			Path:       source.Path,
		}
		err = local.SyncDirectory(cmd.Context(), lakeFSClient, stableSource, fullPath, maxParallelism)
		DieIfErr(err)

		// write to config
		if isGit {
			err = repoCfg.AddSource(pathInRepository, source.String(), stableRef)
			DieIfErr(err)
			err = repoCfg.GitIgnore(pathInRepository)
			DieIfErr(err)
		}
	},
}

func printDiffLine(diffType, path string) {
	var color text.Color
	var action string

	switch diffType {
	case "added":
		color = text.FgGreen
		action = "added"
	case "removed":
		color = text.FgRed
		action = "removed"
	case "changed":
		color = text.FgYellow
		action = "modified"
	default:
	}
	fmt.Print(color.Sprintf("\t%s %s\n", action, path))
}

func printLocalDiff(d *local.Diff) (total int) {
	if len(d.Modified) > 0 {
		for _, p := range d.Modified {
			printDiffLine("changed", p)
			total += 1
		}
		fmt.Print("\n")
	}

	if len(d.Added) > 0 {
		for _, p := range d.Added {
			printDiffLine("added", p)
			total += 1
		}
		fmt.Print("\n")
	}

	if len(d.Removed) > 0 {
		for _, p := range d.Removed {
			printDiffLine("removed", p)
			total += 1
		}
	}
	if total == 0 {
		fmt.Print("\n\tNo local changes\n")
	}
	return
}

var statusCmd = &cobra.Command{
	Use:   "status [<target directory>]",
	Short: "show local changes to data pulled from lakeFS",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isGit := true
		if len(args) > 0 {
			// directory passed
			repoCfg, err := local.PathConfig(args[0])
			if errors.Is(err, git.ErrRepositoryNotExists) {
				// not a git repo
				isGit = false
			} else if err != nil {
				DieErr(err)
			}
			var fullPath string
			if isGit {
				fullPath, err = repoCfg.RelativeToRoot(args[0])
				hasSource, err := repoCfg.HasSource(fullPath)
				DieIfErr(err)
				if !hasSource {
					DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
				}
			} else {
				fullPath, err = filepath.Abs(args[0])
				DieIfErr(err)
			}
			fmt.Printf("Directory: '%s':\n\n", fullPath)
			diffResults, err := local.DoDiff(fullPath)
			DieIfErr(err)
			printLocalDiff(diffResults)
			return
		}

		// no directory passed
		repoCfg, err := local.Config()
		if errors.Is(err, git.ErrRepositoryNotExists) {
			// not a git repo
			isGit = false
		} else if err != nil {
			DieErr(err)
		}
		if !isGit {
			DieFmt("outside a git repository, `lakectl local status` requires an explicit path to a cloned source")
		}

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

// runCmd executes a container with mounted data!
var runCmd = &cobra.Command{
	Use:  "run",
	Args: cobra.MaximumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		repoCfg, err := local.Config()
		if errors.Is(err, git.ErrRepositoryNotExists) {
			// not a git repo
			DieFmt("run should be executed in the context if a Git repository, in a directory with a Runfile.yaml")
		} else if err != nil {
			DieErr(err)
		}

		specBytes, err := os.ReadFile(local.SpecFileName)
		DieIfErr(err)
		spec := &local.RunSpec{}
		DieIfErr(yaml.Unmarshal(specBytes, spec))
		if spec.SpecVersion != local.SpecVersion {
			DieFmt("spec version not supported: %d (only %d supported)",
				spec.SpecVersion, local.SpecVersion)
		}

		// set up mounts
		mountedSources := make([]mount.Mount, 0)
		for _, mountPoint := range spec.Sources {
			hasSource, err := repoCfg.HasSource(mountPoint.Source)
			DieIfErr(err)
			if !hasSource {
				DieFmt("'%s' is not a valid data source configured in this repository."+
					" Try doing `lakectl repo clone` first?",
					mountPoint.Source)
			}
			fullPath, err := repoCfg.RelativeToRoot(mountPoint.Source)
			DieIfErr(err)
			fullPath, err = filepath.Abs(fullPath)
			DieIfErr(err)
			mountedSources = append(mountedSources, mount.Mount{
				Type:   mount.TypeBind,
				Source: fullPath,
				Target: mountPoint.Target,
			})
		}

		// run container
		docker, err := client.NewClientWithOpts(
			client.FromEnv,
			client.WithAPIVersionNegotiation(),
		)
		DieIfErr(err)

		// pull always?
		out, err := docker.ImagePull(cmd.Context(), spec.Exec.Image, types.ImagePullOptions{})
		DieIfErr(err)
		defer func() {
			_ = out.Close()
		}()
		_, err = io.Copy(os.Stdout, out)
		DieIfErr(err)

		containerName := fmt.Sprintf("lakectl-run-%s", uuid.New().String())
		resp, err := docker.ContainerCreate(
			cmd.Context(),
			&container.Config{
				Image: spec.Exec.Image,
				Env:   spec.Exec.Environ,
				Cmd:   spec.Exec.Cmd,
			},
			&container.HostConfig{
				AutoRemove: true,
				Mounts:     mountedSources,
			}, nil, nil, containerName,
		)
		DieIfErr(err)
		done := make(chan struct{})
		go func() {
			attach, err := docker.ContainerAttach(cmd.Context(), containerName, types.ContainerAttachOptions{Stream: true, Stderr: true, Stdout: true})
			if err != nil {
				DieErr(err)
			}
			defer attach.Close()
			_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, attach.Reader)
			DieIfErr(err)
			close(done)
		}()

		fmt.Printf("running container ID: %s\n", resp.ID)
		DieIfErr(docker.ContainerStart(cmd.Context(), resp.ID, types.ContainerStartOptions{}))
		<-done // stdout closed

		// run status to show changes:
		fmt.Printf("Execution Complete! Changed sources:\n")
		clean := true
		for _, s := range mountedSources {
			sourcePath, err := repoCfg.RelativeToRoot(s.Source)
			DieIfErr(err)

			diffResult, err := local.DoDiff(sourcePath)
			DieIfErr(err)
			if !diffResult.IsClean() {
				clean = false
				fmt.Printf("%s\n", sourcePath)
				printLocalDiff(diffResult)
			}
		}
		if clean {
			fmt.Printf("no changes to data source\n")
		} else {
			fmt.Printf("You may commit these changes with `lakectl local commit <data source>`\n")
		}
	},
}

var localCommitCmd = &cobra.Command{
	Use:   "commit <target directory>",
	Short: "upload & commit changes to data files to the remote lakeFS repository",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		repoCfg, err := local.PathConfig(args[0])
		if errors.Is(err, git.ErrRepositoryNotExists) {
			DieFmt("commit is possible available for data cloned into a git repository")
		} else if err != nil {
			DieErr(err)
		}

		kvPairs, err := getKV(cmd, "meta")
		DieIfErr(err)

		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)

		allowDirty, err := cmd.Flags().GetBool("allow-dirty")
		DieIfErr(err)

		message, err := cmd.Flags().GetString("message")
		DieIfErr(err)

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
		if latestCommitId != src.AtVersion {
			DieFmt("local copy of lakeFS branch '%s' is not up to date with server. Please run `pull` first.", source.Ref)
		}

		// let's go!
		DieIfErr(local.UploadDirectoryChanges(cmd.Context(), client, source, fullPath, repoCfg.Root(), maxParallelism))

		currentCommitId, err := repoCfg.CurrentCommitId()
		DieIfErr(err)

		hasRemote, err := repoCfg.HasRemote(gitDefaultRemote)
		DieIfErr(err)

		kvPairs[gitCommitKeyName] = currentCommitId
		kvPairs[gitPathKeyName] = pathInRepository
		if hasRemote {
			remote, err := repoCfg.GetRemote(gitDefaultRemote)
			DieIfErr(err)
			kvPairs[gitRepoUrlKeyName] = remote
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

func pull(ctx context.Context, maxParallelism int, update bool, args ...string) error {
	client := getClient()
	var err error
	var repoCfg *local.Conf
	if len(args) > 0 {
		repoCfg, err = local.PathConfig(args[0])
	} else {
		repoCfg, err = local.Config()
	}
	if errors.Is(err, git.ErrRepositoryNotExists) {
		DieFmt("commit is possible available for data cloned into a git repository")
	} else if err != nil {
		DieErr(err)
	}

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

		currentStableRef := src.AtVersion
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
		currentStableRef := src.AtVersion
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

var localPullCmd = &cobra.Command{
	Use:   "pull [<target directory>]",
	Short: "pull data files from lakeFS as described in $GIT_REPOSITORY/data.yaml",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		update, err := cmd.Flags().GetBool("update")
		DieIfErr(err)

		// make sure no local changes
		if len(args) > 0 {
			// directory passed
			repoCfg, err := local.PathConfig(args[0])
			if errors.Is(err, git.ErrRepositoryNotExists) {
				// not a git repo
				DieFmt("pull only works in the context of a git repository")
			} else if err != nil {
				DieErr(err)
			}
			fullPath, err := repoCfg.RelativeToRoot(args[0])
			hasSource, err := repoCfg.HasSource(fullPath)
			DieIfErr(err)
			if !hasSource {
				DieFmt("'%s' doesn't seem to be a  data directory. You can try running `clone`.", args[0])
			}

			fmt.Printf("Directory: '%s':\n\n", fullPath)
			diffResults, err := local.DoDiff(fullPath)
			DieIfErr(err)
			if !diffResults.IsClean() {
				DieFmt("Found uncommitted changes under '%s', please commit or reset first", fullPath)
			}
		}

		// no directory passed
		repoCfg, err := local.Config()
		if errors.Is(err, git.ErrRepositoryNotExists) {
			// not a git repo
			DieFmt("pull only works in the context of a git repository")
		} else if err != nil {
			DieErr(err)
		}

		srcConfig, err := repoCfg.GetSourcesConfig()
		DieIfErr(err)
		for pathInRepository := range srcConfig.Sources {
			fmt.Printf("Directory: '%s':\n\n", pathInRepository)
			fullPath := path.Join(repoCfg.Root(), pathInRepository)
			diffResults, err := local.DoDiff(fullPath)
			DieIfErr(err)
			if !diffResults.IsClean() {
				DieFmt("Found uncommitted changes under '%s', please commit or reset first", fullPath)
			}
		}

		DieIfErr(pull(cmd.Context(), maxParallelism, update, args...))
	},
}

var localResetCmd = &cobra.Command{
	Use:   "reset [<target directory>]",
	Short: "overwrite local data files with files from lakeFS as described in $GIT_REPOSITORY/data.yaml",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		maxParallelism, err := cmd.Flags().GetInt("parallelism")
		DieIfErr(err)
		DieIfErr(pull(cmd.Context(), maxParallelism, false, args...))
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(localCmd)

	localCmd.AddCommand(runCmd)

	localCmd.AddCommand(statusCmd)

	localCmd.AddCommand(cloneCmd)
	cloneCmd.Flags().IntP("parallelism", "p", DownloadConcurrency, "maximum objects to download in parallel")

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
